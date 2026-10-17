package com.openagentflow.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.openagentflow.entity.KnowledgeChunkEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/** OpenSearch BM25索引与检索客户端。 */
@Service
public class KeywordSearchService {

    /** HTTP客户端。 */
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** JSON工具。 */
    private final ObjectMapper objectMapper;

    /** 是否启用OpenSearch。 */
    private final boolean enabled;

    /** OpenSearch地址。 */
    private final String endpoint;

    /** 基础认证文本。 */
    private final String authorization;

    /** 当前发布索引的权威路由。 */
    private final KnowledgeIndexRouteService indexRoutes;

    public KeywordSearchService(ObjectMapper objectMapper,
                                @Value("${openagentflow.opensearch.enabled:false}") boolean enabled,
                                @Value("${openagentflow.opensearch.endpoint:http://localhost:9200}") String endpoint,
                                @Value("${openagentflow.opensearch.username:}") String username,
                                @Value("${openagentflow.opensearch.password:}") String password) {
        this(objectMapper, enabled, endpoint, username, password, null);
    }

    @Autowired
    public KeywordSearchService(ObjectMapper objectMapper,
                                @Value("${openagentflow.opensearch.enabled:false}") boolean enabled,
                                @Value("${openagentflow.opensearch.endpoint:http://localhost:9200}") String endpoint,
                                @Value("${openagentflow.opensearch.username:}") String username,
                                @Value("${openagentflow.opensearch.password:}") String password,
                                KnowledgeIndexRouteService indexRoutes) {
        this.objectMapper = objectMapper;
        this.enabled = enabled;
        this.endpoint = endpoint.replaceAll("/$", "");
        this.authorization = StringUtils.hasText(username)
                ? "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8)) : "";
        this.indexRoutes = indexRoutes;
    }

    /** 是否启用关键词集群。 */
    public boolean isEnabled() { return enabled; }

    /** 批量写入知识分片。 */
    public void indexChunks(String kbId, List<KnowledgeChunkEntity> chunks) {
        if (!enabled || chunks == null || chunks.isEmpty()) return;
        indexChunksInto(indexName(kbId), kbId, chunks);
    }

    /** 构建新版本时显式写入物理关键词索引，不影响当前线上版本。 */
    public void indexChunksInto(String index, String kbId, List<KnowledgeChunkEntity> chunks) {
        if (!enabled || chunks == null || chunks.isEmpty()) return;
        try {
            ensureIndex(index);
            StringBuilder body = new StringBuilder();
            for (KnowledgeChunkEntity chunk : chunks) {
                body.append(objectMapper.writeValueAsString(java.util.Map.of("index", java.util.Map.of("_index", index, "_id", chunk.getId())))).append('\n');
                body.append(objectMapper.writeValueAsString(java.util.Map.of(
                        "chunkId", chunk.getId(), "kbId", kbId, "documentId", chunk.getDocumentId(),
                        "title", safe(chunk.getTitle()), "content", safe(chunk.getContent()), "status", safe(chunk.getStatus())))).append('\n');
            }
            HttpResponse<String> response = send("/_bulk", "POST", body.toString(), "application/x-ndjson");
            if (response.statusCode() >= 300 || objectMapper.readTree(response.body()).path("errors").asBoolean(false)) {
                throw new IllegalStateException("OpenSearch批量索引失败：" + response.body());
            }
        } catch (Exception exception) {
            throw new IllegalStateException("OpenSearch批量索引失败", exception);
        }
    }

    /** 使用BM25查询分片ID和归一化分数。 */
    public List<KeywordHit> search(String kbId, String query, int topK) {
        if (!enabled || !StringUtils.hasText(query)) return List.of();
        return searchIndex(indexName(kbId), query, topK);
    }

    /** 使用检索开始时固定的物理版本，避免中途再次解析 active 造成串版。 */
    public List<KeywordHit> searchIndex(String index, String query, int topK) {
        if (!enabled || !StringUtils.hasText(query)) return List.of();
        try {
            ObjectNode body = objectMapper.createObjectNode();
            body.put("size", Math.max(1, Math.min(topK, 200)));
            body.putObject("query").putObject("multi_match").put("query", query).putArray("fields").add("title^2").add("content");
            HttpResponse<String> response = send("/" + index + "/_search", "POST", body.toString(), "application/json");
            if (response.statusCode() >= 300) throw new IllegalStateException("关键词索引查询HTTP " + response.statusCode());
            JsonNode root = objectMapper.readTree(response.body());
            double maxScore = Math.max(0.000001D, root.path("hits").path("max_score").asDouble(1D));
            List<KeywordHit> result = new ArrayList<>();
            for (JsonNode hit : root.path("hits").path("hits")) {
                result.add(new KeywordHit(hit.path("_id").asText(), Math.min(1D, hit.path("_score").asDouble() / maxScore)));
            }
            return result;
        } catch (Exception exception) {
            throw new IllegalStateException("OpenSearch关键词检索失败", exception);
        }
    }

    /** 按文档ID删除关键词索引。 */
    public void deleteDocument(String kbId, String documentId) {
        if (!enabled) return;
        try {
            String body = objectMapper.writeValueAsString(java.util.Map.of("query", java.util.Map.of("term", java.util.Map.of("documentId", documentId))));
            HttpResponse<String> response = send("/" + indexName(kbId) + "/_delete_by_query?conflicts=proceed", "POST", body, "application/json");
            if (response.statusCode() >= 300 && response.statusCode() != 404) throw new IllegalStateException(response.body());
        } catch (Exception exception) {
            throw new IllegalStateException("OpenSearch文档删除失败", exception);
        }
    }

    /** 查询文档在OpenSearch中的真实分片数量。 */
    public long documentChunkCount(String kbId, String documentId) {
        if (!enabled) return -1L;
        try {
            String body = objectMapper.writeValueAsString(java.util.Map.of(
                    "query", java.util.Map.of("term", java.util.Map.of("documentId", documentId))));
            HttpResponse<String> response = send("/" + indexName(kbId) + "/_count", "POST", body, "application/json");
            if (response.statusCode() == 404) return 0L;
            if (response.statusCode() >= 300) throw new IllegalStateException(response.body());
            return objectMapper.readTree(response.body()).path("count").asLong();
        } catch (Exception exception) {
            throw new IllegalStateException("OpenSearch文档计数失败", exception);
        }
    }

    private HttpResponse<String> send(String path, String method, String body, String contentType) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint + path)).timeout(Duration.ofSeconds(20))
                .header("Content-Type", contentType).method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        if (StringUtils.hasText(authorization)) builder.header("Authorization", authorization);
        return httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    /** 确保知识库BM25索引及中文文本字段映射存在。 */
    private void ensureIndex(String index) throws Exception {
        HttpResponse<String> exists = send("/" + index, "HEAD", "", "application/json");
        if (exists.statusCode() == 200) return;
        String mapping = """
                {"mappings":{"properties":{"chunkId":{"type":"keyword"},"kbId":{"type":"keyword"},
                "documentId":{"type":"keyword"},"status":{"type":"keyword"},"title":{"type":"text"},"content":{"type":"text"}}}}
                """;
        HttpResponse<String> created = send("/" + index, "PUT", mapping, "application/json");
        if (created.statusCode() >= 300 && created.statusCode() != 400) throw new IllegalStateException(created.body());
    }

    /** 构建完成后等待关键词索引对检索可见。 */
    public void refreshIndex(String index) {
        if (!enabled) return;
        try {
            ensureIndex(index);
            HttpResponse<String> response = send("/" + index + "/_refresh", "POST", "", "application/json");
            if (response.statusCode() >= 300) throw new IllegalStateException(response.body());
        } catch (Exception exception) {
            throw new IllegalStateException("关键词索引刷新失败", exception);
        }
    }

    /** 普通增删操作跟随发布路由，兼容尚未发布版本的知识库。 */
    private String indexName(String kbId) {
        return indexRoutes == null ? KnowledgeIndexRouteService.legacyKeywordIndex(kbId)
                : indexRoutes.resolveByKnowledgeBaseId(kbId).keywordIndexName();
    }
    private String safe(String value) { return value == null ? "" : value; }

    /**
     * BM25命中项。
     *
     * @param chunkId 分片ID
     * @param score 归一化分数
     */
    public record KeywordHit(String chunkId, double score) { }
}
