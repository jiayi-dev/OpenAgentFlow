package com.openagentflow.service;

import com.openagentflow.entity.KnowledgeBaseEntity;
import com.openagentflow.exception.BusinessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.stereotype.Service;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 将模型、向量集合与关键词索引解析为同一次检索使用的不可变路由。 */
@Service
public class KnowledgeIndexRouteService {

    /** 索引版本与源数据查询工具。 */
    private final JdbcTemplate jdbcTemplate;

    public KnowledgeIndexRouteService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * 索引读取快照，数据库 active 版本是权威路由，别名仅为外部投影。
     * @param versionId 索引版本，空值表示兼容旧集合
     * @param collectionName 实际物理集合，版本集合已包含维度后缀
     * @param embeddingModelId 与文档向量兼容的模型
     * @param keywordIndexName 同版本关键词索引
     * @param dimension 版本向量维度，旧集合可为空
     * @param cacheFingerprint 缓存隔离指纹
     */
    public record Route(String versionId, String collectionName, String embeddingModelId,
                        String keywordIndexName, Integer dimension, String cacheFingerprint) { }

    /** 只解析一次当前发布版本，防止一次查询在多个组件间读到不同代际。 */
    public Route resolve(KnowledgeBaseEntity kb) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                SELECT id, workspace_id, collection_name, embedding_model_id, keyword_index_name, dimension
                FROM knowledge_index_version WHERE kb_id=? AND status='active'
                ORDER BY activated_at DESC, version_no DESC LIMIT 2
                """, kb.getId());
        if (rows.size() > 1) {
            throw new BusinessException("INDEX_ACTIVE_VERSION_CONFLICT", "知识库存在多个激活索引，请修复发布状态后重试");
        }
        if (rows.isEmpty()) {
            return route("", kb.getMilvusCollectionName(), kb.getEmbeddingModelId(), legacyKeywordIndex(kb.getId()), null);
        }
        Map<String, Object> row = rows.getFirst();
        if (!Objects.equals(kb.getWorkspaceId(), row.get("workspace_id"))) {
            throw new BusinessException("INDEX_WORKSPACE_MISMATCH", "索引版本与知识库工作空间不一致");
        }
        Integer dimension = row.get("dimension") instanceof Number value ? value.intValue() : null;
        if (dimension == null || dimension <= 0 || !StringUtils.hasText(text(row.get("embedding_model_id")))
                || !StringUtils.hasText(text(row.get("keyword_index_name")))) {
            throw new BusinessException("INDEX_ROUTE_INVALID", "激活索引缺少模型、维度或关键词路由，请重新构建");
        }
        return route(text(row.get("id")), physicalCollection(text(row.get("collection_name")), dimension),
                text(row.get("embedding_model_id")), text(row.get("keyword_index_name")), dimension);
    }

    /** 根据知识库标识解析写入或删除目标，避免发布后继续更新旧集合。 */
    public Route resolveByKnowledgeBaseId(String kbId) {
        Map<String, Object> row = jdbcTemplate.queryForMap("""
                SELECT id, workspace_id, milvus_collection_name, embedding_model_id
                FROM knowledge_base WHERE id=?
                """, kbId);
        KnowledgeBaseEntity kb = new KnowledgeBaseEntity();
        kb.setId(kbId);
        kb.setWorkspaceId(textOrNull(row.get("workspace_id")));
        kb.setMilvusCollectionName(textOrNull(row.get("milvus_collection_name")));
        kb.setEmbeddingModelId(textOrNull(row.get("embedding_model_id")));
        return resolve(kb);
    }

    /** 版本集合的维度后缀只能追加一次。 */
    public static String physicalCollection(String base, int dimension) {
        String suffix = "_d" + dimension;
        return base.endsWith(suffix) ? base : base + suffix;
    }

    /** 返回旧知识库关键词索引名，保持未发布版本的兼容性。 */
    public static String legacyKeywordIndex(String kbId) {
        return "oaf-kb-" + kbId.toLowerCase(java.util.Locale.ROOT).replaceAll("[^a-z0-9_-]", "-");
    }

    /**
     * 流式计算当前模型的有效源清单，索引构建和激活均须核验。
     * 只保存摘要状态，不将全部切片或向量载入内存。
     */
    public String sourceFingerprint(String kbId, String modelId) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            String sql = """
                    SELECT e.id, e.model_id, e.embedding_dim, e.content_hash, c.content_hash AS chunk_hash,
                           c.created_at AS chunk_created_at, d.updated_at AS document_updated_at
                    FROM knowledge_embedding e
                    JOIN knowledge_chunk c ON c.id=e.chunk_id
                    JOIN knowledge_document d ON d.id=c.document_id
                    WHERE e.kb_id=? AND e.model_id=? AND e.embedding_json IS NOT NULL
                      AND c.status='active' AND (c.chunk_level IS NULL OR c.chunk_level<>'parent')
                      AND d.parse_status='parsed'
                    ORDER BY e.id
                    """;
            jdbcTemplate.query(connection -> {
                // MySQL 流式结果集，源清单只保留摘要状态，避免大库核验时缓存全部行。
                var statement = connection.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(Integer.MIN_VALUE);
                statement.setString(1, kbId);
                statement.setString(2, modelId);
                return statement;
            }, (RowCallbackHandler) rs -> {
                for (int index = 1; index <= 7; index++) {
                    String value = String.valueOf(rs.getObject(index));
                    digest.update((value.length() + ":" + value).getBytes(StandardCharsets.UTF_8));
                }
            });
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("无法计算索引源清单摘要", exception);
        }
    }

    /** 将一组固定路由生成缓存隔离指纹。 */
    private Route route(String id, String collection, String model, String keyword, Integer dimension) {
        String fingerprint = DigestUtils.md5DigestAsHex((text(id) + "|" + text(collection) + "|" + text(model)
                + "|" + text(keyword) + "|" + dimension).getBytes(StandardCharsets.UTF_8));
        return new Route(id, collection, model, keyword, dimension, fingerprint);
    }

    /** 将可空数据库值转为安全字符串。 */
    private static String text(Object value) { return value == null ? "" : value.toString(); }

    /** 保留数据库中缺省字段的空值语义。 */
    private static String textOrNull(Object value) { return value == null ? null : value.toString(); }
}
