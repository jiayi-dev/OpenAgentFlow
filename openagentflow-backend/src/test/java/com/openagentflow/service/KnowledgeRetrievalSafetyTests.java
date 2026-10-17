package com.openagentflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openagentflow.config.OpenAgentFlowProperties;
import com.openagentflow.domain.knowledge.KnowledgeRetrievalRequest;
import com.openagentflow.domain.knowledge.KnowledgeRetrievalResult;
import com.openagentflow.domain.knowledge.KnowledgeSource;
import com.openagentflow.domain.knowledge.RerankResult;
import com.openagentflow.entity.KnowledgeBaseEntity;
import com.openagentflow.entity.KnowledgeChunkEntity;
import com.openagentflow.entity.KnowledgeDocumentEntity;
import com.openagentflow.mapper.KnowledgeBaseMapper;
import com.openagentflow.mapper.KnowledgeChunkMapper;
import com.openagentflow.mapper.KnowledgeDocumentMapper;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 热缓存与重排安全的真实服务回归测试。 */
class KnowledgeRetrievalSafetyTests {

    /** 热缓存不得把低置信拒答改为可回答。 */
    @Test
    void shouldKeepColdAndHotConfidenceDecisionsEquivalent() throws Exception {
        Fixture f = new Fixture();
        KnowledgeRetrievalResult cold = f.retrieve();
        f.cache(cold);
        KnowledgeRetrievalResult hot = f.retrieve();
        assertThat(cold.getLowConfidence()).isTrue();
        assertThat(hot.getLowConfidence()).isEqualTo(cold.getLowConfidence());
        assertThat(hot.getAnswerable()).isEqualTo(cold.getAnswerable());
        assertThat(hot.getRejectReason()).isEqualTo(cold.getRejectReason());
    }

    /** 停用的缓存分片必须重新召回，不得返回旧引用。 */
    @Test
    void shouldDiscardCacheWhenChunkBecomesInactive() throws Exception {
        Fixture f = new Fixture();
        f.cache(f.retrieve());
        f.chunk.setStatus("inactive");
        assertThat(f.retrieve().getSources()).isEmpty();
    }

    /** 缓存分片迁往其他知识库时必须失效。 */
    @Test
    void shouldDiscardCacheWhenChunkMovesToAnotherKnowledgeBase() throws Exception {
        Fixture f = new Fixture();
        f.cache(f.retrieve());
        f.chunk.setKbId("other-kb");
        assertThat(f.retrieve().getSources()).isEmpty();
    }

    /** 已删除文档不得通过缓存或重新召回泄露。 */
    @Test
    void shouldDiscardCacheWhenDocumentIsMissing() throws Exception {
        Fixture f = new Fixture();
        f.cache(f.retrieve());
        when(f.documents.selectById("doc")).thenReturn(null);
        assertThat(f.retrieve().getSources()).isEmpty();
    }

    /** 分片配置变更后缓存键必须变化，即使知识库ID未变。 */
    @Test
    void shouldInvalidateCacheKeyForKnowledgeBaseConfigurationChanges() throws Exception {
        Fixture f = new Fixture();
        f.retrieve();
        String previous = f.cacheKeys.getLast();
        f.kb.setChunkSize(987);
        f.retrieve();
        assertThat(f.cacheKeys.getLast()).isNotEqualTo(previous);
    }

    /** 文档元数据变化后不得复用旧过滤结果。 */
    @Test
    void shouldInvalidateCacheKeyForDocumentConfigurationChanges() throws Exception {
        Fixture f = new Fixture();
        f.retrieve();
        String previous = f.cacheKeys.getLast();
        f.document.setMetadata("{\"visible\":false}");
        f.retrieve();
        assertThat(f.cacheKeys.getLast()).isNotEqualTo(previous);
    }

    /** 原始召回顺序不同于分数顺序时，RRF排名必须来自排序后的列表。 */
    @Test
    void shouldPassSortedRanksToFusion() throws Exception {
        Fixture f = new Fixture();
        KnowledgeChunkEntity high = f.addChunk("high");
        when(f.keyword.isEnabled()).thenReturn(true);
        when(f.keyword.searchIndex(anyString(), anyString(), anyInt())).thenReturn(List.of(
                new KeywordSearchService.KeywordHit("low", 0.4D),
                new KeywordSearchService.KeywordHit("high", 0.8D)));
        when(f.chunks.selectBatchIds(anyCollection())).thenReturn(List.of(f.chunk, high));
        f.retrieve();
        var captor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(f.fusion).fuse(captor.capture(), anyInt());
        List<?> hits = (List<?>) captor.getValue().getFirst();
        assertThat(((MultiQueryFusionPolicy.RankedHit<?>) hits.getFirst()).id()).isEqualTo("high");
        assertThat(((MultiQueryFusionPolicy.RankedHit<?>) hits.getFirst()).rank()).isEqualTo(1);
    }

    /** 部分候选重排成功时，只能输出同一模型评分池，不能混排未评分候选。 */
    @Test
    void shouldNotMixPartialCrossEncoderPoolWithRuleScores() throws Exception {
        Fixture f = new Fixture();
        f.addChunk("tail");
        f.enableRerank(1);
        RerankResult reranked = new RerankResult();
        reranked.setSuccess(true);
        reranked.setMode("cross_encoder");
        reranked.setModelId("reranker");
        reranked.setScores(List.of(0.1D));
        when(f.rerank.rerank(anyString(), anyString(), anyList())).thenReturn(reranked);
        KnowledgeRetrievalResult result = f.retrieve();
        assertThat(result.getSources()).hasSize(1);
        assertThat(result.getSources().getFirst().getScore()).isEqualTo(0.1D);
    }

    /** 重排调用抛出异常时必须保留规则结果并返回可观察降级信息。 */
    @Test
    void shouldSafelyFallbackWhenCrossEncoderThrows() throws Exception {
        Fixture f = new Fixture();
        f.enableRerank(3);
        when(f.rerank.rerank(anyString(), anyString(), anyList())).thenThrow(new IllegalStateException("offline"));
        assertThatCode(f::retrieve).doesNotThrowAnyException();
        assertThat(f.retrieve().getRerankMode()).isEqualTo("rule_fallback");
        assertThat(f.retrieve().getSources()).isNotEmpty();
    }

    /** 构建真实检索服务，所有外部基础设施由内存夹具代替。 */
    private static final class Fixture {
        /** 数据访问与外部模型依赖。 */
        final KnowledgeBaseMapper bases = mock(KnowledgeBaseMapper.class);
        final KnowledgeChunkMapper chunks = mock(KnowledgeChunkMapper.class);
        final KnowledgeDocumentMapper documents = mock(KnowledgeDocumentMapper.class);
        final KeywordSearchService keyword = mock(KeywordSearchService.class);
        final CrossEncoderRerankService rerank = mock(CrossEncoderRerankService.class);
        final MultiQueryFusionPolicy fusion = spy(new MultiQueryFusionPolicy());
        final JdbcTemplate jdbc = mock(JdbcTemplate.class);
        final ObjectMapper json = new ObjectMapper().findAndRegisterModules();
        final OpenAgentFlowProperties properties = new OpenAgentFlowProperties();
        final KnowledgeBaseEntity kb = new KnowledgeBaseEntity();
        final KnowledgeDocumentEntity document = new KnowledgeDocumentEntity();
        final List<KnowledgeChunkEntity> recalled = new ArrayList<>();
        final List<String> cacheKeys = new ArrayList<>();
        final KnowledgeRetrievalRequest request = new KnowledgeRetrievalRequest();
        final KnowledgeChunkEntity chunk;
        final KnowledgeBaseService service;
        List<Map<String, Object>> cacheRows = List.of();

        /** 兼容父任务正在增加的路由构造参数，且始终使用真实路由解析器。 */
        Fixture() throws Exception {
            kb.setId("kb");
            kb.setStatus("active");
            kb.setMilvusCollectionName("kb_base");
            document.setId("doc");
            document.setKbId("kb");
            document.setParseStatus("parsed");
            document.setMetadata("{}");
            when(bases.selectById("kb")).thenReturn(kb);
            when(documents.selectById("doc")).thenReturn(document);
            when(documents.selectList(any())).thenReturn(List.of(document));
            when(jdbc.queryForMap(anyString(), eq("kb"))).thenAnswer(call -> Map.of("document_revision", document.getMetadata()));
            when(chunks.selectList(any())).thenAnswer(call -> recalled);
            when(jdbc.queryForList(anyString(), any(Object[].class))).thenAnswer(call -> {
                String sql = call.getArgument(0);
                if (sql.contains("knowledge_retrieval_cache")) {
                    cacheKeys.add(call.getArgument(1));
                    return cacheRows;
                }
                return List.of();
            });
            chunk = addChunk("low");
            request.setQuery("unmatched query");
            request.setSearchMode("keyword");
            request.setTopK(3);
            request.setCandidateK(6);
            request.setScoreThreshold(0D);
            request.setLowConfidenceThreshold(0.95D);
            request.setRejectLowConfidence(true);
            request.setQueryRewriteEnabled(false);
            request.setMultiQueryEnabled(false);
            request.setRerankEnabled(false);
            Map<Class<?>, Object> dependencies = new HashMap<>();
            dependencies.put(KnowledgeBaseMapper.class, bases);
            dependencies.put(KnowledgeChunkMapper.class, chunks);
            dependencies.put(KnowledgeDocumentMapper.class, documents);
            dependencies.put(KeywordSearchService.class, keyword);
            dependencies.put(CrossEncoderRerankService.class, rerank);
            dependencies.put(MultiQueryFusionPolicy.class, fusion);
            dependencies.put(JdbcTemplate.class, jdbc);
            dependencies.put(ObjectMapper.class, json);
            dependencies.put(OpenAgentFlowProperties.class, properties);
            dependencies.put(QueryEnhancementPolicy.class, new QueryEnhancementPolicy());
            WorkspaceGovernanceService access = mock(WorkspaceGovernanceService.class);
            when(access.canViewResource(anyString(), anyString(), nullable(String.class), nullable(String.class), nullable(String.class), nullable(String.class)))
                    .thenReturn(true);
            dependencies.put(WorkspaceGovernanceService.class, access);
            var constructor = KnowledgeBaseService.class.getConstructors()[0];
            List<Object> arguments = new ArrayList<>();
            for (Class<?> type : constructor.getParameterTypes()) {
                if (type.getSimpleName().equals("KnowledgeIndexRouteService")) {
                    arguments.add(type.getConstructor(JdbcTemplate.class).newInstance(jdbc));
                } else {
                    arguments.add(dependencies.computeIfAbsent(type, ignored -> mock(type)));
                }
            }
            service = (KnowledgeBaseService) constructor.newInstance(arguments.toArray());
        }

        /** 加入可检索分片及对应数据库主键查询。 */
        KnowledgeChunkEntity addChunk(String id) {
            KnowledgeChunkEntity value = new KnowledgeChunkEntity();
            value.setId(id);
            value.setKbId("kb");
            value.setDocumentId("doc");
            value.setStatus("active");
            value.setContent("background material");
            value.setTokenCount(30);
            recalled.add(value);
            when(chunks.selectById(id)).thenReturn(value);
            return value;
        }

        /** 生成与数据库缓存行相同的持久化载荷。 */
        void cache(KnowledgeRetrievalResult result) throws Exception {
            cacheRows = List.of(Map.of("sources_json", json.writeValueAsString(result.getSources()),
                    "confidence_score", result.getConfidenceScore()));
        }

        /** 设置受限的真实模型重排池。 */
        void enableRerank(int limit) {
            kb.setRerankModelId("reranker");
            request.setRerankEnabled(true);
            properties.getRag().setRerankCandidateLimit(limit);
        }

        /** 通过公开入口执行完整检索。 */
        KnowledgeRetrievalResult retrieve() {
            return service.retrievalTest("kb", request);
        }
    }
}
