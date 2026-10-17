package com.openagentflow.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/** 索引激活的事务回滚与串行化回归测试。 */
class KnowledgeIndexActivationTests {

    /** 源文档变化时必须先重建，不能发布过期向量或执行别名切换。 */
    @Test
    void rejectsChangedSourceBeforeAliasMutation() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        MilvusKnowledgeVectorService milvus = mock(MilvusKnowledgeVectorService.class);
        KnowledgeIndexRouteService routes = mock(KnowledgeIndexRouteService.class);
        when(jdbc.queryForMap(anyString(), eq("v2"))).thenReturn(Map.of("id", "v2", "kb_id", "kb", "status", "ready",
                "collection_name", "kb_v2", "collection_alias", "kb_alias", "dimension", 3,
                "embedding_model_id", "m1", "source_fingerprint", "before"));
        when(routes.sourceFingerprint("kb", "m1")).thenReturn("after");
        KnowledgeIndexGovernanceService service = new KnowledgeIndexGovernanceService(jdbc, milvus, mock(AsyncTaskService.class), routes);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> service.activate("v2")).hasMessageContaining("内容已变化");
        verifyNoInteractions(milvus);
    }

    /** 原手工 ready 接口不能伪造异步构建结果。 */
    @Test
    void rejectsUnverifiedManualReady() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForMap(anyString(), eq("v2"))).thenReturn(Map.of("id", "v2", "status", "building"));
        KnowledgeIndexGovernanceService service = new KnowledgeIndexGovernanceService(jdbc,
                mock(MilvusKnowledgeVectorService.class), mock(AsyncTaskService.class));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> service.markReady("v2", 3, 10)).hasMessageContaining("真实构建");
    }

    /** 清理测试手动建立的事务同步状态。 */
    @AfterEach
    void clearSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    /** 数据库回滚后必须恢复原别名，不能留下远端投影单独切换。 */
    @Test
    void shouldCompensateAliasWhenDatabaseRollsBack() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        MilvusKnowledgeVectorService milvus = mock(MilvusKnowledgeVectorService.class);
        Map<String, Object> next = Map.of("id", "v2", "kb_id", "kb", "status", "ready",
                "collection_name", "kb_v2", "collection_alias", "kb_alias", "dimension", 3,
                "embedding_model_id", "m1", "source_fingerprint", "snapshot");
        when(jdbc.queryForMap(anyString(), eq("v2"))).thenReturn(next);
        when(jdbc.queryForMap(anyString(), eq("kb"))).thenReturn(Map.of("id", "kb", "milvus_collection_name", "kb_alias"));
        when(jdbc.queryForList(anyString(), eq("kb"))).thenReturn(List.of(Map.of(
                "id", "v1", "collection_name", "kb_v1", "dimension", 3, "embedding_model_id", "m1")));
        TransactionSynchronizationManager.initSynchronization();
        KnowledgeIndexRouteService routes = mock(KnowledgeIndexRouteService.class);
        when(routes.sourceFingerprint("kb", "m1")).thenReturn("snapshot");
        KnowledgeIndexGovernanceService service = new KnowledgeIndexGovernanceService(jdbc, milvus, mock(AsyncTaskService.class), routes);

        service.activate("v2");

        assertThat(TransactionSynchronizationManager.getSynchronizations()).isNotEmpty();
        TransactionSynchronizationManager.getSynchronizations().forEach(
                sync -> sync.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));
        verify(milvus).activateAlias("kb_v2_d3", "kb_alias");
        verify(milvus).activateAlias("kb_v1_d3", "kb_alias");
    }

    /** 不同版本并发激活时必须竞争同一知识库锁。 */
    @Test
    void shouldLockKnowledgeBaseBeforeChangingActiveVersion() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForMap(anyString(), eq("v2"))).thenReturn(Map.of("id", "v2", "kb_id", "kb", "status", "ready",
                "collection_name", "kb_v2", "collection_alias", "kb_alias", "dimension", 3,
                "embedding_model_id", "m1", "source_fingerprint", "snapshot"));
        when(jdbc.queryForMap(anyString(), eq("kb"))).thenReturn(Map.of("id", "kb", "milvus_collection_name", "kb_alias"));
        KnowledgeIndexRouteService routes = mock(KnowledgeIndexRouteService.class);
        when(routes.sourceFingerprint("kb", "m1")).thenReturn("snapshot");
        new KnowledgeIndexGovernanceService(jdbc, mock(MilvusKnowledgeVectorService.class), mock(AsyncTaskService.class), routes).activate("v2");

        verify(jdbc).queryForMap(contains("knowledge_base WHERE id=? FOR UPDATE"), eq("kb"));
    }
}
