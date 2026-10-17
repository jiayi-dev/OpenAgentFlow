package com.openagentflow.service;

import com.openagentflow.entity.KnowledgeBaseEntity;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** 权威路由快照必须同时绑定模型、物理集合与关键词版本。 */
class KnowledgeIndexRouteServiceTests {

    /** 未发布版本的知识库继续兼容旧集合，不解析别名。 */
    @Test
    void legacyRouteRemainsCompatible() {
        KnowledgeBaseEntity kb = base();
        KnowledgeIndexRouteService.Route route = new KnowledgeIndexRouteService(mock(JdbcTemplate.class)).resolve(kb);
        assertThat(route.versionId()).isEmpty();
        assertThat(route.collectionName()).isEqualTo("legacy");
        assertThat(route.embeddingModelId()).isEqualTo("configured-model");
        assertThat(route.keywordIndexName()).isEqualTo("oaf-kb-kb");
    }

    /** 修改知识库模型配置不能让旧已发布向量被新模型查询。 */
    @Test
    void publishedRouteUsesFrozenModelAndExactPhysicalCollection() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq("kb"))).thenReturn(List.of(version("v2", "ws")));
        KnowledgeIndexRouteService.Route route = new KnowledgeIndexRouteService(jdbc).resolve(base());
        assertThat(route.collectionName()).isEqualTo("kb_v2_d3");
        assertThat(route.embeddingModelId()).isEqualTo("frozen-model");
        assertThat(route.keywordIndexName()).isEqualTo("oaf-kb-kb-v2");
        assertThat(KnowledgeIndexRouteService.physicalCollection(route.collectionName(), 3)).isEqualTo("kb_v2_d3");
    }

    /** 切换版本必须改变缓存路由指纹。 */
    @Test
    void versionChangeInvalidatesCacheFingerprint() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq("kb"))).thenReturn(List.of(version("v1", "ws")), List.of(version("v2", "ws")));
        KnowledgeIndexRouteService service = new KnowledgeIndexRouteService(jdbc);
        assertThat(service.resolve(base()).cacheFingerprint()).isNotEqualTo(service.resolve(base()).cacheFingerprint());
    }

    /** 损坏的跨空间版本不能作为合法读路由。 */
    @Test
    void crossWorkspaceVersionFailsClosed() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq("kb"))).thenReturn(List.of(version("v2", "another-space")));
        assertThatThrownBy(() -> new KnowledgeIndexRouteService(jdbc).resolve(base())).hasMessageContaining("工作空间不一致");
    }

    /** 同一知识库有多个 active 时不能任意挑一个掩盖发布错误。 */
    @Test
    void multipleActiveVersionsFailClosed() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq("kb"))).thenReturn(List.of(version("v1", "ws"), version("v2", "ws")));
        assertThatThrownBy(() -> new KnowledgeIndexRouteService(jdbc).resolve(base())).hasMessageContaining("多个激活索引");
    }

    /** 生成当前配置与已发布配置不同的知识库。 */
    private KnowledgeBaseEntity base() {
        KnowledgeBaseEntity kb = new KnowledgeBaseEntity();
        kb.setId("kb");
        kb.setWorkspaceId("ws");
        kb.setMilvusCollectionName("legacy");
        kb.setEmbeddingModelId("configured-model");
        return kb;
    }

    /** 构造固定的索引版本元数据。 */
    private Map<String, Object> version(String id, String workspace) {
        return Map.of("id", id, "workspace_id", workspace, "collection_name", "kb_v2_d3",
                "embedding_model_id", "frozen-model", "keyword_index_name", "oaf-kb-kb-v2", "dimension", 3);
    }
}
