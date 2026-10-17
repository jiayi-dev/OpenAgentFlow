package com.openagentflow.service;

import com.openagentflow.config.OpenAgentFlowProperties;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.collection.HasCollectionParam;
import io.milvus.param.dml.SearchParam;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** 检索只读路由回归测试。 */
class KnowledgeIndexReadRoutingTests {

    /** 查询失败不能在错误名称下自动创建空集合掩盖版本错误。 */
    @Test
    @SuppressWarnings("unchecked")
    void shouldSearchExactPhysicalCollectionWithoutCreatingIt() {
        MilvusServiceClient client = mock(MilvusServiceClient.class);
        ObjectProvider<MilvusServiceClient> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(client);
        when(client.search(any(SearchParam.class))).thenThrow(new IllegalStateException("search-probe"));
        MilvusKnowledgeVectorService service = new MilvusKnowledgeVectorService(provider, new OpenAgentFlowProperties());

        assertThatThrownBy(() -> service.searchKnowledgeChunks("kb_v2_d3", "kb", List.of(1D, 0D, 0D), 5))
                .isInstanceOf(IllegalStateException.class).hasMessage("search-probe");

        verify(client, never()).hasCollection(any(HasCollectionParam.class));
        verify(client).search(argThat((SearchParam param) -> "kb_v2_d3".equals(param.getCollectionName())));
    }
}
