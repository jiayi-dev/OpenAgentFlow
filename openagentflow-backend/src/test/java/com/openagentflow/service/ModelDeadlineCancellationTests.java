package com.openagentflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openagentflow.domain.chat.ChatRunContext;
import com.openagentflow.workflow.WorkflowNodeDeadline;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** 节点超时必须取消底层 HTTP future，不能只结束等待线程。 */
class ModelDeadlineCancellationTests {

    /** 使用可观察未完成请求模拟长期等待的供应商。 */
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void deadlineCancelsUnderlyingHttpRequest() throws Exception {
        HttpClient http = mock(HttpClient.class);
        CompletableFuture<HttpResponse<String>> pending = new CompletableFuture<>();
        when(http.sendAsync(any(HttpRequest.class), any(HttpResponse.BodyHandler.class))).thenReturn((CompletableFuture) pending);
        OpenAiCompatibleClient client = new OpenAiCompatibleClient(new ObjectMapper());
        ReflectionTestUtils.setField(client, "httpClient", http);
        Method method = OpenAiCompatibleClient.class.getDeclaredMethod("sendCancelable", ChatRunContext.class,
                HttpRequest.class, HttpResponse.BodyHandler.class);
        method.setAccessible(true);
        try (WorkflowNodeDeadline deadline = new WorkflowNodeDeadline()) {
            assertThatThrownBy(() -> deadline.call(200, () -> {
                try {
                    return method.invoke(client, new ChatRunContext(), HttpRequest.newBuilder(URI.create("http://unused.test")).build(),
                            HttpResponse.BodyHandlers.ofString());
                } catch (InvocationTargetException exception) {
                    throw (Exception) exception.getCause();
                }
            })).hasMessageContaining("超时");
            // Future.cancel 通知与工作线程中断处理异步发生，仅等待取消确认。
            long until = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(1);
            while (!pending.isCancelled() && System.nanoTime() < until) Thread.onSpinWait();
            assertThat(pending.isCancelled()).isTrue();
        }
    }
}
