package com.openagentflow.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Agent Runtime 独立执行面线程配置。
 *
 * <p>使用有界队列和拒绝策略隔离模型调用，避免流式会话占满Web容器或JDK公共线程池。</p>
 */
@Configuration
public class RuntimeExecutionConfig {

    /**
     * 创建Runtime专用执行器。
     *
     * @param coreSize 核心线程数
     * @param maxSize 最大线程数
     * @param queueCapacity 等待队列容量
     * @return Runtime任务执行器
     */
    @Bean("agentRuntimeExecutor")
    public TaskExecutor agentRuntimeExecutor(
            @Value("${openagentflow.runtime.executor.core-size:16}") int coreSize,
            @Value("${openagentflow.runtime.executor.max-size:64}") int maxSize,
            @Value("${openagentflow.runtime.executor.queue-capacity:200}") int queueCapacity) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(Math.max(2, coreSize));
        executor.setMaxPoolSize(Math.max(coreSize, maxSize));
        executor.setQueueCapacity(Math.max(10, queueCapacity));
        executor.setThreadNamePrefix("oaf-runtime-");
        // 把HTTP请求ID复制到Runtime异步线程，确保SSE、LLM、RAG和SQL日志可按同一请求检索。
        executor.setTaskDecorator(new ExecutionContextTaskDecorator());
        executor.setWaitForTasksToCompleteOnShutdown(false);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }
}
