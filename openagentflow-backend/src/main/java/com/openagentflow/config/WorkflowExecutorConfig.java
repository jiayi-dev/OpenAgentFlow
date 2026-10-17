package com.openagentflow.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.task.DelegatingSecurityContextAsyncTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/** 工作流并行分支执行器配置。 */
@Configuration
public class WorkflowExecutorConfig {

    /**
     * 创建有界工作流执行器，并传播 Spring Security 上下文。
     *
     * @return 工作流并行执行器
     */
    @Bean("workflowParallelExecutor")
    public Executor workflowParallelExecutor() {
        int processors = Math.max(2, Runtime.getRuntime().availableProcessors());
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(Math.min(8, processors));
        executor.setMaxPoolSize(Math.min(32, processors * 2));
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("oaf-workflow-");
        executor.setTaskDecorator(new ExecutionContextTaskDecorator());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return new DelegatingSecurityContextAsyncTaskExecutor(executor);
    }
}

