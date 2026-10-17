package com.openagentflow.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.lang.reflect.Array;
import java.util.*;

/** 深层复制工作流数据，隔离分支、插件与超时后的迟到写入。 */
public final class WorkflowContextSnapshots {
    private WorkflowContextSnapshots() { }

    /** 复制上下文并保留嵌套数据的独立所有权。 */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> copy(Map<String, Object> value, ObjectMapper mapper) {
        return (Map<String, Object>) copyValue(value, mapper, new IdentityHashMap<>());
    }

    /** 对 JSON 容器、数组、日期和 DTO 逐层复制，不共享可变对象。 */
    private static Object copyValue(Object value, ObjectMapper mapper, Map<Object, Object> visited) {
        if (value == null || value instanceof String || value instanceof Integer || value instanceof Long
                || value instanceof Double || value instanceof Float || value instanceof Short || value instanceof Byte
                || value instanceof java.math.BigDecimal || value instanceof java.math.BigInteger || value instanceof Boolean
                || value instanceof Character || value instanceof Enum<?> || value instanceof UUID
                || value.getClass().getPackageName().equals("java.time")) return value;
        if (visited.containsKey(value)) return visited.get(value);
        if (value instanceof Map<?, ?> source) {
            Map<Object, Object> target = new LinkedHashMap<>();
            visited.put(value, target);
            source.forEach((key, item) -> target.put(key, copyValue(item, mapper, visited)));
            return target;
        }
        if (value instanceof Collection<?> source) {
            Collection<Object> target = value instanceof Set<?> ? new LinkedHashSet<>() : new ArrayList<>();
            visited.put(value, target);
            source.forEach(item -> target.add(copyValue(item, mapper, visited)));
            return target;
        }
        if (value.getClass().isArray()) {
            Object target = Array.newInstance(value.getClass().getComponentType(), Array.getLength(value));
            visited.put(value, target);
            for (int i = 0; i < Array.getLength(value); i++) Array.set(target, i, copyValue(Array.get(value, i), mapper, visited));
            return target;
        }
        if (value instanceof Date date) return date.clone();
        if (value instanceof JsonNode json) return json.deepCopy();
        try {
            return mapper.convertValue(value, value.getClass());
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("工作流上下文无法安全隔离：" + value.getClass().getName(), exception);
        }
    }
}
