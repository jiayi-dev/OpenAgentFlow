package com.openagentflow.service;

import com.openagentflow.exception.BusinessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 知识库蓝绿索引版本治理服务。
 *
 * <p>构建时写入带版本号的物理集合，激活后由稳定别名承接查询，旧版本保留至回滚窗口结束。</p>
 */
@Service
public class KnowledgeIndexGovernanceService {

    /** 别名补偿失败需要进入运维日志，但不能覆盖数据库回滚的原始异常。 */
    private static final Logger log = LoggerFactory.getLogger(KnowledgeIndexGovernanceService.class);

    /** JDBC工具。 */
    private final JdbcTemplate jdbcTemplate;

    /** Milvus集合发布服务。 */
    private final MilvusKnowledgeVectorService milvusService;

    /** 异步任务服务，用于提交索引构建。 */
    private final AsyncTaskService asyncTaskService;

    /** 同一次激活使用的权威路由与源清单校验器。 */
    private final KnowledgeIndexRouteService indexRoutes;

    public KnowledgeIndexGovernanceService(JdbcTemplate jdbcTemplate,
                                           MilvusKnowledgeVectorService milvusService,
                                           AsyncTaskService asyncTaskService) {
        this(jdbcTemplate, milvusService, asyncTaskService, new KnowledgeIndexRouteService(jdbcTemplate));
    }

    @Autowired
    public KnowledgeIndexGovernanceService(JdbcTemplate jdbcTemplate,
                                           MilvusKnowledgeVectorService milvusService,
                                           AsyncTaskService asyncTaskService,
                                           KnowledgeIndexRouteService indexRoutes) {
        this.jdbcTemplate = jdbcTemplate;
        this.milvusService = milvusService;
        this.asyncTaskService = asyncTaskService;
        this.indexRoutes = indexRoutes;
    }

    /** 创建下一版索引元数据。 */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> createVersion(String kbId) {
        Map<String, Object> kb;
        try {
            kb = jdbcTemplate.queryForMap("SELECT id, workspace_id, milvus_collection_name, embedding_model_id FROM knowledge_base WHERE id=? FOR UPDATE", kbId);
        } catch (Exception exception) {
            throw new BusinessException("KNOWLEDGE_BASE_NOT_FOUND", "知识库不存在");
        }
        Integer version = jdbcTemplate.queryForObject("SELECT COALESCE(MAX(version_no),0)+1 FROM knowledge_index_version WHERE kb_id=?", Integer.class, kbId);
        int versionNo = version == null ? 1 : version;
        String alias = text(kb.get("milvus_collection_name"));
        if (alias.isBlank()) {
            alias = "kb_" + kbId.replace("-", "");
        }
        // 默认集合可能被多个知识库共用，版本别名必须加入知识库身份。
        alias = alias.replaceAll("[^a-zA-Z0-9_]", "_");
        alias = alias.substring(0, Math.min(alias.length(), 120)) + "_kb_" + kbId.replace("-", "");
        String physical = alias + "_v" + versionNo;
        String id = UUID.randomUUID().toString();
        jdbcTemplate.update("""
                INSERT INTO knowledge_index_version
                  (id,kb_id,workspace_id,version_no,collection_name,collection_alias,keyword_index_name,embedding_model_id,status,created_at)
                VALUES (?,?,?,?,?,?,?,?, 'building', NOW(3))
                """, id, kbId, kb.get("workspace_id"), versionNo, physical, alias,
                "oaf-kb-" + kbId + "-v" + versionNo, kb.get("embedding_model_id"));
        var task = asyncTaskService.createTask("构建知识库索引版本v" + versionNo, "KNOWLEDGE_INDEX_BUILD",
                "knowledge_index_version", id, "knowledge_index_version", id, text(kb.get("workspace_id")),
                Map.of("versionId", id, "kbId", kbId, "collectionName", physical));
        Map<String, Object> result = new java.util.LinkedHashMap<>(jdbcTemplate.queryForMap("SELECT * FROM knowledge_index_version WHERE id=?", id));
        result.put("asyncTaskId", task.getId());
        return result;
    }

    /** 串行发布数据库路由，远端别名作为可补偿投影，查询不依赖未提交别名。 */
    @Transactional(rollbackFor = Exception.class)
    public Map<String, Object> activate(String versionId) {
        Map<String, Object> initial = jdbcTemplate.queryForMap("SELECT * FROM knowledge_index_version WHERE id=?", versionId);
        String kbId = text(initial.get("kb_id"));
        Map<String, Object> kb = jdbcTemplate.queryForMap("SELECT * FROM knowledge_base WHERE id=? FOR UPDATE", kbId);
        Map<String, Object> version = jdbcTemplate.queryForMap("SELECT * FROM knowledge_index_version WHERE id=? FOR UPDATE", versionId);
        String status = text(version.get("status"));
        if (!List.of("ready", "active", "retired").contains(status)) {
            throw new BusinessException("INDEX_VERSION_NOT_READY", "只有已就绪索引版本可以激活");
        }
        if (!StringUtils.hasText(text(version.get("source_fingerprint")))) {
            throw new BusinessException("INDEX_REBUILD_REQUIRED", "索引未保存源清单，请重新构建后激活");
        }
        String currentFingerprint = indexRoutes.sourceFingerprint(kbId, text(version.get("embedding_model_id")));
        if (!currentFingerprint.equals(version.get("source_fingerprint"))) {
            throw new BusinessException("INDEX_SOURCE_CHANGED", "知识库内容已变化，请重新构建索引后激活");
        }
        Number processing = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM knowledge_document WHERE kb_id=? AND parse_status IN ('pending','processing')", Long.class, kbId);
        if (processing != null && processing.longValue() > 0) {
            throw new BusinessException("INDEX_SOURCE_PROCESSING", "知识库仍有文档处理中，暂不能切换索引");
        }
        String collectionName = text(version.get("collection_name"));
        String alias = text(version.get("collection_alias"));
        Number dimension = version.get("dimension") instanceof Number number ? number : null;
        if (dimension == null || dimension.intValue() <= 0) {
            throw new BusinessException("INDEX_DIMENSION_REQUIRED", "索引缺少有效向量维度，请重新构建");
        }
        String physicalCollection = KnowledgeIndexRouteService.physicalCollection(collectionName, dimension.intValue());
        List<Map<String, Object>> previous = jdbcTemplate.queryForList("SELECT * FROM knowledge_index_version WHERE kb_id=? AND status='active' ORDER BY version_no DESC", kbId);
        String previousCollection = previous.isEmpty()
                ? KnowledgeIndexRouteService.physicalCollection(text(kb.get("milvus_collection_name")), dimension.intValue())
                : KnowledgeIndexRouteService.physicalCollection(text(previous.getFirst().get("collection_name")),
                        ((Number) previous.getFirst().get("dimension")).intValue());
        // 先注册回滚补偿，覆盖远端已执行但返回超时、数据库提交失败等窗口。
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCompletion(int completionStatus) {
                    if (completionStatus != STATUS_COMMITTED) {
                        try {
                            milvusService.activateAlias(previousCollection, alias);
                        } catch (Exception exception) {
                            log.error("索引别名回滚补偿失败，数据库物理路由仍保持原版本：kbId={}, alias={}, target={}", kbId, alias, previousCollection, exception);
                        }
                    }
                }
            });
        }
        milvusService.activateAlias(physicalCollection, alias);
        jdbcTemplate.update("UPDATE knowledge_index_version SET status='retired', retired_at=NOW(3) WHERE kb_id=? AND status='active' AND id<>?", kbId, versionId);
        jdbcTemplate.update("UPDATE knowledge_index_version SET status='active', activated_at=NOW(3), retired_at=NULL WHERE id=?", versionId);
        jdbcTemplate.update("DELETE FROM knowledge_retrieval_cache WHERE kb_id=?", kbId);
        return jdbcTemplate.queryForMap("SELECT * FROM knowledge_index_version WHERE id=?", versionId);
    }

    /** 查询知识库全部索引版本。 */
    public List<Map<String, Object>> list(String kbId) {
        return jdbcTemplate.queryForList("SELECT * FROM knowledge_index_version WHERE kb_id=? ORDER BY version_no DESC", kbId);
    }

    /** 标记索引构建完成并写入容量数据。 */
    public Map<String, Object> markReady(String versionId, int dimension, long chunkCount) {
        Map<String, Object> version = jdbcTemplate.queryForMap("SELECT * FROM knowledge_index_version WHERE id=?", versionId);
        // 手工接口只能读取真实构建结果，不能绕过构建与源清单校验伪造 ready。
        if (!StringUtils.hasText(text(version.get("source_fingerprint")))
                || !(version.get("dimension") instanceof Number actualDimension)
                || actualDimension.intValue() != dimension
                || !(version.get("chunk_count") instanceof Number actualCount)
                || actualCount.longValue() != chunkCount) {
            throw new BusinessException("INDEX_BUILD_NOT_VERIFIED", "请等待异步索引真实构建完成，不能手工覆盖校验结果");
        }
        return version;
    }

    private String text(Object value) { return value == null ? "" : String.valueOf(value); }
}
