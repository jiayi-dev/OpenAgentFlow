-- 索引发布必须关联实际构建源清单；旧版本需要重新构建后才能安全激活。
ALTER TABLE knowledge_index_version
  ADD COLUMN source_fingerprint varchar(64) DEFAULT NULL COMMENT '索引构建源清单SHA-256摘要，用于激活前一致性校验',
  ADD COLUMN built_at datetime(3) DEFAULT NULL COMMENT '索引完成源清单核验及构建的时间';
