-- ========================================
-- DATABASE INITIALIZATION
-- ========================================
-- File: DB0204_fixed.sql
-- Date: 2026-02-04
-- Description: Financial App Review ETL Pipeline Schema (Redesigned)
-- Architecture: Hybrid Storage (PostgreSQL + NAS Parquet)

-- ========================================
-- EXTENSIONS
-- ========================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";
CREATE EXTENSION IF NOT EXISTS "ltree";

-- ========================================
-- ENUM TYPES
-- ========================================

CREATE TYPE platform_type AS ENUM ('APPSTORE', 'PLAYSTORE');
CREATE TYPE app_type AS ENUM ('CONSUMER', 'CORPORATE', 'GLOBAL');
CREATE TYPE analysis_status_type AS ENUM ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED');
CREATE TYPE processing_status_type AS ENUM ('RAW', 'CLEANED', 'ANALYZED', 'FAILED');

-- ========================================
-- TABLE DEFINITIONS
-- ========================================

-- ----------------------------------------
-- App Service (Logical Service Master)
-- ----------------------------------------

CREATE TABLE app_service
(
  service_id   UUID NOT NULL,
  service_name TEXT,
  PRIMARY KEY (service_id)
);

COMMENT ON TABLE app_service IS '논리적 서비스 마스터';
COMMENT ON COLUMN app_service.service_id IS 'Logical Service ID';
COMMENT ON COLUMN app_service.service_name IS '논리적 기준점';

-- ----------------------------------------
-- Apps (Physical App Instances)
-- ----------------------------------------

CREATE TABLE apps
(
  app_id          UUID         NOT NULL,
  platform_app_id TEXT         NOT NULL,
  platform_type   platform_type,
  name            TEXT         NOT NULL,
  PRIMARY KEY (app_id)
);

COMMENT ON TABLE apps IS '물리적 앱 인스턴스';
COMMENT ON COLUMN apps.app_id IS '시스템 고유 식별자';
COMMENT ON COLUMN apps.platform_app_id IS 'store ID, package name';
COMMENT ON COLUMN apps.platform_type IS 'PLAYSTORE / APPSTORE';
COMMENT ON COLUMN apps.name IS '스토어에 표시된 앱 이름';

-- ----------------------------------------
-- App Metadata (Connection & History)
-- ----------------------------------------

CREATE TABLE app_metadata
(
  id         INT      NOT NULL GENERATED ALWAYS AS IDENTITY,
  app_id     UUID     NOT NULL,
  service_id UUID     NOT NULL,
  group_id   TEXT,
  group_type TEXT,
  app_type   app_type,
  valid_from DATE,
  valid_to   DATE,
  is_active  BOOLEAN,
  PRIMARY KEY (id)
);

COMMENT ON TABLE app_metadata IS '연결 및 이력 관리 (SCD Type 2)';
COMMENT ON COLUMN app_metadata.app_id IS '시스템 고유 식별자';
COMMENT ON COLUMN app_metadata.service_id IS 'Logical Service ID';
COMMENT ON COLUMN app_metadata.group_id IS 'e.g., 우리금융그룹';
COMMENT ON COLUMN app_metadata.group_type IS 'e.g., 시중은행';
COMMENT ON COLUMN app_metadata.app_type IS 'CONSUMER / CORPORATE / GLOBAL 등';

-- ----------------------------------------
-- Review Master Index (Central Index)
-- ----------------------------------------

CREATE TABLE review_master_index
(
  review_id          UUID                   NOT NULL,
  app_id             UUID                   NOT NULL,
  service_id         UUID,
  platform_review_id TEXT                   NOT NULL UNIQUE,
  platform_type      platform_type,
  review_created_at  TIMESTAMPTZ,
  ingested_at        TIMESTAMPTZ,
  processing_status  processing_status_type,
  is_active          BOOLEAN,
  is_reply           BOOLEAN,
  PRIMARY KEY (review_id)
);

COMMENT ON TABLE review_master_index IS '리뷰 중앙 인덱스 (DB)';
COMMENT ON COLUMN review_master_index.review_id IS 'Global ID (UUID v7)';
COMMENT ON COLUMN review_master_index.app_id IS '각 앱 버전 고유 ID';
COMMENT ON COLUMN review_master_index.service_id IS '논리적 동일 앱 ID';
COMMENT ON COLUMN review_master_index.platform_review_id IS '플랫폼 원본 리뷰 ID (중복 방지)';
COMMENT ON COLUMN review_master_index.platform_type IS 'PLAYSTORE | APPSTORE';
COMMENT ON COLUMN review_master_index.processing_status IS 'RAW / CLEANED / ANALYZED / FAILED';
COMMENT ON COLUMN review_master_index.is_active IS 'T / F';
COMMENT ON COLUMN review_master_index.is_reply IS 'T / F';

-- ----------------------------------------
-- App Reviews (Bronze - NAS Parquet)
-- ----------------------------------------
review_llm_analysis_logs
CREATE TABLE app_reviews
(
  review_id          UUID         NOT NULL,
  app_id             UUID         NOT NULL,
  platform_type      platform_type NOT NULL,
  country_code       TEXT         NOT NULL DEFAULT 'kr',
  platform_review_id TEXT         NOT NULL,
  reviewer_name      TEXT,
  review_text        TEXT         NOT NULL,
  rating             SMALLINT     NOT NULL,
  app_version        VARCHAR,
  reviewed_at        TIMESTAMPTZ  NOT NULL,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  is_reply           BOOLEAN,
  reply_comment      TEXT
);

COMMENT ON TABLE app_reviews IS '리뷰 원본 데이터 (Bronze), NAS Parquet 저장';
COMMENT ON COLUMN app_reviews.review_id IS 'Global ID (UUID v7)';
COMMENT ON COLUMN app_reviews.app_id IS '앱 ID';
COMMENT ON COLUMN app_reviews.platform_type IS 'PLAYSTORE / APPSTORE';
COMMENT ON COLUMN app_reviews.country_code IS 'MVP에서 kr로 고정 (kr, us, uk ...)';
COMMENT ON COLUMN app_reviews.platform_review_id IS '해당 스토어의 리뷰 고유 ID';
COMMENT ON COLUMN app_reviews.reviewer_name IS '작성자 이름';
COMMENT ON COLUMN app_reviews.review_text IS '리뷰 원본 텍스트';
COMMENT ON COLUMN app_reviews.rating IS '별점 (1-5)';
COMMENT ON COLUMN app_reviews.app_version IS '앱 버전';
COMMENT ON COLUMN app_reviews.reviewed_at IS '리뷰 작성 시간';
COMMENT ON COLUMN app_reviews.is_reply IS 'T / F';

-- ----------------------------------------
-- Reviews Preprocessed (Silver - NAS Parquet)
-- ----------------------------------------

CREATE TABLE reviews_preprocessed
(
  review_id          UUID        NOT NULL,
  platform_review_id TEXT        NOT NULL UNIQUE,
  refined_text       TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE reviews_preprocessed IS '전처리 후 데이터 (Silver), NAS Parquet 저장';
COMMENT ON COLUMN reviews_preprocessed.review_id IS 'Global ID (UUID v7)';
COMMENT ON COLUMN reviews_preprocessed.platform_review_id IS '플랫폼 원본 리뷰 ID';
COMMENT ON COLUMN reviews_preprocessed.refined_text IS '전처리된 텍스트';
COMMENT ON COLUMN reviews_preprocessed.created_at IS '생성일';
COMMENT ON COLUMN reviews_preprocessed.updated_at IS '수정일';

-- ----------------------------------------
-- Review Embeddings (Silver - DB)
-- ----------------------------------------

CREATE TABLE review_embeddings
(
  review_id           UUID        NOT NULL,
  source_content_type VARCHAR     NOT NULL,
  model_name          VARCHAR,
  vector              VECTOR(1536),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (review_id)
);

COMMENT ON TABLE review_embeddings IS '임베딩 벡터 (Silver - DB)';
COMMENT ON COLUMN review_embeddings.review_id IS 'Global ID (UUID v7)';
COMMENT ON COLUMN review_embeddings.source_content_type IS '임베딩 소스 (raw / preprocessed / TBD)';
COMMENT ON COLUMN review_embeddings.model_name IS '임베딩 모델명 (e.g., text-embedding-3-small)';
COMMENT ON COLUMN review_embeddings.vector IS '임베딩 벡터 (1536 dimensions)';
COMMENT ON COLUMN review_embeddings.created_at IS '생성일';
COMMENT ON COLUMN review_embeddings.updated_at IS '수정일';

-- ----------------------------------------
-- Review Aspects (Silver - DB)
-- ----------------------------------------

CREATE TABLE review_aspects
(
  aspect_id       BIGINT  NOT NULL GENERATED ALWAYS AS IDENTITY,
  review_id       UUID    NOT NULL,
  keyword         TEXT,
  sentiment_score FLOAT,
  category        TEXT,
  PRIMARY KEY (aspect_id)
);

COMMENT ON TABLE review_aspects IS '애스펙트 기반 감성 분석 (Silver - DB)';
COMMENT ON COLUMN review_aspects.aspect_id IS '애스펙트 고유 ID';
COMMENT ON COLUMN review_aspects.review_id IS 'Global ID (UUID v7)';
COMMENT ON COLUMN review_aspects.keyword IS '키워드';
COMMENT ON COLUMN review_aspects.sentiment_score IS '감성 점수 (0.0 ~ 1.0)';
COMMENT ON COLUMN review_aspects.category IS '카테고리';

-- ----------------------------------------
-- Review Action Analysis (Silver - DB)
-- ----------------------------------------

CREATE TABLE review_action_analysis
(
  review_id               UUID    NOT NULL,
  is_action_required      BOOLEAN,
  action_confidence_score FLOAT,
  trigger_reason          TEXT,
  is_attention_required   BOOLEAN,
  is_verified             BOOLEAN,
  analyzed_at             TIMESTAMPTZ,
  PRIMARY KEY (review_id)
);

COMMENT ON TABLE review_action_analysis IS '조치 필요 여부 분석 (Snorkel)';
COMMENT ON COLUMN review_action_analysis.review_id IS 'Global ID (UUID v7)';
COMMENT ON COLUMN review_action_analysis.is_action_required IS '조치 필요 여부 (Feedback/Bug 등)';
COMMENT ON COLUMN review_action_analysis.action_confidence_score IS 'Snorkel/모델이 내뱉은 확률값 (0.0 ~ 1.0)';
COMMENT ON COLUMN review_action_analysis.trigger_reason IS '어떤 LF나 키워드가 결정적이었는지 기록';
COMMENT ON COLUMN review_action_analysis.is_attention_required IS '주의 필요 여부 (별점-감성 불일치 등)';
COMMENT ON COLUMN review_action_analysis.is_verified IS 'human in the loop';

-- ----------------------------------------
-- Reviews Assigned (Gold - DB)
-- ----------------------------------------

CREATE TABLE reviews_assigned
(
  assigned_id       BIGINT      NOT NULL GENERATED ALWAYS AS IDENTITY,
  review_id         UUID        NOT NULL,
  assigned_dept     TEXT[],
  assignment_reason TEXT,
  confidence        FLOAT,
  is_failed         BOOLEAN,
  try_number        INT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (assigned_id)
);

COMMENT ON TABLE reviews_assigned IS '최종 처리 부서 할당 (Gold - DB)';
COMMENT ON COLUMN reviews_assigned.assigned_id IS '업무 할당 ID';
COMMENT ON COLUMN reviews_assigned.review_id IS 'Global ID (UUID v7)';
COMMENT ON COLUMN reviews_assigned.assigned_dept IS '배정 부서 (배열)';
COMMENT ON COLUMN reviews_assigned.assignment_reason IS '배정 사유 (aspect_id)';
COMMENT ON COLUMN reviews_assigned.confidence IS '배정 확률';
COMMENT ON COLUMN reviews_assigned.is_failed IS '성공여부';
COMMENT ON COLUMN reviews_assigned.try_number IS '몇번째 시도 만에 성공했는지 기록';
COMMENT ON COLUMN reviews_assigned.created_at IS '생성일';
COMMENT ON COLUMN reviews_assigned.updated_at IS '수정일';

-- ----------------------------------------
-- Organizations (Hierarchy - ltree)
-- ----------------------------------------

CREATE TABLE organizations
(
  org_id              ltree       NOT NULL,
  org_name            VARCHAR,
  role_responsibility TEXT,
  keywords            TEXT[],
  review_types        TEXT,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (org_id)
);

COMMENT ON TABLE organizations IS '금융사 조직도 (ltree 계층 구조)';
COMMENT ON COLUMN organizations.org_id IS '조직 계층 ID (1, 1.1, 1.1.1)';
COMMENT ON COLUMN organizations.org_name IS '조직명 (디지털채널본부, 모바일뱅킹부)';
COMMENT ON COLUMN organizations.role_responsibility IS '역할 및 책임 설명';
COMMENT ON COLUMN organizations.keywords IS '주요 담당 키워드 (배열)';
COMMENT ON COLUMN organizations.review_types IS '담당 리뷰 유형';
COMMENT ON COLUMN organizations.created_at IS '생성일';
COMMENT ON COLUMN organizations.updated_at IS '수정일';

-- ----------------------------------------
-- Profanities (Dictionary)
-- ----------------------------------------

CREATE TABLE profanities
(
  id              INT         NOT NULL GENERATED ALWAYS AS IDENTITY,
  word            TEXT        NOT NULL,
  normalized_form TEXT,
  severity_level  SMALLINT    NOT NULL,
  is_active       BOOLEAN     NOT NULL DEFAULT true,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id)
);

COMMENT ON TABLE profanities IS '비속어 사전 (Dictionary)';
COMMENT ON COLUMN profanities.word IS '비속어';
COMMENT ON COLUMN profanities.normalized_form IS '정규화된 형태';
COMMENT ON COLUMN profanities.severity_level IS '심각도 (1-5)';
COMMENT ON COLUMN profanities.is_active IS '활성 상태';
COMMENT ON COLUMN profanities.created_at IS '생성일';
COMMENT ON COLUMN profanities.updated_at IS '수정일';

-- ----------------------------------------
-- Synonyms (Dictionary)
-- ----------------------------------------

CREATE TABLE synonyms
(
  id              INT         NOT NULL GENERATED ALWAYS AS IDENTITY,
  variant_form    TEXT        NOT NULL,
  canonical_form  TEXT        NOT NULL,
  normalized_form TEXT,
  is_active       BOOLEAN     NOT NULL DEFAULT true,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id)
);

COMMENT ON TABLE synonyms IS '동의어 사전 (Dictionary)';
COMMENT ON COLUMN synonyms.variant_form IS '변형/동의어';
COMMENT ON COLUMN synonyms.canonical_form IS '대표 용어';
COMMENT ON COLUMN synonyms.normalized_form IS '정규화된 형태';
COMMENT ON COLUMN synonyms.is_active IS '활성 상태';
COMMENT ON COLUMN synonyms.created_at IS '생성일';
COMMENT ON COLUMN synonyms.updated_at IS '수정일';

-- ----------------------------------------
-- Review LLM Analysis Logs (Audit)
-- ----------------------------------------

CREATE TABLE review_llm_analysis_logs
(
  id               INT                   NOT NULL GENERATED ALWAYS AS IDENTITY,
  source_table     TEXT,
  source_record_id TEXT,
  model_name       TEXT,
  params           TEXT,
  result_payload   JSONB,
  status           analysis_status_type,
  error_message    TEXT,
  processed_at     TIMESTAMPTZ,
  created_at       TIMESTAMPTZ           NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ           NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id)
);

COMMENT ON TABLE review_llm_analysis_logs IS 'AI 분석 로그 (Audit)';
COMMENT ON COLUMN review_llm_analysis_logs.id IS '로그 ID';
COMMENT ON COLUMN review_llm_analysis_logs.source_table IS '분석 소스 테이블';
COMMENT ON COLUMN review_llm_analysis_logs.source_record_id IS '분석 소스 레코드 ID';
COMMENT ON COLUMN review_llm_analysis_logs.model_name IS '사용한 AI 모델';
COMMENT ON COLUMN review_llm_analysis_logs.params IS '사용한 파라미터 기록';
COMMENT ON COLUMN review_llm_analysis_logs.result_payload IS 'LLM 결과 (JSONB)';
COMMENT ON COLUMN review_llm_analysis_logs.status IS 'PENDING / PROCESSING / SUCCESS / FAILED';
COMMENT ON COLUMN review_llm_analysis_logs.error_message IS '오류 메시지';
COMMENT ON COLUMN review_llm_analysis_logs.processed_at IS '분석 수행 시각 (UTC)';
COMMENT ON COLUMN review_llm_analysis_logs.created_at IS 'DB 등록일';
COMMENT ON COLUMN review_llm_analysis_logs.updated_at IS 'DB 수정일';

-- ========================================
-- FOREIGN KEY CONSTRAINTS
-- ========================================

-- App metadata relationships
ALTER TABLE app_metadata
  ADD CONSTRAINT FK_apps_TO_app_metadata
    FOREIGN KEY (app_id)
    REFERENCES apps (app_id);

ALTER TABLE app_metadata
  ADD CONSTRAINT FK_app_service_TO_app_metadata
    FOREIGN KEY (service_id)
    REFERENCES app_service (service_id);

-- Review master index relationships
ALTER TABLE review_master_index
  ADD CONSTRAINT FK_apps_TO_review_master_index
    FOREIGN KEY (app_id)
    REFERENCES apps (app_id);

-- Review analysis tables (all reference review_master_index)
ALTER TABLE review_aspects
  ADD CONSTRAINT FK_review_master_index_TO_review_aspects
    FOREIGN KEY (review_id)
    REFERENCES review_master_index (review_id);

ALTER TABLE review_embeddings
  ADD CONSTRAINT FK_review_master_index_TO_review_embeddings
    FOREIGN KEY (review_id)
    REFERENCES review_master_index (review_id);

ALTER TABLE review_action_analysis
  ADD CONSTRAINT FK_review_master_index_TO_review_action_analysis
    FOREIGN KEY (review_id)
    REFERENCES review_master_index (review_id);

ALTER TABLE reviews_assigned
  ADD CONSTRAINT FK_review_master_index_TO_reviews_assigned
    FOREIGN KEY (review_id)
    REFERENCES review_master_index (review_id);

-- ========================================
-- INDEXES
-- ========================================

-- ----------------------------------------
-- Foreign Key Indexes
-- ----------------------------------------

CREATE INDEX idx_app_metadata_app_id ON app_metadata(app_id);
CREATE INDEX idx_app_metadata_service_id ON app_metadata(service_id);
CREATE INDEX idx_review_master_index_app_id ON review_master_index(app_id);
CREATE INDEX idx_review_aspects_review_id ON review_aspects(review_id);
CREATE INDEX idx_review_embeddings_review_id ON review_embeddings(review_id);
CREATE INDEX idx_review_action_analysis_review_id ON review_action_analysis(review_id);
CREATE INDEX idx_reviews_assigned_review_id ON reviews_assigned(review_id);

-- ----------------------------------------
-- Search and Filter Indexes
-- ----------------------------------------

-- Apps table
CREATE INDEX idx_apps_platform ON apps(platform_type);
CREATE INDEX idx_apps_platform_app_id ON apps(platform_app_id);

-- Review master index
CREATE INDEX idx_review_master_index_processing_status ON review_master_index(processing_status);
CREATE INDEX idx_review_master_index_is_active ON review_master_index(is_active);
CREATE INDEX idx_review_master_index_platform_type ON review_master_index(platform_type);
CREATE INDEX idx_review_master_index_review_created_at ON review_master_index(review_created_at);

-- App metadata
CREATE INDEX idx_app_metadata_is_active ON app_metadata(is_active);
CREATE INDEX idx_app_metadata_group_id ON app_metadata(group_id);
CREATE INDEX idx_app_metadata_valid_from_to ON app_metadata(valid_from, valid_to);

-- Review action analysis
CREATE INDEX idx_review_action_analysis_is_action_required ON review_action_analysis(is_action_required);
CREATE INDEX idx_review_action_analysis_is_verified ON review_action_analysis(is_verified);

-- LLM analysis logs
CREATE INDEX idx_review_llm_analysis_logs_status ON review_llm_analysis_logs(status);
CREATE INDEX idx_review_llm_analysis_logs_source_table ON review_llm_analysis_logs(source_table);

-- ----------------------------------------
-- ltree Indexes (Organizations Hierarchy)
-- ----------------------------------------

-- GIST index for ltree operations (ancestor/descendant queries)
CREATE INDEX idx_organizations_org_id_gist ON organizations USING GIST(org_id);

-- BTREE index for exact match and ordering
CREATE INDEX idx_organizations_org_id_btree ON organizations USING BTREE(org_id);

-- ----------------------------------------
-- Array Column GIN Indexes
-- ----------------------------------------

-- Organizations keywords
CREATE INDEX idx_organizations_keywords ON organizations USING GIN(keywords);

-- Reviews assigned departments
CREATE INDEX idx_reviews_assigned_assigned_dept ON reviews_assigned USING GIN(assigned_dept);

-- ----------------------------------------
-- Vector Similarity Search Index
-- ----------------------------------------

-- HNSW index for vector similarity search (cosine distance)
CREATE INDEX idx_review_embeddings_vector ON review_embeddings
  USING hnsw (vector vector_cosine_ops);

-- ========================================
-- END OF SCHEMA
-- ========================================
