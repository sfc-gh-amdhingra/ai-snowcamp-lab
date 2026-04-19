-- ============================================================
-- AI Snowcamp — Lab Setup Script
-- Run this entire script as ACCOUNTADMIN in a SQL Worksheet
-- Estimated runtime: under 2 minutes
--
-- Objects created:
--   Database:   OPTUM_LAB_DB
--   Schema:     OPTUM_LAB_DB.PAYER
--   Tables:     MEMBERS, MEDICAL_CLAIMS, PHARMACY_CLAIMS, PROVIDERS
--   Role:       OPTUM_LAB_ROLE
--   Warehouse:  OPTUM_LAB_WH (MEDIUM)
--   Database:   SNOWFLAKE_INTELLIGENCE
--   Schema:     SNOWFLAKE_INTELLIGENCE.AGENTS
--   Stages:     OPTUM_LAB_DB.PAYER.SEMANTIC_MODELS (internal)
--               OPTUM_LAB_DB.PAYER.POLICY_DOCS (internal)
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ── 1. DATABASE AND SCHEMA ────────────────────────────────────────────────────
CREATE OR REPLACE DATABASE optum_lab_db;
CREATE OR REPLACE SCHEMA   optum_lab_db.payer;
USE DATABASE optum_lab_db;
USE SCHEMA   payer;

-- ── 2. TABLES ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE members (
  member_id         VARCHAR(20)   NOT NULL,  -- e.g. MBR000001
  name              VARCHAR(100),
  dob               DATE,
  gender            VARCHAR(10),             -- M, F, Other
  plan_id           VARCHAR(20),
  plan_name         VARCHAR(100),
  plan_type         VARCHAR(20),             -- HMO, PPO, EPO, HDHP
  pcp               VARCHAR(100),
  chronic_condition VARCHAR(50),             -- Diabetes, Hypertension, COPD, Heart Disease, None
  smoker_ind        BOOLEAN,
  enrollment_start  DATE,
  enrollment_end    DATE                     -- NULL = active member
);

CREATE OR REPLACE TABLE medical_claims (
  claim_id       VARCHAR(20)   NOT NULL,     -- e.g. CLM000001
  member_id      VARCHAR(20),
  service_date   DATE,
  provider_id    VARCHAR(20),
  icd10_code     VARCHAR(10),               -- ICD-10 diagnosis code
  procedure_code VARCHAR(10),               -- CPT procedure code
  billed_amt     NUMBER(10,2),
  allowed_amt    NUMBER(10,2),
  paid_amt       NUMBER(10,2),
  claim_status   VARCHAR(20),               -- Approved, Denied, Pending
  service_type   VARCHAR(30)                -- Inpatient, Outpatient, ER, Office Visit, Preventive
);

CREATE OR REPLACE TABLE pharmacy_claims (
  rx_id         VARCHAR(20)   NOT NULL,     -- e.g. RX000001
  member_id     VARCHAR(20),
  drug_name     VARCHAR(100),
  ndc_code      VARCHAR(15),
  drug_class    VARCHAR(50),               -- Diabetes, Cardiovascular, Respiratory, etc.
  days_supply   NUMBER(5),                 -- typically 30 or 90
  paid_amt      NUMBER(10,2),
  fill_date     DATE,
  prescriber_id VARCHAR(20)
);

CREATE OR REPLACE TABLE providers (
  provider_id        VARCHAR(20)   NOT NULL, -- e.g. PRV000001
  name               VARCHAR(100),
  specialty          VARCHAR(50),
  npi                VARCHAR(15),
  network_status     VARCHAR(20),           -- In-Network, Out-of-Network
  location           VARCHAR(100),          -- City, State
  avg_cost_per_visit NUMBER(10,2)
);

-- ── 3. FILE FORMAT AND DATA LOAD FROM GIT REPO ───────────────────────────────
-- CSVs are loaded directly from the public GitHub repo (no credentials needed).
-- The Git repository object (snowcamp_lab_repo) was created by the bootstrap
-- snippet in Step 3 of the lab guide before this script was executed.

CREATE OR REPLACE FILE FORMAT payer_csv_format
  TYPE                         = 'CSV'
  SKIP_HEADER                  = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('NULL', 'null', '');

-- ── 4. LOAD DATA FROM GIT REPO ────────────────────────────────────────────────
COPY INTO members
  FROM @snowcamp_lab_repo/branches/main/assets/data/members.csv
  FILE_FORMAT = payer_csv_format ON_ERROR = CONTINUE;

COPY INTO medical_claims
  FROM @snowcamp_lab_repo/branches/main/assets/data/medical_claims.csv
  FILE_FORMAT = payer_csv_format ON_ERROR = CONTINUE;

COPY INTO pharmacy_claims
  FROM @snowcamp_lab_repo/branches/main/assets/data/pharmacy_claims.csv
  FILE_FORMAT = payer_csv_format ON_ERROR = CONTINUE;

COPY INTO providers
  FROM @snowcamp_lab_repo/branches/main/assets/data/providers.csv
  FILE_FORMAT = payer_csv_format ON_ERROR = CONTINUE;

-- ── 5. ROLE, WAREHOUSE, AND GRANTS ────────────────────────────────────────────
CREATE OR REPLACE ROLE      optum_lab_role;
CREATE OR REPLACE WAREHOUSE optum_lab_wh
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND        = 120
  AUTO_RESUME         = TRUE;

SET current_user = (SELECT CURRENT_USER());
GRANT ROLE optum_lab_role TO USER IDENTIFIER($current_user);

GRANT USAGE  ON DATABASE  optum_lab_db                   TO ROLE optum_lab_role;
GRANT USAGE  ON SCHEMA    optum_lab_db.payer              TO ROLE optum_lab_role;
GRANT SELECT ON ALL TABLES IN SCHEMA optum_lab_db.payer   TO ROLE optum_lab_role;
GRANT USAGE  ON WAREHOUSE optum_lab_wh                    TO ROLE optum_lab_role;

-- ── 6. SNOWFLAKE INTELLIGENCE SCHEMA ──────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS snowflake_intelligence;
CREATE SCHEMA  IF NOT EXISTS snowflake_intelligence.agents;

GRANT USAGE ON DATABASE snowflake_intelligence            TO ROLE optum_lab_role;
GRANT USAGE ON SCHEMA   snowflake_intelligence.agents     TO ROLE optum_lab_role;
GRANT CREATE AGENT ON SCHEMA snowflake_intelligence.agents TO ROLE optum_lab_role;

-- ── 7. INTERNAL STAGES FOR LAB ARTIFACTS ─────────────────────────────────────
-- Switch to lab role to own these stages
USE ROLE optum_lab_role;

CREATE OR REPLACE STAGE optum_lab_db.payer.semantic_models
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY  = (ENABLE = TRUE);

CREATE OR REPLACE STAGE optum_lab_db.payer.policy_docs
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY  = (ENABLE = TRUE);

-- ── 8. VERIFY ─────────────────────────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;

SELECT 'MEMBERS'         AS tbl, COUNT(*) AS rows FROM members
UNION ALL SELECT 'MEDICAL_CLAIMS',  COUNT(*) FROM medical_claims
UNION ALL SELECT 'PHARMACY_CLAIMS', COUNT(*) FROM pharmacy_claims
UNION ALL SELECT 'PROVIDERS',       COUNT(*) FROM providers;

SELECT 'Setup complete. Switch your role to OPTUM_LAB_ROLE.' AS status;
