-- ============================================================
-- AI Snowcamp — Lab Setup Script
-- Executed automatically via EXECUTE IMMEDIATE FROM the
-- bootstrap snippet in Step 3 of the lab guide.
-- Do not run this script directly.
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
-- IF NOT EXISTS preserves the SNOWCAMP_LAB_REPO Git repository object that the
-- bootstrap snippet created in OPTUM_LAB_DB.PAYER before invoking this script.
CREATE DATABASE IF NOT EXISTS OPTUM_LAB_DB;
CREATE SCHEMA  IF NOT EXISTS OPTUM_LAB_DB.PAYER;
USE DATABASE OPTUM_LAB_DB;
USE SCHEMA   PAYER;

-- ── 2. WAREHOUSE (created early — needed for COPY FILES / COPY INTO below) ────
-- IF NOT EXISTS avoids dropping an active warehouse on re-runs.
CREATE WAREHOUSE IF NOT EXISTS OPTUM_LAB_WH
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND        = 120
  AUTO_RESUME         = TRUE;

USE WAREHOUSE OPTUM_LAB_WH;

-- ── 3. TABLES ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE MEMBERS (
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

CREATE OR REPLACE TABLE MEDICAL_CLAIMS (
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

CREATE OR REPLACE TABLE PHARMACY_CLAIMS (
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

CREATE OR REPLACE TABLE PROVIDERS (
  provider_id        VARCHAR(20)   NOT NULL, -- e.g. PRV000001
  name               VARCHAR(100),
  specialty          VARCHAR(50),
  npi                VARCHAR(15),
  network_status     VARCHAR(20),           -- In-Network, Out-of-Network
  location           VARCHAR(100),          -- City, State
  avg_cost_per_visit NUMBER(10,2)
);

-- ── 4. FILE FORMAT ────────────────────────────────────────────────────────────
CREATE OR REPLACE FILE FORMAT PAYER_CSV_FORMAT
  TYPE                         = 'CSV'
  SKIP_HEADER                  = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('NULL', 'null', '');

-- Text format for reading .txt policy documents from stage (Step 5)
CREATE OR REPLACE FILE FORMAT TEXT_FORMAT
  TYPE             = 'CSV'
  FIELD_DELIMITER  = NONE
  RECORD_DELIMITER = '\n'
  SKIP_BLANK_LINES = TRUE;

-- ── 5. LOAD DATA FROM GIT REPO ────────────────────────────────────────────────
-- COPY INTO does not support Git Repository stages as a source directly.
-- Step 1: copy CSV files from the Git stage into a temporary internal stage.
-- Step 2: load each table from the internal stage with COPY INTO.

CREATE OR REPLACE STAGE OPTUM_LAB_DB.PAYER.lab_data
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

COPY FILES INTO @OPTUM_LAB_DB.PAYER.lab_data
  FROM @SNOWCAMP_LAB_REPO/branches/main/assets/data/
  FILES = ('members.csv', 'medical_claims.csv', 'pharmacy_claims.csv', 'providers.csv');

COPY INTO MEMBERS
  FROM @OPTUM_LAB_DB.PAYER.lab_data
  FILES        = ('members.csv')
  FILE_FORMAT  = (FORMAT_NAME = PAYER_CSV_FORMAT)
  ON_ERROR     = CONTINUE;

COPY INTO MEDICAL_CLAIMS
  FROM @OPTUM_LAB_DB.PAYER.lab_data
  FILES        = ('medical_claims.csv')
  FILE_FORMAT  = (FORMAT_NAME = PAYER_CSV_FORMAT)
  ON_ERROR     = CONTINUE;

COPY INTO PHARMACY_CLAIMS
  FROM @OPTUM_LAB_DB.PAYER.lab_data
  FILES        = ('pharmacy_claims.csv')
  FILE_FORMAT  = (FORMAT_NAME = PAYER_CSV_FORMAT)
  ON_ERROR     = CONTINUE;

COPY INTO PROVIDERS
  FROM @OPTUM_LAB_DB.PAYER.lab_data
  FILES        = ('providers.csv')
  FILE_FORMAT  = (FORMAT_NAME = PAYER_CSV_FORMAT)
  ON_ERROR     = CONTINUE;

-- ── 6. ROLE AND GRANTS ────────────────────────────────────────────────────────
CREATE OR REPLACE ROLE OPTUM_LAB_ROLE;

BEGIN
  LET v_user VARCHAR := CURRENT_USER();
  EXECUTE IMMEDIATE 'GRANT ROLE OPTUM_LAB_ROLE TO USER ' || :v_user;
END;

GRANT USAGE  ON DATABASE  OPTUM_LAB_DB                          TO ROLE OPTUM_LAB_ROLE;
GRANT USAGE  ON SCHEMA    OPTUM_LAB_DB.PAYER                    TO ROLE OPTUM_LAB_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA OPTUM_LAB_DB.PAYER         TO ROLE OPTUM_LAB_ROLE;
GRANT USAGE  ON WAREHOUSE OPTUM_LAB_WH                          TO ROLE OPTUM_LAB_ROLE;

-- Grants needed for Steps 5 and beyond
GRANT CREATE STAGE             ON SCHEMA OPTUM_LAB_DB.PAYER TO ROLE OPTUM_LAB_ROLE;
GRANT CREATE TABLE             ON SCHEMA OPTUM_LAB_DB.PAYER TO ROLE OPTUM_LAB_ROLE;
GRANT CREATE FILE FORMAT       ON SCHEMA OPTUM_LAB_DB.PAYER TO ROLE OPTUM_LAB_ROLE;
GRANT CREATE CORTEX SEARCH SERVICE ON SCHEMA OPTUM_LAB_DB.PAYER TO ROLE OPTUM_LAB_ROLE;

-- Cortex AI functions (Analyst, Search, Intelligence)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE OPTUM_LAB_ROLE;

-- ── 7. SNOWFLAKE INTELLIGENCE SCHEMA ──────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA  IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE             TO ROLE OPTUM_LAB_ROLE;
GRANT USAGE ON SCHEMA   SNOWFLAKE_INTELLIGENCE.AGENTS      TO ROLE OPTUM_LAB_ROLE;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE OPTUM_LAB_ROLE;

-- ── 8. INTERNAL STAGES FOR LAB ARTIFACTS ─────────────────────────────────────
-- Switch to lab role to own these stages
USE ROLE OPTUM_LAB_ROLE;
USE WAREHOUSE OPTUM_LAB_WH;

CREATE OR REPLACE STAGE OPTUM_LAB_DB.PAYER.SEMANTIC_MODELS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY  = (ENABLE = TRUE);

-- Attendees will copy policy documents into this stage manually in Step 5.
-- This is an intentional learning step — do not pre-populate it here.
CREATE OR REPLACE STAGE OPTUM_LAB_DB.PAYER.POLICY_DOCS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY  = (ENABLE = TRUE);

-- ── 9. VERIFY ─────────────────────────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;

SELECT 'MEMBERS'         AS tbl, COUNT(*) AS row_count FROM MEMBERS
UNION ALL SELECT 'MEDICAL_CLAIMS',  COUNT(*) FROM MEDICAL_CLAIMS
UNION ALL SELECT 'PHARMACY_CLAIMS', COUNT(*) FROM PHARMACY_CLAIMS
UNION ALL SELECT 'PROVIDERS',       COUNT(*) FROM PROVIDERS;

SELECT 'Setup complete. Switch your role to OPTUM_LAB_ROLE.' AS status;
