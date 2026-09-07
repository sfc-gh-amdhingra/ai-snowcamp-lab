-- ============================================================
-- AI Snowcamp — Lab Setup Script
-- Executed automatically via EXECUTE IMMEDIATE FROM the
-- bootstrap snippet in Step 3 of the lab guide.
-- Do not run this script directly.
--
-- Objects created:
--   Database:   OPTUM_LAB_DB
--   Schema:     OPTUM_LAB_DB.PAYER
--   Schema:     OPTUM_LAB_DB.AGENTS (holds the Cortex Agent built in Step 6)
--   Tables:     MEMBERS, MEDICAL_CLAIMS, PHARMACY_CLAIMS, PROVIDERS
--   Role:       OPTUM_LAB_ROLE
--   Warehouse:  OPTUM_LAB_WH (MEDIUM)
--   CoWork obj: SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT (account-level)
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

-- ── 5b. ROLL DATES FORWARD TO TODAY ───────────────────────────────────────────
-- The committed CSVs hold three full years of history ending 2025-12-30. Left
-- as-is, every time-relative question an attendee asks ("this year", "last
-- month", "recent trend") returns nothing, because CoWork resolves rolling
-- windows against the real current date. Cortex Analyst and the agent both look
-- broken when the honest answer is that the data is old.
--
-- This shifts every transactional date forward so the most recent claim lands
-- five days ago, preserving the exact three-year shape and all distributions.
-- The shift is computed from CURRENT_DATE rather than hardcoded, so the lab
-- stays current at every future delivery with no CSV regeneration.
--
-- Tables are CREATE OR REPLACE above, so re-running setup reloads the original
-- 2023-2025 data and recomputes the shift correctly. It never double-shifts.
--
-- DOB is deliberately left alone: shifting it would hold every member's age
-- frozen, whereas leaving it lets members age naturally with the calendar.
SET date_shift_days = (
  SELECT DATEDIFF(day, MAX(service_date), DATEADD(day, -5, CURRENT_DATE()))
  FROM OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS
);

UPDATE OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS
  SET service_date = DATEADD(day, $date_shift_days, service_date);

UPDATE OPTUM_LAB_DB.PAYER.PHARMACY_CLAIMS
  SET fill_date = DATEADD(day, $date_shift_days, fill_date);

UPDATE OPTUM_LAB_DB.PAYER.MEMBERS
  SET enrollment_start = DATEADD(day, $date_shift_days, enrollment_start),
      enrollment_end   = DATEADD(day, $date_shift_days, enrollment_end);

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
GRANT CREATE SEMANTIC VIEW     ON SCHEMA OPTUM_LAB_DB.PAYER TO ROLE OPTUM_LAB_ROLE;
GRANT CREATE CORTEX SEARCH SERVICE ON SCHEMA OPTUM_LAB_DB.PAYER TO ROLE OPTUM_LAB_ROLE;
GRANT CREATE FUNCTION          ON SCHEMA OPTUM_LAB_DB.PAYER TO ROLE OPTUM_LAB_ROLE;

-- Cortex AI functions (Analyst, Search, CoWork)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE OPTUM_LAB_ROLE;

-- Belt and braces for the May 2026 shift to per-AI-function privileges.
-- CORTEX_USER has historically covered everything this lab needs, but agent
-- invocation and AI_* functions now have narrower roles of their own. Granting
-- them costs nothing where CORTEX_USER already suffices, and prevents a
-- privilege error mid-lab where it does not. Each is granted separately so one
-- unavailable role on a given account cannot block the others.
BEGIN
  GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE OPTUM_LAB_ROLE;
EXCEPTION WHEN OTHER THEN NULL;
END;

BEGIN
  GRANT DATABASE ROLE SNOWFLAKE.AI_FUNCTIONS_USER TO ROLE OPTUM_LAB_ROLE;
EXCEPTION WHEN OTHER THEN NULL;
END;

-- ── 7. AGENT SCHEMA ───────────────────────────────────────────────────────────
-- In earlier versions of this lab the agent lived in a top-level
-- SNOWFLAKE_INTELLIGENCE database, because that schema was how agents were made
-- visible in the UI. That is no longer the case: visibility is now controlled by
-- the CoWork object (section 7b), and SNOWFLAKE_INTELLIGENCE.AGENTS is
-- deprecated for that purpose.
--
-- Keeping the agent in OPTUM_LAB_DB also removes a trial failure mode. Creating
-- an account-level database could fail with "Insufficient privileges to operate
-- on account" on some trials, which is why this block used to need an exception
-- handler. A schema inside a database we just created cannot fail that way, so
-- the handler is gone and a genuine error will now surface instead of passing
-- silently.
CREATE SCHEMA IF NOT EXISTS OPTUM_LAB_DB.AGENTS;
GRANT USAGE        ON SCHEMA OPTUM_LAB_DB.AGENTS TO ROLE OPTUM_LAB_ROLE;
GRANT CREATE AGENT ON SCHEMA OPTUM_LAB_DB.AGENTS TO ROLE OPTUM_LAB_ROLE;

-- ── 7b. SNOWFLAKE COWORK OBJECT ───────────────────────────────────────────────
-- The Snowflake CoWork object is an account-level object that controls which
-- agents are visible in Snowflake CoWork (ai.snowflake.com).
--
-- Why this block exists: agent visibility is CONDITIONAL. If an account has a
-- CoWork object, an agent is only listed in CoWork when it has been explicitly
-- added to that object. If the account has no CoWork object, all accessible
-- agents are listed automatically.
--
-- The trap: opening CoWork Settings in Snowsight AUTO-CREATES the object. An
-- attendee who explores Settings would silently lose their agent from the list
-- with no error message. We therefore create the object up front so that every
-- attendee is on the same, known code path — the agent is registered in Step 6
-- and behaviour does not depend on whether anyone clicked Settings.
--
-- CREATE SNOWFLAKE INTELLIGENCE ON ACCOUNT is granted to ACCOUNTADMIN by default.
-- The exception handler mirrors the block above: trial account provisioning
-- varies, and setup must not abort if this is unavailable. If it does fail,
-- Step 7 documents the Snowsight route, which works regardless of this object.
BEGIN
  CREATE SNOWFLAKE INTELLIGENCE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT;
  GRANT USAGE  ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE OPTUM_LAB_ROLE;
  GRANT MODIFY ON SNOWFLAKE INTELLIGENCE SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT TO ROLE OPTUM_LAB_ROLE;
EXCEPTION
  WHEN OTHER THEN NULL;
END;

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

-- ── 9. SEED QUERY HISTORY FOR AUTOPILOT ──────────────────────────────────────
-- Semantic View Autopilot uses query history to suggest better dimensions,
-- metrics, and relationships. These queries exercise the key payer analytics
-- patterns so Autopilot has signal to work with when attendees run it in Step 4.
-- Results are discarded — only the query history matters.

-- Cost of care by chronic condition
SELECT m.chronic_condition, COUNT(DISTINCT mc.claim_id) AS claims, SUM(mc.paid_amt) AS total_paid
FROM OPTUM_LAB_DB.PAYER.MEMBERS m
JOIN OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS mc ON m.member_id = mc.member_id
WHERE mc.claim_status = 'Approved'
GROUP BY 1 ORDER BY 3 DESC;

-- Monthly medical spend trend
SELECT DATE_TRUNC('MONTH', mc.service_date) AS month, SUM(mc.paid_amt) AS total_paid, COUNT(DISTINCT mc.claim_id) AS claim_count
FROM OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS mc
WHERE mc.claim_status = 'Approved'
GROUP BY 1 ORDER BY 1;

-- Pharmacy spend by drug class
SELECT pc.drug_class, SUM(pc.paid_amt) AS total_spend, COUNT(DISTINCT pc.rx_id) AS fills
FROM OPTUM_LAB_DB.PAYER.PHARMACY_CLAIMS pc
GROUP BY 1 ORDER BY 2 DESC;

-- Members without a preventive visit in the last 12 months
SELECT COUNT(DISTINCT m.member_id) AS members_without_preventive
FROM OPTUM_LAB_DB.PAYER.MEMBERS m
WHERE m.enrollment_end IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS mc
    WHERE mc.member_id = m.member_id AND mc.service_type = 'Preventive'
      AND mc.service_date >= DATEADD(YEAR, -1, CURRENT_DATE())
  );

-- Provider specialty cost comparison (in-network only)
SELECT p.specialty, ROUND(AVG(mc.paid_amt), 2) AS avg_paid_per_claim, COUNT(DISTINCT mc.claim_id) AS claims
FROM OPTUM_LAB_DB.PAYER.PROVIDERS p
JOIN OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS mc ON p.provider_id = mc.provider_id
WHERE p.network_status = 'In-Network' AND mc.claim_status = 'Approved'
GROUP BY 1 ORDER BY 2 DESC;

-- Top 10 diabetes members by pharmacy spend
SELECT m.member_id, m.name, SUM(pc.paid_amt) AS total_rx_spend, COUNT(DISTINCT pc.rx_id) AS fills
FROM OPTUM_LAB_DB.PAYER.MEMBERS m
JOIN OPTUM_LAB_DB.PAYER.PHARMACY_CLAIMS pc ON m.member_id = pc.member_id
WHERE m.chronic_condition = 'Diabetes'
GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10;

-- Service type utilization breakdown
SELECT mc.service_type, COUNT(DISTINCT mc.claim_id) AS claims, COUNT(DISTINCT mc.member_id) AS unique_members,
       SUM(mc.paid_amt) AS total_paid
FROM OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS mc
GROUP BY 1 ORDER BY 4 DESC;

-- Plan type enrollment and cost summary
SELECT m.plan_type, COUNT(DISTINCT m.member_id) AS members, SUM(mc.paid_amt) AS medical_cost
FROM OPTUM_LAB_DB.PAYER.MEMBERS m
JOIN OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS mc ON m.member_id = mc.member_id
WHERE m.enrollment_end IS NULL AND mc.claim_status = 'Approved'
GROUP BY 1 ORDER BY 3 DESC;

-- Monthly pharmacy trend for chronic condition members
SELECT DATE_TRUNC('MONTH', pc.fill_date) AS month, m.chronic_condition,
       SUM(pc.paid_amt) AS rx_spend, COUNT(DISTINCT pc.rx_id) AS fills
FROM OPTUM_LAB_DB.PAYER.MEMBERS m
JOIN OPTUM_LAB_DB.PAYER.PHARMACY_CLAIMS pc ON m.member_id = pc.member_id
WHERE m.chronic_condition != 'None'
GROUP BY 1, 2 ORDER BY 1, 4 DESC;

-- Denied claims analysis
SELECT mc.service_type, COUNT(DISTINCT mc.claim_id) AS denied_claims, SUM(mc.billed_amt) AS denied_billed
FROM OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS mc
WHERE mc.claim_status = 'Denied'
GROUP BY 1 ORDER BY 2 DESC;

-- ── 10. VERIFY ─────────────────────────────────────────────────────────────────
USE ROLE ACCOUNTADMIN;
USE DATABASE OPTUM_LAB_DB;
USE SCHEMA   PAYER;

SELECT 'MEMBERS'         AS tbl, COUNT(*) AS row_count FROM OPTUM_LAB_DB.PAYER.MEMBERS
UNION ALL SELECT 'MEDICAL_CLAIMS',  COUNT(*) FROM OPTUM_LAB_DB.PAYER.MEDICAL_CLAIMS
UNION ALL SELECT 'PHARMACY_CLAIMS', COUNT(*) FROM OPTUM_LAB_DB.PAYER.PHARMACY_CLAIMS
UNION ALL SELECT 'PROVIDERS',       COUNT(*) FROM OPTUM_LAB_DB.PAYER.PROVIDERS;

-- Confirm the CoWork object exists. Expect exactly one row named
-- SNOWFLAKE_INTELLIGENCE_OBJECT_DEFAULT. If this returns no rows, section 7b
-- was skipped by its exception handler — the lab still works, but follow the
-- Snowsight route in Step 7 rather than expecting the agent in CoWork.
SHOW SNOWFLAKE INTELLIGENCES;

SELECT 'Setup complete. Switch your role to OPTUM_LAB_ROLE.' AS status;
