-- ============================================================
-- AI Snowcamp — Synthetic Data Generation Script
-- Run this in your SE demo account (SYSADMIN or ACCOUNTADMIN)
-- Output: 4 CSV files in @payer_export stage ready for Azure upload
--
-- After running, download the CSV files and upload to Azure Blob:
--   azure://sfselabs.blob.core.windows.net/ai-snowcamp/data/
-- Then generate a SAS token and update assets/sql/setup.sql
-- ============================================================

USE ROLE SYSADMIN;

CREATE OR REPLACE DATABASE optum_lab_generator;
CREATE OR REPLACE SCHEMA   optum_lab_generator.payer;
USE DATABASE optum_lab_generator;
USE SCHEMA   payer;

CREATE OR REPLACE WAREHOUSE optum_lab_gen_wh
  WITH WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 120 AUTO_RESUME = TRUE;
USE WAREHOUSE optum_lab_gen_wh;

-- ── PROVIDERS (2,000 rows) ────────────────────────────────────────────────────
CREATE OR REPLACE TABLE providers AS
WITH base AS (
  SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 2000))
)
SELECT
  'PRV' || LPAD(n+1, 6, '0') AS provider_id,
  'Dr. ' || CASE MOD(n, 25)
    WHEN 0  THEN 'Sarah Chen'       WHEN 1  THEN 'Michael Torres'   WHEN 2  THEN 'Emily Patel'
    WHEN 3  THEN 'James Okafor'     WHEN 4  THEN 'Priya Sharma'     WHEN 5  THEN 'Robert Kim'
    WHEN 6  THEN 'Maria Gonzalez'   WHEN 7  THEN 'David Nguyen'     WHEN 8  THEN 'Lisa Johnson'
    WHEN 9  THEN 'Ahmed Hassan'     WHEN 10 THEN 'Jennifer Wu'      WHEN 11 THEN 'Carlos Rivera'
    WHEN 12 THEN 'Sunita Reddy'     WHEN 13 THEN 'Thomas O''Brien'  WHEN 14 THEN 'Fatima Malik'
    WHEN 15 THEN 'Andrew Park'      WHEN 16 THEN 'Meera Krishnan'   WHEN 17 THEN 'Brian Walsh'
    WHEN 18 THEN 'Aisha Williams'   WHEN 19 THEN 'Kevin Murphy'     WHEN 20 THEN 'Deepa Iyer'
    WHEN 21 THEN 'Samuel Osei'      WHEN 22 THEN 'Rachel Cohen'     WHEN 23 THEN 'Omar Farouk'
    ELSE 'Nina Petrov'
  END AS name,
  CASE MOD(n, 15)
    WHEN 0  THEN 'Family Medicine'      WHEN 1  THEN 'Internal Medicine'
    WHEN 2  THEN 'Cardiology'           WHEN 3  THEN 'Endocrinology'
    WHEN 4  THEN 'Pulmonology'          WHEN 5  THEN 'Orthopedics'
    WHEN 6  THEN 'Psychiatry'           WHEN 7  THEN 'Dermatology'
    WHEN 8  THEN 'Gastroenterology'     WHEN 9  THEN 'Neurology'
    WHEN 10 THEN 'Oncology'             WHEN 11 THEN 'Physical Therapy'
    WHEN 12 THEN 'Emergency Medicine'   WHEN 13 THEN 'Ophthalmology'
    ELSE 'Obstetrics/Gynecology'
  END AS specialty,
  '1' || LPAD(MOD(ABS(HASH(n + 9999)), 999999999)::VARCHAR, 9, '0') AS npi,
  CASE WHEN MOD(n, 5) = 0 THEN 'Out-of-Network' ELSE 'In-Network' END AS network_status,
  CASE MOD(n, 16)
    WHEN 0  THEN 'Minneapolis, MN'  WHEN 1  THEN 'Chicago, IL'
    WHEN 2  THEN 'Houston, TX'      WHEN 3  THEN 'Phoenix, AZ'
    WHEN 4  THEN 'Philadelphia, PA' WHEN 5  THEN 'San Antonio, TX'
    WHEN 6  THEN 'Dallas, TX'       WHEN 7  THEN 'Austin, TX'
    WHEN 8  THEN 'Jacksonville, FL' WHEN 9  THEN 'Columbus, OH'
    WHEN 10 THEN 'Charlotte, NC'    WHEN 11 THEN 'Indianapolis, IN'
    WHEN 12 THEN 'Seattle, WA'      WHEN 13 THEN 'Denver, CO'
    WHEN 14 THEN 'Nashville, TN'    ELSE 'Boston, MA'
  END AS location,
  ROUND(UNIFORM(65, 750, RANDOM()), 2) AS avg_cost_per_visit
FROM base;

-- ── MEMBERS (10,000 rows) ─────────────────────────────────────────────────────
-- Chronic condition distribution: None 40%, Hypertension 25%, Diabetes 20%,
-- Heart Disease 8%, COPD 7%
CREATE OR REPLACE TABLE members AS
WITH base AS (
  SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 10000))
)
SELECT
  'MBR' || LPAD(n+1, 6, '0') AS member_id,
  CASE MOD(n, 20)
    WHEN 0  THEN 'James'     WHEN 1  THEN 'Mary'      WHEN 2  THEN 'Robert'
    WHEN 3  THEN 'Patricia'  WHEN 4  THEN 'John'      WHEN 5  THEN 'Jennifer'
    WHEN 6  THEN 'Michael'   WHEN 7  THEN 'Linda'     WHEN 8  THEN 'William'
    WHEN 9  THEN 'Barbara'   WHEN 10 THEN 'David'     WHEN 11 THEN 'Susan'
    WHEN 12 THEN 'Richard'   WHEN 13 THEN 'Jessica'   WHEN 14 THEN 'Joseph'
    WHEN 15 THEN 'Sarah'     WHEN 16 THEN 'Thomas'    WHEN 17 THEN 'Karen'
    WHEN 18 THEN 'Charles'   ELSE 'Nancy'
  END || ' ' || CASE MOD(n, 30)
    WHEN 0  THEN 'Smith'      WHEN 1  THEN 'Johnson'    WHEN 2  THEN 'Williams'
    WHEN 3  THEN 'Brown'      WHEN 4  THEN 'Jones'      WHEN 5  THEN 'Garcia'
    WHEN 6  THEN 'Miller'     WHEN 7  THEN 'Davis'      WHEN 8  THEN 'Rodriguez'
    WHEN 9  THEN 'Martinez'   WHEN 10 THEN 'Hernandez'  WHEN 11 THEN 'Lopez'
    WHEN 12 THEN 'Gonzalez'   WHEN 13 THEN 'Wilson'     WHEN 14 THEN 'Anderson'
    WHEN 15 THEN 'Thomas'     WHEN 16 THEN 'Taylor'     WHEN 17 THEN 'Moore'
    WHEN 18 THEN 'Jackson'    WHEN 19 THEN 'Martin'     WHEN 20 THEN 'Lee'
    WHEN 21 THEN 'Perez'      WHEN 22 THEN 'Thompson'   WHEN 23 THEN 'White'
    WHEN 24 THEN 'Harris'     WHEN 25 THEN 'Sanchez'    WHEN 26 THEN 'Clark'
    WHEN 27 THEN 'Ramirez'    WHEN 28 THEN 'Lewis'      ELSE 'Robinson'
  END AS name,
  -- Age range: 22–80
  DATEADD(DAY, -UNIFORM(8000, 29200, RANDOM()), DATE '2025-01-01') AS dob,
  CASE WHEN MOD(n, 100) < 49 THEN 'M' WHEN MOD(n, 100) < 98 THEN 'F' ELSE 'Other' END AS gender,
  'PLN' || LPAD(MOD(n, 10)+1, 3, '0') AS plan_id,
  CASE MOD(n, 10)
    WHEN 0 THEN 'UHC Choice Plus PPO'      WHEN 1 THEN 'UHC Navigate HMO'
    WHEN 2 THEN 'UHC Select Plus PPO'      WHEN 3 THEN 'UHC Core EPO'
    WHEN 4 THEN 'UHC SignatureValue HMO'   WHEN 5 THEN 'UHC Choice HDHP'
    WHEN 6 THEN 'UHC Nexus ACO'            WHEN 7 THEN 'UHC Navigate HMO Plus'
    WHEN 8 THEN 'UHC Alliance PPO'         ELSE 'UHC Consumer HDHP'
  END AS plan_name,
  CASE MOD(n, 10)
    WHEN 0 THEN 'PPO' WHEN 1 THEN 'HMO' WHEN 2 THEN 'PPO' WHEN 3 THEN 'EPO'
    WHEN 4 THEN 'HMO' WHEN 5 THEN 'HDHP' WHEN 6 THEN 'HMO' WHEN 7 THEN 'HMO'
    WHEN 8 THEN 'PPO' ELSE 'HDHP'
  END AS plan_type,
  -- Assign each member to one of 15 PCPs (from providers table specialty: Family Medicine / Internal Medicine)
  'Dr. ' || CASE MOD(n, 15)
    WHEN 0  THEN 'Sarah Chen'      WHEN 1  THEN 'Michael Torres'  WHEN 2  THEN 'Emily Patel'
    WHEN 3  THEN 'James Okafor'    WHEN 4  THEN 'Priya Sharma'    WHEN 5  THEN 'Robert Kim'
    WHEN 6  THEN 'Maria Gonzalez'  WHEN 7  THEN 'David Nguyen'    WHEN 8  THEN 'Lisa Johnson'
    WHEN 9  THEN 'Ahmed Hassan'    WHEN 10 THEN 'Jennifer Wu'     WHEN 11 THEN 'Carlos Rivera'
    WHEN 12 THEN 'Sunita Reddy'    WHEN 13 THEN 'Thomas O''Brien' ELSE 'Fatima Malik'
  END AS pcp,
  -- Chronic condition distribution (realistic payer population)
  CASE
    WHEN MOD(n, 100) < 40 THEN 'None'
    WHEN MOD(n, 100) < 65 THEN 'Hypertension'
    WHEN MOD(n, 100) < 85 THEN 'Diabetes'
    WHEN MOD(n, 100) < 93 THEN 'Heart Disease'
    ELSE 'COPD'
  END AS chronic_condition,
  CASE WHEN UNIFORM(0, 9, RANDOM()) < 2 THEN TRUE ELSE FALSE END AS smoker_ind,
  -- Enrollment between 2020 and 2023
  DATEADD(DAY, -UNIFORM(730, 1825, RANDOM()), DATE '2025-01-01') AS enrollment_start,
  -- 80% active (NULL end), 20% terminated
  CASE
    WHEN UNIFORM(0, 9, RANDOM()) < 8 THEN NULL
    ELSE DATEADD(DAY, -UNIFORM(30, 365, RANDOM()), DATE '2025-01-01')
  END AS enrollment_end
FROM base;

-- ── MEDICAL CLAIMS (~80,000 rows) ────────────────────────────────────────────
-- ~8 claims per member on average; service dates from 2023-01-01 to 2025-12-31
-- ICD-10 and service types are distributed to support realistic analytics queries
CREATE OR REPLACE TABLE medical_claims AS
WITH base AS (
  SELECT
    SEQ4()                              AS n,
    ROUND(UNIFORM(80, 12000, RANDOM()), 2) AS billed_amt,
    UNIFORM(40, 75, RANDOM())           AS allowed_pct,
    UNIFORM(70, 95, RANDOM())           AS paid_pct
  FROM TABLE(GENERATOR(ROWCOUNT => 80000))
)
SELECT
  'CLM' || LPAD(n+1, 7, '0') AS claim_id,
  'MBR' || LPAD(MOD(n, 10000)+1, 6, '0') AS member_id,
  -- Service dates spread across 2023–2025
  DATEADD(DAY, UNIFORM(0, 1094, RANDOM()), DATE '2023-01-01') AS service_date,
  -- Assign provider from pool of 2000 (PRV000001–PRV002000)
  'PRV' || LPAD(MOD(ABS(HASH(n)), 2000)+1, 6, '0') AS provider_id,
  -- ICD-10 codes: weighted realistic distribution
  CASE MOD(n, 50)
    WHEN 0  THEN 'E11.9'   -- Type 2 Diabetes, uncontrolled
    WHEN 1  THEN 'E11.65'  -- Type 2 Diabetes with hyperglycemia
    WHEN 2  THEN 'E11.40'  -- Type 2 Diabetes with neuropathy
    WHEN 3  THEN 'I10'     -- Essential hypertension
    WHEN 4  THEN 'I10'     -- Essential hypertension (weighted higher)
    WHEN 5  THEN 'I25.10'  -- Atherosclerotic heart disease
    WHEN 6  THEN 'I50.9'   -- Heart failure, unspecified
    WHEN 7  THEN 'I48.0'   -- Atrial fibrillation
    WHEN 8  THEN 'J44.1'   -- COPD with acute exacerbation
    WHEN 9  THEN 'J44.0'   -- COPD with acute lower respiratory infection
    WHEN 10 THEN 'Z00.00'  -- General adult exam, no abnormal findings (preventive)
    WHEN 11 THEN 'Z00.01'  -- General adult exam, abnormal findings (preventive)
    WHEN 12 THEN 'Z12.11'  -- Colorectal cancer screening (colonoscopy)
    WHEN 13 THEN 'Z12.31'  -- Mammography screening
    WHEN 14 THEN 'M54.5'   -- Low back pain
    WHEN 15 THEN 'M54.5'   -- Low back pain (weighted)
    WHEN 16 THEN 'J06.9'   -- Acute upper respiratory infection
    WHEN 17 THEN 'J18.9'   -- Pneumonia
    WHEN 18 THEN 'F32.1'   -- Major depressive disorder, moderate
    WHEN 19 THEN 'F41.1'   -- Generalized anxiety disorder
    WHEN 20 THEN 'K21.0'   -- GERD with esophagitis
    WHEN 21 THEN 'E78.5'   -- Hyperlipidemia
    WHEN 22 THEN 'N18.3'   -- Chronic kidney disease, stage 3
    WHEN 23 THEN 'N18.4'   -- Chronic kidney disease, stage 4
    WHEN 24 THEN 'M17.11'  -- Primary osteoarthritis, right knee
    WHEN 25 THEN 'M16.11'  -- Primary osteoarthritis, right hip
    WHEN 26 THEN 'G43.909' -- Migraine, unspecified
    WHEN 27 THEN 'E11.9'   -- Diabetes (repeated for weight)
    WHEN 28 THEN 'I10'     -- Hypertension (repeated for weight)
    WHEN 29 THEN 'Z23'     -- Immunization encounter
    WHEN 30 THEN 'S93.401' -- Ankle sprain
    WHEN 31 THEN 'J20.9'   -- Acute bronchitis
    WHEN 32 THEN 'K92.1'   -- Melena/GI bleed
    WHEN 33 THEN 'E66.9'   -- Obesity, unspecified
    WHEN 34 THEN 'R05'     -- Cough
    WHEN 35 THEN 'R51'     -- Headache
    WHEN 36 THEN 'I25.10'  -- Atherosclerotic heart disease (weight)
    WHEN 37 THEN 'Z87.891' -- Personal history of tobacco use
    WHEN 38 THEN 'E11.9'   -- Type 2 Diabetes (repeated for weight)
    WHEN 39 THEN 'E11.9'   -- Diabetes
    WHEN 40 THEN 'I10'     -- Hypertension
    WHEN 41 THEN 'Z00.00'  -- Preventive (weight)
    WHEN 42 THEN 'M79.3'   -- Panniculitis
    WHEN 43 THEN 'F33.0'   -- MDD, recurrent
    WHEN 44 THEN 'Z79.4'   -- Long-term insulin use
    WHEN 45 THEN 'E13.9'   -- Other specified diabetes
    WHEN 46 THEN 'J44.1'   -- COPD
    WHEN 47 THEN 'I50.32'  -- Chronic diastolic heart failure
    WHEN 48 THEN 'Z12.12'  -- Colon cancer screening (CT colonography)
    ELSE 'Z71.9'           -- Counseling, unspecified
  END AS icd10_code,
  -- CPT codes by service type
  CASE MOD(n, 10)
    WHEN 0 THEN '99213'  -- Office visit, established, low complexity
    WHEN 1 THEN '99214'  -- Office visit, established, moderate complexity
    WHEN 2 THEN '99215'  -- Office visit, established, high complexity
    WHEN 3 THEN '99283'  -- ED visit, moderate severity
    WHEN 4 THEN '99284'  -- ED visit, high severity
    WHEN 5 THEN '99232'  -- Inpatient subsequent care
    WHEN 6 THEN '99223'  -- Initial inpatient care, high complexity
    WHEN 7 THEN '99396'  -- Preventive visit, 40-64 years
    WHEN 8 THEN '45378'  -- Colonoscopy, diagnostic
    ELSE '77067'         -- Bilateral screening mammography
  END AS procedure_code,
  -- Billed > Allowed > Paid (realistic hierarchy)
  billed_amt,
  ROUND(billed_amt * allowed_pct / 100, 2) AS allowed_amt,
  -- Denied claims have paid_amt = 0
  CASE
    WHEN MOD(n, 100) < 10 THEN 0.00
    ELSE ROUND(billed_amt * allowed_pct / 100 * paid_pct / 100, 2)
  END AS paid_amt,
  CASE
    WHEN MOD(n, 100) < 10 THEN 'Denied'
    WHEN MOD(n, 100) < 15 THEN 'Pending'
    ELSE 'Approved'
  END AS claim_status,
  CASE MOD(n, 10)
    WHEN 0 THEN 'Office Visit'  WHEN 1 THEN 'Office Visit'  WHEN 2 THEN 'Office Visit'
    WHEN 3 THEN 'Outpatient'    WHEN 4 THEN 'Outpatient'
    WHEN 5 THEN 'ER'
    WHEN 6 THEN 'Inpatient'     WHEN 7 THEN 'Inpatient'
    WHEN 8 THEN 'Preventive'
    ELSE 'Preventive'
  END AS service_type
FROM base;

-- ── PHARMACY CLAIMS (~35,000 rows) ───────────────────────────────────────────
-- ~3.5 fills per member on average; fill dates from 2023-01-01 to 2025-12-31
CREATE OR REPLACE TABLE pharmacy_claims AS
WITH base AS (
  SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 35000))
)
SELECT
  'RX' || LPAD(n+1, 7, '0') AS rx_id,
  'MBR' || LPAD(MOD(n, 10000)+1, 6, '0') AS member_id,
  -- Drug name, class, NDC, and cost tier
  CASE MOD(n, 25)
    WHEN 0  THEN 'Metformin'           WHEN 1  THEN 'Metformin ER'
    WHEN 2  THEN 'Lisinopril'          WHEN 3  THEN 'Amlodipine'
    WHEN 4  THEN 'Atorvastatin'        WHEN 5  THEN 'Rosuvastatin'
    WHEN 6  THEN 'Metoprolol'          WHEN 7  THEN 'Carvedilol'
    WHEN 8  THEN 'Sertraline'          WHEN 9  THEN 'Escitalopram'
    WHEN 10 THEN 'Gabapentin'          WHEN 11 THEN 'Omeprazole'
    WHEN 12 THEN 'Albuterol Inhaler'   WHEN 13 THEN 'Tiotropium'
    WHEN 14 THEN 'Fluticasone Inhaler' WHEN 15 THEN 'Ozempic (semaglutide)'
    WHEN 16 THEN 'Mounjaro (tirzepatide)' WHEN 17 THEN 'Jardiance (empagliflozin)'
    WHEN 18 THEN 'Eliquis (apixaban)'  WHEN 19 THEN 'Eliquis (apixaban)'
    WHEN 20 THEN 'Wegovy (semaglutide)' WHEN 21 THEN 'Insulin Glargine'
    WHEN 22 THEN 'Insulin Aspart'      WHEN 23 THEN 'Trelegy Ellipta'
    ELSE 'Dupixent (dupilumab)'
  END AS drug_name,
  CASE MOD(n, 25)
    WHEN 0  THEN '00093-7267-01'  WHEN 1  THEN '00093-7268-01'
    WHEN 2  THEN '00093-1062-01'  WHEN 3  THEN '00093-1064-01'
    WHEN 4  THEN '00093-5194-01'  WHEN 5  THEN '55111-0344-30'
    WHEN 6  THEN '00378-2050-01'  WHEN 7  THEN '00378-3210-05'
    WHEN 8  THEN '00093-7199-01'  WHEN 9  THEN '00093-5279-01'
    WHEN 10 THEN '00093-0945-01'  WHEN 11 THEN '00093-0317-01'
    WHEN 12 THEN '00173-0682-20'  WHEN 13 THEN '00597-0075-41'
    WHEN 14 THEN '00173-0719-00'  WHEN 15 THEN '00169-4060-12'
    WHEN 16 THEN '00002-1433-80'  WHEN 17 THEN '00597-0141-30'
    WHEN 18 THEN '59148-0016-05'  WHEN 19 THEN '59148-0016-10'
    WHEN 20 THEN '00169-4030-12'  WHEN 21 THEN '00088-2220-33'
    WHEN 22 THEN '00169-7501-12'  WHEN 23 THEN '00173-0898-10'
    ELSE '66582-0414-01'
  END AS ndc_code,
  CASE MOD(n, 25)
    WHEN 0  THEN 'Diabetes'           WHEN 1  THEN 'Diabetes'
    WHEN 2  THEN 'Cardiovascular'     WHEN 3  THEN 'Cardiovascular'
    WHEN 4  THEN 'Cardiovascular'     WHEN 5  THEN 'Cardiovascular'
    WHEN 6  THEN 'Cardiovascular'     WHEN 7  THEN 'Cardiovascular'
    WHEN 8  THEN 'Mental Health'      WHEN 9  THEN 'Mental Health'
    WHEN 10 THEN 'Neurology/Pain'     WHEN 11 THEN 'Gastrointestinal'
    WHEN 12 THEN 'Respiratory'        WHEN 13 THEN 'Respiratory'
    WHEN 14 THEN 'Respiratory'        WHEN 15 THEN 'GLP-1/Diabetes'
    WHEN 16 THEN 'GLP-1/Diabetes'     WHEN 17 THEN 'Diabetes'
    WHEN 18 THEN 'Anticoagulant'      WHEN 19 THEN 'Anticoagulant'
    WHEN 20 THEN 'GLP-1/Obesity'      WHEN 21 THEN 'Diabetes'
    WHEN 22 THEN 'Diabetes'           WHEN 23 THEN 'Respiratory'
    ELSE 'Specialty Biologic'
  END AS drug_class,
  -- 30-day supply common; 90-day for maintenance medications
  CASE WHEN MOD(n, 10) < 3 THEN 90 ELSE 30 END AS days_supply,
  -- Paid amount reflects drug tier
  ROUND(CASE MOD(n, 25)
    WHEN 15 THEN UNIFORM(800, 1100, RANDOM())   -- Ozempic: Tier 5 specialty
    WHEN 16 THEN UNIFORM(900, 1200, RANDOM())   -- Mounjaro: Tier 5 specialty
    WHEN 17 THEN UNIFORM(400, 650, RANDOM())    -- Jardiance: Tier 4
    WHEN 18 THEN UNIFORM(350, 500, RANDOM())    -- Eliquis: Tier 4
    WHEN 19 THEN UNIFORM(350, 500, RANDOM())    -- Eliquis: Tier 4
    WHEN 20 THEN UNIFORM(850, 1150, RANDOM())   -- Wegovy: Tier 5 specialty
    WHEN 21 THEN UNIFORM(120, 200, RANDOM())    -- Insulin Glargine: Tier 3
    WHEN 22 THEN UNIFORM(100, 180, RANDOM())    -- Insulin Aspart: Tier 3
    WHEN 23 THEN UNIFORM(280, 420, RANDOM())    -- Trelegy: Tier 4
    WHEN 24 THEN UNIFORM(1800, 2600, RANDOM())  -- Dupixent: Tier 5 specialty biologic
    WHEN 13 THEN UNIFORM(40, 80, RANDOM())      -- Tiotropium: Tier 3
    WHEN 6  THEN UNIFORM(8, 18, RANDOM())       -- Metoprolol: Tier 1
    WHEN 4  THEN UNIFORM(8, 18, RANDOM())       -- Atorvastatin: Tier 1
    WHEN 2  THEN UNIFORM(5, 15, RANDOM())       -- Lisinopril: Tier 1
    ELSE         UNIFORM(5, 45, RANDOM())       -- Tier 1–2 generics
  END, 2) AS paid_amt,
  DATEADD(DAY, UNIFORM(0, 1094, RANDOM()), DATE '2023-01-01') AS fill_date,
  -- Assign prescriber from provider pool (subset weighted to relevant specialties)
  'PRV' || LPAD(MOD(ABS(HASH(n + 7777)), 2000)+1, 6, '0') AS prescriber_id
FROM base;

-- ── VERIFY ROW COUNTS ─────────────────────────────────────────────────────────
SELECT 'MEMBERS'         AS tbl, COUNT(*) AS rows FROM members
UNION ALL SELECT 'MEDICAL_CLAIMS',  COUNT(*) FROM medical_claims
UNION ALL SELECT 'PHARMACY_CLAIMS', COUNT(*) FROM pharmacy_claims
UNION ALL SELECT 'PROVIDERS',       COUNT(*) FROM providers;

-- ── EXPORT TO CSV STAGE ───────────────────────────────────────────────────────
-- After verifying row counts, export all tables to CSVs.
-- Download from the stage and upload to your Azure Blob container.

CREATE OR REPLACE FILE FORMAT csv_export_format
  TYPE                         = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('');

CREATE OR REPLACE STAGE payer_export
  FILE_FORMAT = csv_export_format;

COPY INTO @payer_export/members.csv
  FROM (SELECT * FROM members)
  HEADER         = TRUE
  OVERWRITE      = TRUE
  SINGLE         = TRUE;

COPY INTO @payer_export/medical_claims.csv
  FROM (SELECT * FROM medical_claims)
  HEADER         = TRUE
  OVERWRITE      = TRUE
  SINGLE         = TRUE;

COPY INTO @payer_export/pharmacy_claims.csv
  FROM (SELECT * FROM pharmacy_claims)
  HEADER         = TRUE
  OVERWRITE      = TRUE
  SINGLE         = TRUE;

COPY INTO @payer_export/providers.csv
  FROM (SELECT * FROM providers)
  HEADER         = TRUE
  OVERWRITE      = TRUE
  SINGLE         = TRUE;

-- List the exported files
LIST @payer_export;

SELECT '✓ Export complete. Download CSVs from @payer_export and upload to Azure Blob.' AS status;

-- ── CLEANUP (optional — run after uploading CSVs to Azure) ───────────────────
-- DROP DATABASE optum_lab_generator;
-- DROP WAREHOUSE optum_lab_gen_wh;
