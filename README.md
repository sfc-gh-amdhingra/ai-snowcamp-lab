# AI Snowcamp — Member Intelligence Agent Lab

Hands-on lab guide for building a Member Intelligence Agent on Snowflake using synthetic payer data, Cortex Analyst, Cortex Search, and Snowflake CoWork.

---

## Lab Guide

The lab guide is published at: **https://sfc-gh-amdhingra.github.io/ai-snowcamp-lab/** — share this URL with attendees.

---

## Repository Contents

| Path | Description |
|------|-------------|
| `index.html` | GitHub Pages lab guide (step-by-step instructions for attendees) |
| `assets/sql/setup.sql` | Snowflake setup script — creates database, warehouse, role, tables, loads data from the Git repo stage, and provisions the Snowflake CoWork object |
| `assets/sql/generate_data.sql` | Synthetic data generation script — regenerates the CSV files for members, medical_claims, pharmacy_claims, and providers. Only needed if changing the dataset; the committed CSVs are what the lab loads |
| `assets/semantic_models/member_intelligence.yaml` | Pre-built Cortex Analyst semantic model. **Fallback only** — attendees normally generate a semantic view with Autopilot in Step 4. Retained for the Step 4c manual path |
| `assets/documents/formulary_guidelines.txt` | Synthetic formulary policy document — drug tiers, PA requirements, GLP-1 criteria, step therapy |
| `assets/documents/medical_benefits_summary.txt` | Synthetic medical benefits summary — preventive care, specialist, PT, DME, OOP maximum |
| `assets/documents/quality_stars_measures.txt` | Synthetic quality/Stars measures reference — HEDIS measure specs, benchmarks, care gap definitions |

---

## Facilitator Checklist (Before Lab Day)

Attendees create their own Snowflake **free trial accounts** on the day. Nothing can be pre-provisioned in their environments, so the checklist below is about validating the guide, not preparing accounts.

1. **Dry run on a throwaway trial account.** Create a trial through the same signup flow attendees will use and run the lab start to finish. This is the only meaningful pre-flight check — an internal or demo account will not reproduce trial behaviour. See "Dry Run Checklist" below.
2. Confirm the CSV data files in `assets/data/` are committed and current on `main` — `setup.sql` loads them directly from the Git repository stage via `COPY FILES`, so whatever is on `main` is what attendees get.
3. Confirm the GitHub Pages guide is publishing the latest `index.html`.
4. Brief your floor helpers on the "Troubleshooting" table below. With a large room on fresh trial accounts, most raised hands will be one of those five items.
5. Share the lab guide URL. No ZIP bundle or credential distribution is needed — the Git repository integration in Step 3 pulls all SQL, documents, and data.

> No cloud storage credentials are involved. Earlier versions of this lab staged CSVs in Azure Blob Storage and required a SAS token to be embedded in `setup.sql` before distribution; that is no longer the case.

---

## Dry Run Checklist

Run these on a fresh trial account and record the answers — they are the assumptions the guide is built on.

| Check | Why it matters |
|-------|----------------|
| Does `SHOW SNOWFLAKE INTELLIGENCES` return a row *before* setup runs? | Determines whether trials ship with a CoWork object pre-provisioned. The guide handles both cases, but confirming removes the unknown. |
| Does `CREATE SNOWFLAKE INTELLIGENCE` succeed as ACCOUNTADMIN? | Section 7b of `setup.sql` depends on it. It fails silently by design, so check explicitly rather than assuming. |
| Does `SHOW AGENTS IN SNOWFLAKE INTELLIGENCE ...` list the agent after Step 6? | This is the Step 6b verification attendees will run. |
| Does the agent appear at `ai.snowflake.com`? | Route 1 in Step 7. |
| Does **AI & ML → Agents → Preview in Snowflake CoWork** work? | Route 2 in Step 7, the registration-independent fallback. |
| Do all Cortex calls work with the granted roles? | `setup.sql` grants `CORTEX_USER` plus `CORTEX_AGENT_USER` and `AI_FUNCTIONS_USER` defensively, following the May 2026 move to per-function privileges. |
| Are the Snowsight nav labels as written? | Labels changed with the CoWork rename; the guide says **AI & ML → Agents**, **→ Analyst**, **→ Search**. Correct them if the live UI differs. |
| Is the MEDIUM warehouse comfortable within trial credits for ~90 min? | Trials have a fixed credit allowance. |
| Does the chosen region support the required models with cross-region inference enabled? | Step 3a sets `CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'`. |
| Do any payer or clinical prompts trip AI guardrails? | Guardrails now apply to CoWork and Agents. Test every sample question in Module 8. |

---

## Troubleshooting (Floor Helper Reference)

| Symptom | Cause | Fix |
|---------|-------|-----|
| Agent missing from the CoWork list | Not registered on the CoWork object. Opening CoWork **Settings** can create that object, after which only registered agents appear. | Re-run the `ADD AGENT` statement in Step 6b. Or use Step 7 Route 2, which is unaffected. |
| `Insufficient privileges to operate on account` during setup | Trial account provisioning variance. | Expected and handled — sections 7 and 7b catch it. Setup continues; use Step 7 Route 2. |
| Autopilot suggests no relationships, or different metrics than the guide shows | Model output is non-deterministic and has changed since April. | Expected. Step 4c documents the manual fallback for all three join keys. |
| Cortex Search returns no results | Embedding index still building (1–2 min), or `POLICY_DOCS` stage is empty. | `LIST @POLICY_DOCS` should show 3 files. Re-run the Step 5a `COPY FILES`. |
| Model or LLM unavailable error | Cross-region inference not enabled for the trial's region. | Re-run Step 3a as ACCOUNTADMIN. |

---

## About the Dataset

The lab uses fully synthetic healthcare payer data modeled after a commercial health plan. The dataset includes four tables — `members`, `medical_claims`, `pharmacy_claims`, and `providers` — with realistic distributions of diagnoses, procedures, drug fills, and member demographics. All member identifiers, names, and clinical values are randomly generated and do not correspond to any real individuals or patient records.

---

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). This repository and all synthetic data assets are provided for educational and demonstration purposes only.
