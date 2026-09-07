# AI Snowcamp — Member Intelligence Agent Lab

Hands-on lab guide for building a Member Intelligence Agent on Snowflake using synthetic payer data, Cortex Analyst, Cortex Search, Cortex AI functions, and Snowflake CoWork.

---

## Lab Guide

The lab guide is published at: **https://sfc-gh-amdhingra.github.io/ai-snowcamp-lab/** — share this URL with attendees.

---

## Repository Contents

| Path | Description |
|------|-------------|
| `index.html` | GitHub Pages lab guide (step-by-step instructions for attendees) |
| `assets/sql/setup.sql` | Snowflake setup script — creates database, warehouse, role, tables, loads data from the Git repo stage, grants the Cortex AI roles, provisions the Snowflake CoWork object, and runs ten reporting-style seed queries so Step 4 Autopilot has query history to learn from |
| `assets/sql/generate_data.sql` | Synthetic data generation script — regenerates the CSV files for members, medical_claims, pharmacy_claims, and providers. Only needed if changing the dataset; the committed CSVs are what the lab loads |
| `assets/semantic_models/member_intelligence.yaml` | Pre-built Cortex Analyst semantic model. **Fallback only** — attendees normally generate a semantic view with Autopilot in Step 4. Retained for the Step 4c manual path |
| `assets/data/*.csv` | The four payer tables the lab loads. **Must stay committed** — `setup.sql` reads them from the Git repository stage, so excluding them would break setup for every attendee |
| `assets/documents/formulary_guidelines.txt` | Synthetic formulary policy document — drug tiers, PA requirements, GLP-1 criteria, step therapy |
| `assets/documents/medical_benefits_summary.txt` | Synthetic medical benefits summary — preventive care, specialist, PT, DME, OOP maximum |
| `assets/documents/quality_stars_measures.txt` | Synthetic quality/Stars measures reference — HEDIS measure specs, benchmarks, care gap definitions |
| `assets/documents/pa_denial_letter.pdf` | Synthetic prior authorization denial letter (2 pages, tables, SAMPLE watermark) — the source document for the `AI_EXTRACT` demo in Step 5d |
| `assets/documents/generate_pa_denial_pdf.py` | Regenerates the PDF above. Not run by the lab; use it only to change the document. Requires `pip install fpdf2` |
| `assets/img/signup_csp_dub.png` | Trial signup screenshot for **Dublin** (AWS, EU Frankfurt) — currently referenced by the guide |
| `assets/img/signup_csp_blr.png` | Trial signup screenshot for **Bangalore** (AWS, AP Tokyo) — swap this in and change the region text before that event |

---

## Facilitator Checklist (Before Lab Day)

Attendees create their own Snowflake **free trial accounts** on the day. Nothing can be pre-provisioned in their environments, so the checklist below is about validating the guide, not preparing accounts.

1. **Dry run on a throwaway trial account.** A full dry run was completed on an AWS Tokyo trial in Sept 2026 and the SQL path is validated end to end — see "Dry Run Checklist" below for what was confirmed and what is still open. Repeat it if more than a few weeks have passed, or if the event region changes. An internal or demo account will not reproduce trial behaviour.
2. Confirm the CSV data files in `assets/data/` are committed and current on `main` — `setup.sql` loads them directly from the Git repository stage via `COPY FILES`, so whatever is on `main` is what attendees get.
3. Confirm the GitHub Pages guide is publishing the latest `index.html`.
4. Brief your floor helpers on the "Troubleshooting" table below. With a large room on fresh trial accounts, most raised hands will be one of those items.
5. Share the lab guide URL. No ZIP bundle or credential distribution is needed — the Git repository integration in Step 3 pulls all SQL, documents, and data.

> No cloud storage credentials are involved. Earlier versions of this lab staged CSVs in Azure Blob Storage and required a SAS token to be embedded in `setup.sql` before distribution; that is no longer the case.

---

## Dry Run Checklist

Most of this was validated on a fresh AWS Tokyo trial (Sept 2026). Confirmed results are recorded below — re-check them only if trial provisioning appears to have changed. The **Open** rows are the ones that cannot be driven from SQL and still need a human pass in Snowsight.

| Check | Status | Notes |
|-------|--------|-------|
| Does `SHOW SNOWFLAKE INTELLIGENCES` return a row *before* setup runs? | **No** | Fresh trials ship with no CoWork object. This is why `setup.sql` 7b creates one — otherwise the lab works by accident until someone opens CoWork Settings. |
| Does `CREATE SNOWFLAKE INTELLIGENCE` succeed as ACCOUNTADMIN? | **Yes** | Trial ACCOUNTADMIN holds the privilege. |
| Do all Cortex calls work with the granted roles? | **Yes** | `CORTEX_USER` plus `CORTEX_AGENT_USER` and `AI_FUNCTIONS_USER`, each granted defensively in its own exception block. |
| Does `SHOW AGENTS IN SNOWFLAKE INTELLIGENCE ...` list the agent after registration? | **Yes** | This is the Step 6b verification attendees run. |
| Do `AI_EXTRACT` and `AI_CLASSIFY` work? | **Yes** | `AI_EXTRACT` returned all 10 requested fields from the staged PDF, including values inside tables. `AI_CLASSIFY` via the `CLASSIFY_CLAIM` UDF classified correctly as an agent tool. |
| Does the agent orchestrate all three tools? | **Yes** | Verified: Analyst SQL, then five parallel `ClassifyClaim` calls, then Search for coverage policy, synthesized into one answer. |
| Does the agent appear at `ai.snowflake.com`? | **Open** | Route 1 in Step 7. |
| Does **AI & ML → Agents → Preview in Snowflake CoWork** work? | **Open** | Route 2 in Step 7, the registration-independent fallback. |
| Does the chosen region support the required models with cross-region inference enabled? | **Yes** (Tokyo) | Step 3a sets `CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'`. Re-confirm for Frankfurt before Dublin. |
| **Step 4 Autopilot in Snowsight** | **Open** | Cannot be driven from SQL. Confirm the wizard flow, the Suggestions tab, and that the saved view carries the three join keys. |
| **Step 6a CoCo prompt** | **Open** | The prompt now asks CoCo for three things: build the `CLASSIFY_CLAIM` UDF, wire it as a generic agent tool, and emit the `ADD AGENT` statement. Confirm all three land — in particular that `tool_resources` includes `execution_environment` with the warehouse, since omitting it makes the agent error rather than degrade. |
| Are the Snowsight nav labels as written? | **Open** | Labels changed with the CoWork rename; the guide says **AI & ML → Agents**, **→ Analyst**, **→ Search**. Correct them if the live UI differs. |
| Is the MEDIUM warehouse comfortable within trial credits for ~95 min? | Unverified | Trials have a fixed credit allowance. |
| Do any payer or clinical prompts trip AI guardrails? | Partly | The Step 8 questions tested so far returned clean. Worth running the full set. |

---

## Troubleshooting (Floor Helper Reference)

| Symptom | Cause | Fix |
|---------|-------|-----|
| Agent missing from the CoWork list | Not registered on the CoWork object. Opening CoWork **Settings** can create that object, after which only registered agents appear. | Re-run the `ADD AGENT` statement in Step 6b. Or use Step 7 Route 2, which is unaffected. |
| `Insufficient privileges to operate on account` during setup | Trial account provisioning variance. | Expected and handled — sections 7 and 7b catch it. Setup continues; use Step 7 Route 2. |
| Autopilot suggests no relationships, or different metrics than the guide shows | Model output is non-deterministic and has changed since April. | Expected. Step 4c documents the manual fallback for all three join keys. |
| Cortex Search returns no results | Embedding index still building (1–2 min), or `POLICY_DOCS` stage is empty. | `LIST @POLICY_DOCS` should show 4 files (3 `.txt` plus the PDF). Re-run the Step 5a `COPY FILES`. |
| `Invalid UTF8 detected in string` on `pa_denial_letter.pdf` during Step 5b | The stage read is picking up the PDF and trying to decode binary as text. | The Step 5b `SELECT` must include `pattern => '.*[.]txt'` on the stage read. If an attendee is on a cached copy of the guide without it, add it. |
| Model or LLM unavailable error | Cross-region inference not enabled for the trial's region. | Re-run Step 3a as ACCOUNTADMIN. |
| Agent replies with *"The Analyst tool is missing an execution environment"* | CoCo generated the agent spec without a warehouse in `tool_resources`. | Add `execution_environment: {type: warehouse, warehouse: OPTUM_LAB_WH}` under the Analyst tool's `tool_resources` entry and re-run `CREATE OR REPLACE AGENT`. |
| Step 5d `AI_EXTRACT` takes several minutes | Cold start on the first call in the account, not a failure. | Expected — the guide tells attendees to leave it running and continue to Step 6. Only the first person to run it in a given account waits; it is fast afterwards. |

---

## About the Dataset

The lab uses fully synthetic healthcare payer data modeled after a commercial health plan. The dataset includes four tables — `members`, `medical_claims`, `pharmacy_claims`, and `providers` — with realistic distributions of diagnoses, procedures, drug fills, and member demographics. All member identifiers, names, and clinical values are randomly generated and do not correspond to any real individuals or patient records.

---

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). This repository and all synthetic data assets are provided for educational and demonstration purposes only.
