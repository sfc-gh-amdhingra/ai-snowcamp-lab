# AI Snowcamp — Member Intelligence Agent Lab

Hands-on lab guide for building a Member Intelligence Agent on Snowflake using synthetic payer data, Cortex Analyst, Cortex Search, and Snowflake Intelligence.

---

## Lab Guide

The lab guide is published at: **https://sfc-gh-amdhingra.github.io/ai-snowcamp-lab/** — share this URL with attendees.

---

## Repository Contents

| Path | Description |
|------|-------------|
| `index.html` | GitHub Pages lab guide (step-by-step instructions for attendees) |
| `assets/sql/setup.sql` | Snowflake setup script — creates database, stages, tables, loads data, configures Cortex Search and Analyst |
| `assets/sql/generate_data.sql` | Synthetic data generation script — produces CSV files for members, medical_claims, pharmacy_claims, and providers |
| `assets/semantic_models/member_intelligence.yaml` | Cortex Analyst semantic model — defines metrics, dimensions, and relationships over the payer tables |
| `assets/documents/formulary_guidelines.txt` | Synthetic formulary policy document — drug tiers, PA requirements, GLP-1 criteria, step therapy |
| `assets/documents/medical_benefits_summary.txt` | Synthetic medical benefits summary — preventive care, specialist, PT, DME, OOP maximum |
| `assets/documents/quality_stars_measures.txt` | Synthetic quality/Stars measures reference — HEDIS measure specs, benchmarks, care gap definitions |

---

## Facilitator Checklist (Before Lab Day)

1. Run `assets/sql/generate_data.sql` in your SE demo account to generate the synthetic CSV files (members, medical_claims, pharmacy_claims, providers).
2. Upload the 4 CSV files to your Azure Blob Storage container (SE Azure sandbox: `sfselabs.blob.core.windows.net/ai-snowcamp/data/`).
3. Generate a SAS token with Read permissions, valid through the lab date + 2 weeks.
4. Replace `<REPLACE_WITH_SAS_TOKEN>` in `assets/sql/setup.sql` with the live SAS token before distributing.
5. Distribute the lab bundle ZIP to attendees: `setup.sql` (with token embedded) + `assets/documents/*.txt` + `assets/semantic_models/member_intelligence.yaml`.
6. Have attendees navigate to the GitHub Pages URL for the step-by-step lab guide.

---

## About the Dataset

The lab uses fully synthetic healthcare payer data modeled after a commercial health plan. The dataset includes four tables — `members`, `medical_claims`, `pharmacy_claims`, and `providers` — with realistic distributions of diagnoses, procedures, drug fills, and member demographics. All member identifiers, names, and clinical values are randomly generated and do not correspond to any real individuals or patient records.

---

## License

Licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0). This repository and all synthetic data assets are provided for educational and demonstration purposes only.
