# Parquet
```bash
python scripts/generate_automatic_photometric_qa.py configs/production/rubin_dp1_QA_parquet.yaml --output outputs/rubin_dp1_qa_parquet.html --executed-notebook outputs/rubin_dp1_qa_parquet.ipynb --hide-code
```

# HATS
```bash
python scripts/generate_automatic_photometric_qa.py configs/production/rubin_dp1_QA_hats.yaml --output outputs/rubin_dp1_qa_hats.html --executed-notebook outputs/rubin_dp1_qa_hats.ipynb --hide-code
```

# PostgreSQL
```bash
IPYTHONDIR=/tmp/automatic-photometric-qa-ipython python scripts/generate_automatic_photometric_qa.py configs/production/rubin_dp1_QA_postgres.yaml --output outputs/rubin_dp1_QA_postgres.html --executed-notebook outputs/rubin_dp1_qa_postgres.ipynb --hide-code
```