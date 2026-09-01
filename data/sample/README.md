# Sample Data

This directory contains small local test samples derived from Rubin DP catalogs:

- `rubin_dp1_1000_sample_scrambled.parquet`
- `rubin_dp2_1000_sample_scrambled.parquet`

For data-safety and anonymization purposes, these samples were column-wise
shuffled. Each column preserves useful technical properties for testing, such as
names, types, approximate value ranges, and missing-value behavior, but row-level
relationships between columns were intentionally broken.

Because of this scrambling, these files are suitable for software tests and
report rendering checks only. They have no scientific validity and must not be
used for scientific analysis, validation of astrophysical relationships, or
production QA interpretation.

The DP1 sample does not contain native magnitude columns. It is included to test
the workflow path that converts flux and flux-error columns to magnitudes and
magnitude errors at execution time.
