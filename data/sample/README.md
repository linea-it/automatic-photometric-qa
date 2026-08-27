# Sample Data

`rubin_dp2_1000_sample_scrambled.parquet` is a small local test sample derived
from the Rubin DP2 catalog used by the QA workflow.

For data-safety and anonymization purposes, the sample was column-wise shuffled.
Each column preserves useful technical properties for testing, such as names,
types, approximate value ranges, and missing-value behavior, but row-level
relationships between columns were intentionally broken.

Because of this scrambling, the file is suitable for software tests and report
rendering checks only. It has no scientific validity and must not be used for
scientific analysis, validation of astrophysical relationships, or production QA
interpretation.
