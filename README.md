# Automatic Photometric QA

Configurable quality-assurance reports for large photometric catalogs. The
project uses a Jupyter notebook template plus a YAML configuration file to
produce executed HTML reports from the command line.

## Repository Layout

```text
.
├── configs/
│   ├── automatic_photometric_qa.yaml              # Production-like LIneA/HPC configuration
│   ├── automatic_photometric_qa_dp2_local_test.yaml   # DP2 local test configuration
│   └── automatic_photometric_qa_dp1_local_test.yaml  # DP1 local test configuration
├── data/
│   ├── footprints/                                # Survey footprint curves used in plots
│   └── sample/                                    # Scrambled parquet sample for local tests
├── notebooks/
│   └── automatic_photometric_qa.ipynb             # Notebook template
├── scripts/
│   └── generate_automatic_photometric_qa.py       # Command-line report generator
├── environment.yml                                # Conda environment
└── README.md
```

## Environment

Create and activate the Conda environment:

```bash
conda env create -f environment.yml
conda activate automatic-photometric-qa
```

If you prefer to create the environment manually:

```bash
conda create -n automatic-photometric-qa python=3.11
conda activate automatic-photometric-qa
conda install -c conda-forge \
  dask distributed dask-jobqueue \
  pandas numpy matplotlib seaborn \
  pyyaml ipython ipykernel nbclient nbconvert nbformat \
  pyarrow
```

After activating the environment, run the CLI with the active environment's
`python`.

## Running a QA Report

Run the DP2 local test configuration and export an HTML report with code cells:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa_dp2_local_test.yaml \
  --output outputs/automatic_photometric_qa_dp2_local_test.html
```

Export the same report without code cells:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa_dp2_local_test.yaml \
  --output outputs/automatic_photometric_qa_dp2_local_test_no_code.html \
  --hide-code
```

Run the DP1 local test configuration, which converts flux columns to magnitudes:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa_dp1_local_test.yaml \
  --output outputs/automatic_photometric_qa_dp1_local_test_no_code.html \
  --hide-code
```

Run the main configuration:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa.yaml \
  --output outputs/automatic_photometric_qa.html
```

Optionally save the executed notebook used to generate the HTML:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa.yaml \
  --output outputs/automatic_photometric_qa.html \
  --executed-notebook outputs/automatic_photometric_qa_executed.ipynb
```

By default, the CLI uses `notebooks/automatic_photometric_qa.ipynb` as the
notebook template.

During execution, the CLI prints progress messages for the main QA steps, for
example:

```text
[12/34] Computing catalog size
[14/34] Computing total row count
[25/34] Generating spatial distribution plot
[31/34] Computing magnitude-error statistics
```

## Configuration

The YAML file controls the notebook title, catalog input, Dask cluster, selected
statistics, plots, and survey footprints.

Required YAML sections:

- `notebook`: report title, subtitle, and last verified run date.
- `catalog`: input parquet file or directory.
- `cluster`: Dask backend configuration.

Optional YAML sections:

- `basic_statistics`
- `unique_count`
- `spatial_distribution`
- `magnitudes`
- `magnitude_errors`

If an optional section is absent from the YAML, the corresponding notebook
section is removed before execution and does not appear in the exported HTML.
The basic product information section always runs: catalog size, total row
count, total column count, and column names.

Relative paths in the YAML are resolved relative to the YAML file location.

### Basic Statistics Columns

`basic_statistics.columns` accepts:

- `null`: use the first `default_first_n` catalog columns. The default is 20.
- `"all"`: use all catalog columns.
- A list of column names.

Safeguards are intentionally strict for wide catalogs:

- `columns: "all"` requires `basic_statistics.allow_all_columns: true`.
- Explicit column lists longer than `basic_statistics.max_columns` fail unless
  `basic_statistics.allow_many_columns: true` is set.
- The default `basic_statistics.max_columns` is 100.

These checks prevent accidental large Dask graphs and heavy reductions. If a
wide run is scientifically required, opt in explicitly in the YAML so the report
configuration records that choice.

### Exact Unique Counts

`unique_count` always reports an exact global count or fails. It never reports an
approximate count, sampled count, or per-partition count as if it were global.

Use `unique_count.max_unique_values` to cap the number of unique values that may
be collected by the driver while computing the exact result:

```yaml
unique_count:
  column: tract
  max_unique_values: 10000
```

If the exact global cardinality exceeds `max_unique_values`, the run raises an
error and no count is reported. Increase this limit only when the high-cardinality
exact count is scientifically required and the driver has enough memory.

### Flux-to-Magnitude Conversion

The `magnitudes` and `magnitude_errors` sections can use either native magnitude
columns, such as `psfMag`, or flux columns, such as `psfFlux`.

When a configured magnitude model contains `Flux`, the notebook lazily converts
that Dask column to magnitude values during execution:

```text
magnitude = mag_offset - 2.5 log10(flux)
```

In that case, `magnitudes.mag_offset` is required. For example:

```yaml
magnitudes:
  bands: [u, g, r, i, z, y]
  models: [psfFlux, kronFlux, cModelFlux]
  mag_offset: 31.4
  model_labels:
    psfFlux: PSF
    kronFlux: Kron
    cModelFlux: cModel
```

When a configured magnitude-error model contains `FluxErr`, the notebook lazily
loads the matching flux column and converts the error with:

```text
sigma_mag = 2.5 / ln(10) * flux_err / flux
```

For example, `psfFluxErr` uses `psfFlux` as the matching flux column.

### Cluster Backends

Local cluster example:

```yaml
cluster:
  type: local
  local:
    n_workers: 1
    cores: 1
    memory: "2GB"
```

SLURM cluster example:

```yaml
cluster:
  type: slurm
  slurm:
    n_workers: 30
    queue: "cpu"
    account: "hpc-public"
    interface: "ib0"
    cores: 2
    processes: 1
    memory: "32GB"
    walltime: "02:00:00"
    adapt:
      minimum_jobs: 30
      maximum_jobs: 30
    wait_for_workers: 30
```

## Test Data

The local test configurations use scrambled DP1 and DP2 parquet samples under
`data/sample/`. See `data/sample/README.md` for details about how these samples
should be interpreted.

## Notes

- `configs/automatic_photometric_qa_dp2_local_test.yaml` is intended for quick DP2 local validation.
- `configs/automatic_photometric_qa_dp1_local_test.yaml` is intended for quick DP1 local validation, including flux-to-magnitude conversion.
- `configs/automatic_photometric_qa.yaml` mirrors the current LIneA/HPC QA configuration.
- Generated HTML reports and executed notebooks should be written under
  `outputs/` or `reports/`, which are ignored by Git.
