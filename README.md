# Automatic Photometric QA

Configurable quality-assurance reports for large photometric catalogs. The
project uses a Jupyter notebook template plus a YAML configuration file to
produce executed HTML reports from the command line.

## Repository Layout

```text
.
├── configs/
│   ├── automatic_photometric_qa.yaml              # Production-like LIneA/HPC configuration
│   └── automatic_photometric_qa_local_test.yaml   # Small local test configuration
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

Run the local test configuration and export an HTML report with code cells:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa_local_test.yaml \
  --output outputs/automatic_photometric_qa_local_test.html
```

Export the same report without code cells:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa_local_test.yaml \
  --output outputs/automatic_photometric_qa_local_test_no_code.html \
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
- `"all"`: use all catalog columns at the user's own risk.
- A list of column names.

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

The local test configuration uses
`data/sample/rubin_dp2_1000_sample_scrambled.parquet`. See
`data/sample/README.md` for details about how this sample should be interpreted.

## Notes

- `configs/automatic_photometric_qa_local_test.yaml` is intended for quick local validation.
- `configs/automatic_photometric_qa.yaml` mirrors the current LIneA/HPC QA configuration.
- Generated HTML reports and executed notebooks should be written under
  `outputs/` or `reports/`, which are ignored by Git.
