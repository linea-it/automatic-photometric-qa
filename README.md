# Automatic Photometric QA

Configurable quality-assurance reports for large photometric catalogs. The
project uses a Jupyter notebook template plus a YAML configuration file to
produce executed HTML reports from the command line.

## Repository Layout

```text
.
├── configs/
│   ├── production/                                 # DP1 and DP2 Parquet/HATS/PostgreSQL configurations
│   ├── automatic_photometric_qa_dp2_local_test.yaml      # DP2 local test configuration
│   ├── automatic_photometric_qa_dp1_local_test.yaml      # DP1 local test configuration
│   └── automatic_photometric_qa_dp1_dp2_local_test.yaml  # Multi-catalog local test configuration
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
  pyarrow lsdb psycopg
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

Run the combined DP1 + DP2 local test configuration:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa_dp1_dp2_local_test.yaml \
  --output outputs/automatic_photometric_qa_dp1_dp2_local_test_no_code.html \
  --hide-code
```

Run the DP1 HATS local test configuration:

```bash
python scripts/generate_automatic_photometric_qa.py configs/automatic_photometric_qa_dp1_hats_local_test.yaml \
  --output outputs/automatic_photometric_qa_dp1_hats_local_test_no_code.html \
  --hide-code
```

Run a production configuration from the repository root (DP2 HATS example):

```bash
python scripts/generate_automatic_photometric_qa.py configs/production/rubin_dp2_QA_hats.yaml \
  --output outputs/automatic_photometric_qa.html
```

Optionally save the executed notebook used to generate the HTML:

```bash
python scripts/generate_automatic_photometric_qa.py configs/production/rubin_dp2_QA_hats.yaml \
  --output outputs/automatic_photometric_qa.html \
  --executed-notebook outputs/automatic_photometric_qa_executed.ipynb
```

By default, the CLI uses `notebooks/automatic_photometric_qa.ipynb` as the
notebook template.

Run the PostgreSQL report (connection details are described below):

```bash
python scripts/generate_automatic_photometric_qa.py configs/production/rubin_dp1_QA_postgres.yaml \
  --output outputs/rubin_dp1_QA_postgres.html \
  --hide-code
```

During execution, the CLI prints progress messages for the main notebook steps
and for each configured catalog section. Internal progress markers are not stored
in the exported HTML.

Example:

```text
[09/10] Rendering configured catalog QA sections
Starting catalog 1: Rubin DP1 — scrambled local sample
[Rubin DP1 — scrambled local sample] Computing total row count
[Rubin DP1 — scrambled local sample] Computing basic statistics
[Rubin DP1 — scrambled local sample] Generating magnitude histograms
Starting catalog 2: Rubin DP2 — scrambled local sample
[Rubin DP2 — scrambled local sample] Computing unique count
```

## Configuration

The YAML file controls the global notebook title, Dask cluster, catalog inputs,
selected statistics, plots, and survey footprints.

Required global YAML sections for Parquet and HATS inputs:

- `notebook`: report title, subtitle, optional introduction, and last verified run date.
- `catalogs`: one or more catalog configurations.
- `cluster`: Dask backend configuration shared by all catalogs.

Each available catalog entry in `catalogs` requires:

- `title`: catalog section title rendered as a level-2 heading.
- `path`: input parquet file or directory.
- `parquet_pattern`: file pattern used when `path` is a directory. The
  default is `*.parquet`.
- `omit_paths`: when `true`, the catalog path is not included in the rendered
  report. It defaults to `false` for backward compatibility.

Catalog entries may also be placeholders for planned data products:

```yaml
catalogs:
  - title: Future Object Catalog
    omit_paths: true
    status: planned
    markdown: To be done
```

For planned catalogs, `path` is not required and no data-processing sections are
run. The notebook renders only the catalog title and the configured markdown.
Use `status: planned` explicitly so a missing `path` in an available catalog is
still treated as a configuration error.

Optional per-catalog sections:

- `basic_statistics`
- `unique_count`
- `spatial_distribution`
- `survey_area`
- `magnitudes`
- `magnitude_errors`
- `magnitude_error_trends`
- `plot_pixels` (HATS only)
- `plot_coverage` (HATS only)

If an optional section is absent from a catalog entry, that section does not
appear for that catalog. The basic product information section always runs for
each catalog: catalog size, total row count, total column count, and a scrollable
table containing column names and data types.

Catalog paths that contain `collection.properties` or `hats.properties` are
treated as HATS catalogs. The primary table is opened with
`lsdb.open_catalog(primary_catalog_path, columns="all")` to inspect its schema,
without loading a collection's margin cache. Each QA section opens the same
primary table with `columns=[...]` so Parquet reads include only its required
columns. The sections use LSDB partition operations for counts, area,
histograms, and magnitude diagnostics. For HATS, `basic_statistics` uses
`Catalog.aggregate_column_statistics()` on the configured columns and reports
row count, null count, minimum, and maximum from Parquet metadata. Mean,
standard deviation, and percentiles are unavailable in that metadata summary.
Its `Rows` includes null entries, while `Nulls` reports how many there are.
The HATS metadata also supplies the total row count without reading catalog
rows. Non-HATS inputs continue to be
read directly with `dask.dataframe.read_parquet`; their `basic_statistics`
percentiles from Dask `describe()` are approximate, and `count` excludes null
entries. The report names any selected columns omitted by Dask's default
data-type selection. Both inputs use the same YAML
sections and options, apart from the HATS-only `plot_pixels` and
`plot_coverage` sections.

The exported report uses this heading hierarchy:

- Level 1: global notebook title beside the logos.
- Level 2: each catalog title from `catalogs[*].title`.
- Level 3: per-catalog QA sections, such as basic product information,
  unique count, spatial distribution, magnitudes, and magnitude errors.

Relative paths in the YAML are resolved relative to the YAML file location.

### PostgreSQL Inputs

Set `from_database: true` and provide a `database` mapping to read selected
catalogs directly from PostgreSQL. In this mode, `cluster` and catalog `path`
are not required. Each available catalog instead requires `schema` and `table`.
The report opens one connection, lists all visible non-system schemas and
tables, then renders the selected table name, row and column counts, column
names and PostgreSQL types, five preview rows by default, and configured basic
statistics. Other distributed QA sections are not run for database catalogs.

```yaml
from_database: true

database:
  credentials_file: ~/.pgcredential
  connect_timeout: 15
  list_objects: true

catalogs:
  - title: DP1 object
    schema: lsst_dp1
    table: object_original_camelcase
    preview:
      rows: 5
      columns: all
    basic_statistics:
      columns: [objectId, coord_ra, coord_dec, g_cModelFlux]
      max_columns: 100
```

Connection values are resolved in this order: PostgreSQL environment variable,
direct YAML value, then `credentials_file`. The defaults use `PGHOST`,
`PGDATABASE`, `PGUSER`, `PGPASSWORD`, and `PGPORT`. Avoid putting passwords in
version-controlled YAML; use `PGPASSWORD` or the credential file instead.

Standard PostgreSQL password files are supported directly. Blank lines and
entries beginning with `#` are ignored, and escaped colons and backslashes are
handled according to the `.pgpass` format:

```yaml
database:
  credentials_file: ~/.pgpass
```

```text
# hostname:port:database:username:password
db.example.org:<port>:<database>:<username>:<password>
```

When the file has multiple active entries, the first entry compatible with any
host, port, database, and user already supplied through the environment or YAML
is selected. With a single active entry, those connection values can all be
read from the file. Wildcards in the first four fields are accepted for
matching, but cannot supply a missing connection value.

The existing custom credential-file format remains supported. Its default
patterns match the format used by
`rubin_dp1_postgres.ipynb` (`user:`, `pass:`, `- long:`, `database name:`, and
optional `port:`). Different files can be supported with regular expressions:

```yaml
database:
  credentials_file: ~/.pgcredential
  credential_patterns:
    host: '^host:\s*(.+)$'
    dbname: '^database:\s*(.+)$'
    user: '^user:\s*(.+)$'
    password: '^password:\s*(.+)$'
```

Set `database.list_objects: false` to omit the schema/table inventory. Preview
defaults to five rows and all columns; `preview.columns` may instead be an
explicit list, and `preview: false` disables it. `preview.rows` is limited to
1–100.

Database basic statistics calculate non-null count, mean, sample standard
deviation, minimum, and maximum in PostgreSQL. When
`basic_statistics.columns` is `null` or omitted, the first
`default_first_n` numeric columns are used. Explicit selections must contain
numeric PostgreSQL columns because `AVG` and `STDDEV_SAMP` are part of this
summary.

`notebook.introduction` is optional. When it is configured, the report renders it
between horizontal rules below the global header. When it is absent or empty, no
introduction section is shown.

Minimal multi-catalog structure:

```yaml
notebook:
  title: Rubin QA Report
  subtitle: Basic dataset characterization
  last_verified_run: '2026-08-25'
  introduction: >
    This notebook provides lightweight statistics and diagnostic plots for quick
    characterization of the data product.

catalogs:
  - title: Rubin DP1 Object Catalog
    path: ../data/sample/rubin_dp1_1000_sample_scrambled.parquet
    parquet_pattern: '*.parquet'
    basic_statistics:
      columns: null
      default_first_n: 20
      max_columns: 100
    unique_count:
      column: tract
      max_unique_values: 10000

  - title: Rubin DP2 Object Catalog
    path: ../data/sample/rubin_dp2_1000_sample_scrambled.parquet
    parquet_pattern: '*.parquet'
    spatial_distribution:
      ra_column: coord_ra
      dec_column: coord_dec
      ra_edge_count: 180
      dec_edge_count: 90
      title_suffix: Spatial Distribution
    survey_area:
      ra_column: coord_ra
      dec_column: coord_dec
      order: 12
      split_out: 64

  - title: Future Visit Catalog
    status: planned
    markdown: To be done

cluster:
  type: local
  local:
    n_workers: 1
    cores: 1
    memory: 2GB
```

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

Use `unique_count.max_unique_values` to cap the unique values retained at each
partition and merge step. Partial sets are merged on workers, so the driver
receives at most `max_unique_values + 1` values:

```yaml
unique_count:
  column: tract
  max_unique_values: 10000
  list_values: true
  list_rows: 10
```

If the exact global cardinality exceeds `max_unique_values`, the run raises an
error and no count is reported. The count still scans the selected column across
all catalog partitions, so it can take time for a very large catalog.

Set `unique_count.list_values: true` to render the exact unique values in a
scrollable HTML text box below the count. The optional `unique_count.list_rows`
setting controls the visible height of that box.

### Survey Area and Object Density

`survey_area` estimates the sky area covered by a catalog from occupied HEALPix
pixels and reports the mean object density as total rows divided by that area.
The section is opt-in: omit it or set it to `false` to skip the calculation.

```yaml
survey_area:
  ra_column: coord_ra
  dec_column: coord_dec
  order: 12
  split_every: 8
  split_out: 64
```

`order` defaults to 12 when the section is enabled. If `ra_column` or
`dec_column` is omitted, the notebook reuses the matching
`spatial_distribution` coordinate setting when available, otherwise it falls
back to `coord_ra` and `coord_dec`.

The calculation reads only the coordinate columns. Each Dask partition converts
coordinates to HEALPix pixels and deduplicates locally; the global unique-pixel
count is then computed with a distributed Dask `drop_duplicates`, controlled by
`split_out`, `split_every`, and optional `shuffle_method`. Raising `split_out`
can improve parallelism for large footprints at the cost of more shuffle tasks.

For HATS inputs, the notebook automatically prefers the catalog's existing
HEALPix column from `hats.properties`, such as `_healpix_29`, when its order is
at least as fine as `survey_area.order`. In that case, each partition only
degrades the existing pixel IDs to the configured order before deduplication,
which avoids a full RA/Dec-to-HEALPix conversion pass. Set
`survey_area.use_hats_healpix_column: false` to force the coordinate-based path,
or provide `healpix_column` and `healpix_column_order` explicitly for another
precomputed HEALPix column.

### Flux-to-Magnitude Conversion

The `magnitudes` and `magnitude_errors` sections can mix native magnitude
columns, such as `psfMag`, with flux columns, such as `gaap1p0Flux` or
`psfFlux`.

When a configured magnitude model contains `Flux`, the notebook lazily converts
that Dask column to magnitude values during execution. Models that already use
`Mag` are read directly without conversion:

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

Flux conversion is intentionally lazy and does not run an additional full-catalog
validation pass. Non-positive, missing, or non-finite flux values are converted
to `NaN` in the Dask expression and are excluded by the existing finite-value
filters used by histograms and distribution statistics. The same rule is applied
to invalid flux or flux-error values in magnitude-error conversion.

For HATS catalogs, magnitude and magnitude-error statistics estimate configured
quantiles from the same fixed-bin histogram used for the other statistics. This
avoids a second read and a separate distributed quantile calculation for each
band and model. Count, mean, threshold fractions, and out-of-range counts still
come directly from the values. Quantile resolution is set by
`statistics.peak_bin_width` (0.1 mag and 0.01 mag error in the production YAML).
The report marks these estimates with `≈` and rounds them to the bin width.
Non-HATS catalogs keep the existing approximate Dask quantile calculation. The
report also marks these quantiles with `≈`. To request this method for a HATS
section, set `quantile_method: dask` under that section's
`statistics` mapping; `quantile_method: histogram` is also available explicitly.

### Magnitude-Error Trends

The optional `magnitude_error_trends` section renders magnitude versus
magnitude-error trend plots for configured bands, with two plots per row by
default. It uses all configured
`magnitudes.models` by default and infers matching error models by appending
`Err`, for example `psfMag` to `psfMagErr` and `gaap1p0Flux` to
`gaap1p0FluxErr`.

The plot computes a 2D histogram per model and magnitude bin. The line is the
binned mean magnitude error, and the shaded region is the configured approximate
quantile range measured from the binned error distribution.

```yaml
magnitude_error_trends:
  bands: [u, g, r, i, z, y]
  ncols: 2
  bins: 50
  magnitude_range: [15, 35]
  error_range: [0, 2]
  dispersion_quantiles: [0.16, 0.84]
  min_count: 1
  fill_alpha: 0.15
  split_every: 8
```

Set `models` and `error_models` explicitly when the magnitude and error model
names do not follow the default `Err` suffix convention. Both lists must have
the same length. Use `band` instead of `bands` to render a single-band plot.

The command-line runner suppresses the known non-fatal NumPy/Dask quantile
warning caused by these invalid values. It does not suppress exceptions,
tracebacks, missing-column errors, failed Dask tasks, or other fatal failures.

### HATS Plots

For HATS catalog inputs, two optional sections can render additional LSDB maps:

- `Catalog.plot_pixels(projection="MOL")`
- `Catalog.plot_coverage()`

The `plot_pixels` colors show HEALPix order (pixel angular resolution), not
object counts. Both HATS maps use a gray background so uncovered regions remain
distinct from colored pixels; pixel polygon edges are drawn without
antialiasing to reduce pale seams in raster output.

These sections are opt-in. If `plot_pixels` or `plot_coverage` is absent, that
map is not rendered. Optional keyword arguments can be passed through the YAML:

```yaml
plot_pixels:
  projection: MOL
plot_coverage: {}
```

Set either section to `false` or omit it to skip that plot for a HATS catalog.
If either section is configured for a non-HATS parquet input, the run fails with
a configuration error. A section value of `true`, `null`, or `{}` renders the
plot with default LSDB arguments.

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
- `configs/automatic_photometric_qa_dp1_dp2_local_test.yaml` is intended for multi-catalog local validation.
- `configs/production/` contains the current LIneA/HPC DP1 and DP2 configurations.
- Generated HTML reports and executed notebooks should be written under
  `outputs/` or `reports/`, which are ignored by Git.
