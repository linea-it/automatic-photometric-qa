import ast
from datetime import date
import importlib.util
import os
from pathlib import Path
import re
import tempfile
from time import perf_counter
import unittest
from unittest import mock

import dask
import dask.array as da
import dask.dataframe as dd
import nbformat
import numpy as np
import pandas as pd
import yaml
from dask import delayed


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "generate_automatic_photometric_qa.py"
)
SPEC = importlib.util.spec_from_file_location(
    "generate_automatic_photometric_qa", SCRIPT_PATH
)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
NOTEBOOK_PATH = (
    Path(__file__).resolve().parents[1] / "notebooks" / "automatic_photometric_qa.ipynb"
)


class LoadConfigTest(unittest.TestCase):
    def load(self, config):
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "config.yaml"
            path.write_text(yaml.safe_dump(config), encoding="utf-8")
            return MODULE.load_config(path)

    def test_file_catalog_requires_cluster_and_path(self):
        config = {
            "notebook": {"title": "File QA"},
            "cluster": {"type": "local"},
            "catalogs": [{"title": "object", "path": "object.parquet"}],
        }

        self.assertEqual(self.load(config), config)

    def test_database_catalog_requires_database_schema_and_table(self):
        config = {
            "notebook": {"title": "Database QA"},
            "from_database": True,
            "database": {"credentials_file": "~/.pgcredential"},
            "catalogs": [{"title": "object", "schema": "lsst_dp1", "table": "object"}],
        }

        self.assertEqual(self.load(config), config)

    def test_database_catalog_does_not_require_cluster_or_path(self):
        config = {
            "notebook": {"title": "Database QA"},
            "from_database": True,
            "database": {},
            "catalogs": [{"schema": "public", "table": "object"}],
        }

        self.load(config)

    def test_database_catalog_reports_missing_table(self):
        config = {
            "notebook": {"title": "Database QA"},
            "from_database": True,
            "database": {},
            "catalogs": [{"schema": "public"}],
        }

        with self.assertRaisesRegex(ValueError, "missing required key.*table"):
            self.load(config)

    def test_from_database_must_be_boolean(self):
        config = {
            "notebook": {"title": "Database QA"},
            "from_database": "yes",
            "database": {},
            "catalogs": [{"schema": "public", "table": "object"}],
        }

        with self.assertRaisesRegex(ValueError, "from_database must be true or false"):
            self.load(config)


class LastVerifiedRunTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
        source = next(
            cell.source
            for cell in notebook.cells
            if cell.get("id") == "initial-configuration"
        )
        tree = ast.parse(source)
        selected_nodes = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "resolve_last_verified_run"
        ]
        cls.namespace = {"date": date}
        exec(
            compile(
                ast.Module(selected_nodes, type_ignores=[]),
                str(NOTEBOOK_PATH),
                "exec",
            ),
            cls.namespace,
        )

    def test_auto_uses_current_system_date(self):
        self.assertEqual(
            self.namespace["resolve_last_verified_run"]("auto"),
            date.today().isoformat(),
        )

    def test_missing_value_defaults_to_current_system_date(self):
        self.assertEqual(
            self.namespace["resolve_last_verified_run"](),
            date.today().isoformat(),
        )

    def test_explicit_value_is_preserved(self):
        self.assertEqual(
            self.namespace["resolve_last_verified_run"]("2026-08-25"),
            "2026-08-25",
        )


class NotebookTimeoutTest(unittest.TestCase):
    def test_timeout_defaults_to_active_backend_walltime(self):
        config = {
            "cluster": {
                "type": "slurm",
                "local": {"walltime": "01:00:00"},
                "slurm": {"walltime": "12:00:00"},
            }
        }

        self.assertEqual(MODULE.resolve_notebook_timeout(config, None), 12 * 3600)

    def test_walltime_supports_days(self):
        self.assertEqual(MODULE.parse_walltime_seconds("2-03:04:05"), 183845)

    def test_walltime_supports_yaml_sexagesimal_integer(self):
        self.assertEqual(MODULE.parse_walltime_seconds(43200), 43200)

    def test_explicit_timeout_takes_precedence(self):
        config = {"cluster": {"type": "slurm", "slurm": {"walltime": "12:00:00"}}}

        self.assertEqual(MODULE.resolve_notebook_timeout(config, 7200), 7200)

    def test_timeout_falls_back_when_backend_has_no_walltime(self):
        config = {"cluster": {"type": "local", "local": {}}}

        self.assertEqual(MODULE.resolve_notebook_timeout(config, None), 3600)

    def test_invalid_walltime_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Invalid cluster walltime"):
            MODULE.parse_walltime_seconds("12:75:00")


class SlurmWalltimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
        source = next(
            cell.source
            for cell in notebook.cells
            if cell.get("id") == "imports-and-helpers"
        )
        tree = ast.parse(source)
        selected_nodes = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "normalize_slurm_walltime"
        ]
        cls.namespace = {}
        exec(
            compile(
                ast.Module(selected_nodes, type_ignores=[]), str(NOTEBOOK_PATH), "exec"
            ),
            cls.namespace,
        )

    def test_yaml_sexagesimal_integer_is_formatted_as_slurm_clock(self):
        self.assertEqual(self.namespace["normalize_slurm_walltime"](43200), "12:00:00")

    def test_explicit_slurm_clock_is_preserved(self):
        self.assertEqual(
            self.namespace["normalize_slurm_walltime"]("12:00:00"), "12:00:00"
        )

    def test_integer_walltime_supports_days(self):
        self.assertEqual(
            self.namespace["normalize_slurm_walltime"](183845), "2-03:04:05"
        )


class DatabaseCredentialsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
        source = next(
            cell.source
            for cell in notebook.cells
            if cell.get("id") == "catalog-qa-sections"
        )
        tree = ast.parse(source)
        function_names = {
            "parse_pgpass_entries",
            "find_pgpass_entry",
            "read_database_credentials",
        }
        selected_nodes = [
            node
            for node in tree.body
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(target, ast.Name)
                    and target.id in {"DATABASE_CREDENTIAL_DEFAULTS", "PGPASS_FIELDS"}
                    for target in node.targets
                )
            )
            or (isinstance(node, ast.FunctionDef) and node.name in function_names)
        ]
        cls.namespace = {"os": os, "re": re}
        credential_tree = ast.Module(selected_nodes, type_ignores=[])
        exec(
            compile(credential_tree, str(NOTEBOOK_PATH), "exec"),
            cls.namespace,
        )

    def read(self, contents, database_config=None, environment=None):
        database_config = dict(database_config or {})
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / ".pgpass"
            path.write_text(contents, encoding="utf-8")
            database_config["credentials_file"] = str(path)
            self.namespace["resolve_config_path"] = lambda value: Path(value)
            clean_environment = {
                key: value
                for key, value in os.environ.items()
                if key not in {"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"}
            }
            clean_environment.update(environment or {})
            with mock.patch.dict(os.environ, clean_environment, clear=True):
                return self.namespace["read_database_credentials"](database_config)

    def test_reads_single_active_pgpass_entry(self):
        credentials = self.read(
            "#db.example.org:6543:archive:qa_reader:ignored\n"
            "db.example.org:6543:science_catalog:qa_reader:test-password\n"
            "#db.example.org:6543:staging:qa_reader:ignored\n"
        )

        self.assertEqual(
            credentials,
            {
                "host": "db.example.org",
                "port": "6543",
                "dbname": "science_catalog",
                "user": "qa_reader",
                "password": "test-password",
                "connect_timeout": 15,
            },
        )

    def test_selects_compatible_pgpass_entry_and_preserves_yaml_precedence(self):
        credentials = self.read(
            "db.example:5432:first:reader:first-pass\n"
            "db.example:5432:second:reader:second-pass\n",
            {"host": "db.example", "dbname": "second", "connect_timeout": 30},
        )

        self.assertEqual(credentials["dbname"], "second")
        self.assertEqual(credentials["password"], "second-pass")
        self.assertEqual(credentials["connect_timeout"], 30)

    def test_environment_still_takes_precedence(self):
        credentials = self.read(
            "file-host:5432:file-db:file-user:file-pass\n",
            {
                "host": "yaml-host",
                "dbname": "yaml-db",
                "user": "yaml-user",
                "password": "yaml-pass",
            },
            {
                "PGHOST": "env-host",
                "PGDATABASE": "env-db",
                "PGUSER": "env-user",
                "PGPASSWORD": "env-pass",
            },
        )

        self.assertEqual(credentials["host"], "env-host")
        self.assertEqual(credentials["dbname"], "env-db")
        self.assertEqual(credentials["user"], "env-user")
        self.assertEqual(credentials["password"], "env-pass")

    def test_complete_yaml_configuration_does_not_require_credentials_file(self):
        database_config = {
            "host": "yaml-host",
            "port": "5432",
            "dbname": "yaml-db",
            "user": "yaml-user",
            "password": "yaml-pass",
            "credentials_file": "/does/not/exist",
        }
        clean_environment = {
            key: value
            for key, value in os.environ.items()
            if key not in {"PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"}
        }

        with mock.patch.dict(os.environ, clean_environment, clear=True):
            credentials = self.namespace["read_database_credentials"](database_config)

        self.assertEqual(credentials["host"], "yaml-host")
        self.assertEqual(credentials["password"], "yaml-pass")

    def test_existing_custom_credential_format_still_works(self):
        credentials = self.read(
            "user: legacy-user\n"
            "pass: legacy-pass\n"
            "  - long: legacy-host\n"
            "database name: legacy-db\n"
            "port: 5433\n"
        )

        self.assertEqual(credentials["host"], "legacy-host")
        self.assertEqual(credentials["port"], "5433")
        self.assertEqual(credentials["dbname"], "legacy-db")
        self.assertEqual(credentials["user"], "legacy-user")
        self.assertEqual(credentials["password"], "legacy-pass")

    def test_pgpass_supports_escaped_colons_and_backslashes(self):
        credentials = self.read(r"db.example:5432:catalog:user:pa\:ss\\word" + "\n")

        self.assertEqual(credentials["password"], r"pa:ss\word")


class ParquetIndexHandlingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
        source = next(
            cell.source
            for cell in notebook.cells
            if cell.get("id") == "catalog-qa-sections"
        )
        tree = ast.parse(source)
        function_names = {
            "get_parquet_read_options",
            "read_catalog_columns",
            "_qa_parquet_footer_batch_row_count",
            "qa_parquet_footer_row_count",
        }
        selected_nodes = [
            node
            for node in tree.body
            if (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(target, ast.Name)
                    and target.id == "PARQUET_READ_OPTION_NAMES"
                    for target in node.targets
                )
            )
            or (isinstance(node, ast.FunctionDef) and node.name in function_names)
        ]
        cls.namespace = {"dask": dask, "dd": dd, "delayed": delayed}
        exec(
            compile(
                ast.Module(selected_nodes, type_ignores=[]), str(NOTEBOOK_PATH), "exec"
            ),
            cls.namespace,
        )

    def test_mixed_named_and_range_indexes_are_read_as_columns(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            empty_path = root / "000-empty.parquet"
            populated_path = root / "001-populated.parquet"

            pd.DataFrame(
                {
                    "diaObjectId": pd.Series(dtype="int64"),
                    "ra": pd.Series(dtype="float64"),
                }
            ).to_parquet(empty_path, engine="pyarrow")
            pd.DataFrame(
                {"ra": [12.5]},
                index=pd.Index([42], name="diaObjectId"),
            ).to_parquet(populated_path, engine="pyarrow")

            result = self.namespace["read_catalog_columns"](
                {
                    "kind": "parquet",
                    "parquet_files": [empty_path, populated_path],
                },
                columns=["diaObjectId"],
            ).compute(scheduler="synchronous")

        self.assertEqual(result["diaObjectId"].tolist(), [42])
        self.assertIsNone(result.index.name)

    def test_catalog_read_uses_configured_file_aggregation(self):
        context = {
            "kind": "parquet",
            "parquet_files": [Path("part.parquet")],
            "parquet_read_options": {
                "aggregate_files": True,
                "split_row_groups": "adaptive",
                "blocksize": "256MiB",
            },
        }

        with mock.patch.object(dd, "read_parquet") as read_parquet:
            self.namespace["read_catalog_columns"](context, columns=["coord_ra"])

        read_parquet.assert_called_once_with(
            context["parquet_files"],
            engine="pyarrow",
            columns=["coord_ra"],
            index=False,
            **context["parquet_read_options"],
        )

    def test_unsupported_parquet_read_option_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unsupported parquet_read option"):
            self.namespace["get_parquet_read_options"](
                {"parquet_read": {"unexpected": True}}
            )

    def test_footer_row_count_does_not_scan_catalog_columns(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            empty_path = root / "empty.parquet"
            populated_path = root / "populated.parquet"
            pd.DataFrame({"value": pd.Series(dtype="float64")}).to_parquet(
                empty_path, engine="pyarrow"
            )
            pd.DataFrame({"value": [1.0, 2.0]}).to_parquet(
                populated_path, engine="pyarrow"
            )

            with dask.config.set(scheduler="synchronous"):
                result = self.namespace["qa_parquet_footer_row_count"](
                    [empty_path, populated_path], batch_size=1
                )

        self.assertEqual(result, 2)


class SpatialHistogramTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
        source = next(
            cell.source
            for cell in notebook.cells
            if cell.get("id") == "imports-and-helpers"
        )
        tree = ast.parse(source)
        function_names = {"_qa_partition_histogram2d_array", "qa_histogram2d"}
        selected_nodes = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name in function_names
        ]
        cls.namespace = {
            "da": da,
            "dask": dask,
            "delayed": delayed,
            "np": np,
            "pd": pd,
        }
        exec(
            compile(
                ast.Module(selected_nodes, type_ignores=[]),
                str(NOTEBOOK_PATH),
                "exec",
            ),
            cls.namespace,
        )

    def test_histogram_submits_bounded_partition_batches(self):
        pandas_frame = pd.DataFrame(
            {
                "ra": [0.0, 45.0, 90.0, 180.0, 270.0],
                "dec": [0.0, 10.0, -10.0, 20.0, -20.0],
            }
        )
        frame = dd.from_pandas(pandas_frame, npartitions=5)
        xedges = np.linspace(-np.pi, np.pi, 9)
        yedges = np.linspace(-np.pi / 2.0, np.pi / 2.0, 5)

        with dask.config.set(scheduler="synchronous"), mock.patch.object(
            dask, "compute", wraps=dask.compute
        ) as compute:
            result = self.namespace["qa_histogram2d"](
                frame,
                "ra",
                "dec",
                xedges,
                yedges,
                split_every=2,
                partition_batch_size=2,
            )

        expected = self.namespace["_qa_partition_histogram2d_array"](
            pandas_frame, "ra", "dec", xedges, yedges
        )
        np.testing.assert_array_equal(result, expected)
        self.assertEqual(compute.call_count, 3)

    def test_partition_batch_size_must_be_positive(self):
        frame = dd.from_pandas(pd.DataFrame({"ra": [], "dec": []}), npartitions=1)

        with self.assertRaisesRegex(ValueError, "partition_batch_size"):
            self.namespace["qa_histogram2d"](
                frame,
                "ra",
                "dec",
                np.linspace(-np.pi, np.pi, 3),
                np.linspace(-np.pi / 2.0, np.pi / 2.0, 3),
                partition_batch_size=0,
            )


class PhotometryOptimizationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
        source = next(
            cell.source
            for cell in notebook.cells
            if cell.get("id") == "imports-and-helpers"
        )
        tree = ast.parse(source)
        function_names = {
            "make_band_model_columns",
            "model_uses_flux_conversion",
            "model_uses_flux_error_conversion",
            "get_mag_offset",
            "_convert_magnitude_partition",
            "_convert_error_partition",
            "flux_error_model_to_flux_model",
            "make_photometry_source",
            "infer_magnitude_error_model",
            "get_magnitude_error_trend_models",
            "_qa_partition_binned_relation_array",
            "make_binned_relation_plan",
            "finalize_binned_relation_products",
            "summarize_binned_relation",
            "_qa_partition_distribution_products",
            "qa_histogram_quantiles",
            "make_distribution_products_plan",
            "finalize_distribution_products",
            "_qa_make_histogram_distribution_spec",
            "_qa_numeric_values",
            "_qa_magnitude_values",
            "_qa_magnitude_error_values",
            "_qa_distribution_products_from_values",
            "_qa_binned_relation_products_from_values",
            "_qa_partition_photometry_products",
            "make_memory_efficient_photometry_plan",
            "finalize_memory_efficient_photometry_plan",
            "should_use_memory_efficient_photometry",
            "compute_memory_efficient_photometry_results",
            "compute_photometry_distribution_results",
        }
        selected_nodes = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name in function_names
        ]
        cls.namespace = {
            "da": da,
            "dask": dask,
            "dd": dd,
            "delayed": delayed,
            "np": np,
            "pd": pd,
            "perf_counter": perf_counter,
            "qa_log": lambda message: None,
        }
        exec(
            compile(
                ast.Module(selected_nodes, type_ignores=[]), str(NOTEBOOK_PATH), "exec"
            ),
            cls.namespace,
        )

    @staticmethod
    def config():
        return {
            "title": "Synthetic photometry",
            "magnitudes": {
                "bands": ["g"],
                "models": ["psfFlux"],
                "mag_offset": 31.4,
                "histogram": {"bins": 5, "range": [25.0, 30.0]},
                "statistics": {
                    "range": [20.0, 40.0],
                    "peak_bin_width": 1.0,
                    "quantile_method": "histogram",
                    "quantiles": [0.5],
                },
            },
            "magnitude_errors": {
                "models": ["psfFluxErr"],
                "histogram": {"bins": 4, "range": [0.0, 0.2]},
                "statistics": {
                    "range": [0.0, 2.0],
                    "peak_bin_width": 0.1,
                    "quantile_method": "histogram",
                    "quantiles": [0.5],
                    "thresholds": [0.2],
                },
            },
            "magnitude_error_trends": {
                "bands": ["g"],
                "bins": 1,
                "magnitude_range": [25.0, 30.0],
                "error_range": [0.0, 0.3],
                "dispersion_quantiles": [0.16, 0.84],
            },
        }

    @staticmethod
    def raw_source():
        frame = pd.DataFrame(
            {
                "g_psfFlux": [100.0, 10.0, 1.0, 0.0, np.nan],
                "g_psfFluxErr": [10.0, 2.0, 0.1, 1.0, 1.0],
            }
        )
        return dd.from_pandas(frame, npartitions=2)

    def test_unified_source_reads_once_and_converts_both_products(self):
        reader = mock.Mock(return_value=self.raw_source())
        self.namespace["read_catalog_columns"] = reader
        config = self.config()

        source, metadata = self.namespace["make_photometry_source"](
            {"kind": "parquet"},
            magnitudes_config=config["magnitudes"],
            magnitude_errors_config=config["magnitude_errors"],
        )
        result = source.compute()

        reader.assert_called_once_with(
            {"kind": "parquet"},
            columns=["g_psfFlux", "g_psfFluxErr"],
        )
        self.assertEqual(metadata["magnitude_columns"], ["g_psfFlux"])
        self.assertEqual(metadata["error_columns"], ["g_psfFluxErr"])
        expected_magnitudes = 31.4 - 2.5 * np.log10([100.0, 10.0, 1.0])
        expected_errors = 2.5 / np.log(10.0) * np.array([0.1, 0.2, 0.1])
        np.testing.assert_allclose(
            result["g_psfFlux"].iloc[:3],
            expected_magnitudes,
        )
        np.testing.assert_allclose(
            result["g_psfFluxErr"].iloc[:3],
            expected_errors,
        )
        self.assertTrue(result["g_psfFlux"].iloc[3:].isna().all())
        self.assertTrue(result["g_psfFluxErr"].iloc[3:].isna().all())

    def test_precomputed_magnitudes_and_errors_are_used_directly(self):
        frame = dd.from_pandas(
            pd.DataFrame(
                {
                    "g_psfMag": [20.0, 21.0],
                    "g_psfMagErr": [0.1, 0.2],
                    "g_kronFlux": [100.0, 10.0],
                    "g_kronFluxErr": [10.0, 2.0],
                }
            ),
            npartitions=1,
        )
        reader = mock.Mock(return_value=frame)
        self.namespace["read_catalog_columns"] = reader
        magnitudes_config = {
            "bands": ["g"],
            "models": ["psfMag", "kronFlux"],
            "mag_offset": 31.4,
        }
        errors_config = {"models": ["psfMagErr", "kronFluxErr"]}

        source, _ = self.namespace["make_photometry_source"](
            {"kind": "parquet"},
            magnitudes_config=magnitudes_config,
            magnitude_errors_config=errors_config,
        )
        result = source.compute()

        reader.assert_called_once_with(
            {"kind": "parquet"},
            columns=["g_kronFlux", "g_kronFluxErr", "g_psfMag", "g_psfMagErr"],
        )
        np.testing.assert_allclose(result["g_psfMag"], [20.0, 21.0])
        np.testing.assert_allclose(result["g_psfMagErr"], [0.1, 0.2])
        np.testing.assert_allclose(
            result["g_kronFlux"],
            31.4 - 2.5 * np.log10([100.0, 10.0]),
        )
        np.testing.assert_allclose(
            result["g_kronFluxErr"],
            2.5 / np.log(10.0) * np.array([0.1, 0.2]),
        )

    def test_combined_products_are_lazy_and_preserve_scientific_selections(self):
        reader = mock.Mock(return_value=self.raw_source())
        self.namespace["read_catalog_columns"] = reader
        config = self.config()
        config["magnitudes"]["statistics"].pop("quantile_method")
        config["magnitude_errors"]["statistics"].pop("quantile_method")

        source, metadata = self.namespace["make_photometry_source"](
            {"kind": "parquet"},
            magnitudes_config=config["magnitudes"],
            magnitude_errors_config=config["magnitude_errors"],
        )
        plan = self.namespace["make_distribution_products_plan"](
            source,
            columns=metadata["magnitude_columns"],
            histogram_config=config["magnitudes"]["histogram"],
            statistics_config=config["magnitudes"]["statistics"],
        )
        self.assertTrue(dask.is_dask_collection(plan["total_products"]))
        self.assertTrue(
            all(
                dask.is_dask_collection(quantile)
                for quantile in plan["distributed_quantiles"]
            )
        )

        reader.reset_mock()
        results = self.namespace["compute_photometry_distribution_results"](
            config,
            {"kind": "parquet"},
        )
        reader.assert_called_once()

        magnitudes = 31.4 - 2.5 * np.log10([100.0, 10.0, 1.0])
        errors = 2.5 / np.log(10.0) * np.array([0.1, 0.2, 0.1])
        magnitude_result = results["magnitudes"]
        error_result = results["magnitude_errors"]
        np.testing.assert_array_equal(
            magnitude_result["histograms"]["g_psfFlux"],
            np.histogram(magnitudes, bins=np.linspace(25.0, 30.0, 6))[0],
        )
        np.testing.assert_array_equal(
            error_result["histograms"]["g_psfFluxErr"],
            np.histogram(errors, bins=np.linspace(0.0, 0.2, 5))[0],
        )
        magnitude_statistics = magnitude_result["statistics"].iloc[0]
        error_statistics = error_result["statistics"].iloc[0]
        self.assertEqual(magnitude_statistics["Count"], 3)
        self.assertEqual(magnitude_statistics["Non-finite"], 2)
        self.assertAlmostEqual(magnitude_statistics["Mean"], magnitudes.mean())
        self.assertAlmostEqual(magnitude_statistics["Peak-bin center"], 26.5)
        self.assertAlmostEqual(magnitude_statistics["Median"], np.median(magnitudes))
        self.assertEqual(error_statistics["Count"], 3)
        self.assertEqual(error_statistics["Non-finite"], 2)
        self.assertAlmostEqual(error_statistics["Fraction ≤ 0.2"], 2 / 3)

        trend_result = results["magnitude_error_trends"]
        column_pair = ("g_psfFlux", "g_psfFluxErr")
        counts, exact_mean, quantiles = self.namespace["summarize_binned_relation"](
            trend_result["histograms"][column_pair],
            trend_result["error_sums"][column_pair],
            trend_result["error_edges"],
        )
        self.assertEqual(counts[0], 2)
        self.assertAlmostEqual(exact_mean[0], errors[:2].mean())
        approximate_bin_center_mean = (
            trend_result["histograms"][column_pair]
            * (trend_result["error_edges"][:-1] + trend_result["error_edges"][1:])
            / 2
        ).sum() / counts[0]
        self.assertNotAlmostEqual(exact_mean[0], approximate_bin_center_mean)
        self.assertEqual(len(quantiles), 2)
        np.testing.assert_allclose(
            [quantile[0] for quantile in quantiles], [0.15, 0.15]
        )

    def test_histogram_path_uses_one_bounded_partition_reduction(self):
        reader = mock.Mock(return_value=self.raw_source())
        self.namespace["read_catalog_columns"] = reader
        config = self.config()
        legacy_magnitude_conversion = self.namespace["_convert_magnitude_partition"]
        legacy_error_conversion = self.namespace["_convert_error_partition"]
        self.namespace["_convert_magnitude_partition"] = mock.Mock(
            side_effect=AssertionError("legacy DataFrame conversion was used")
        )
        self.namespace["_convert_error_partition"] = mock.Mock(
            side_effect=AssertionError("legacy DataFrame conversion was used")
        )

        try:
            results = self.namespace["compute_photometry_distribution_results"](
                config,
                {"kind": "parquet"},
            )
        finally:
            self.namespace["_convert_magnitude_partition"] = legacy_magnitude_conversion
            self.namespace["_convert_error_partition"] = legacy_error_conversion

        reader.assert_called_once_with(
            {"kind": "parquet"},
            columns=["g_psfFlux", "g_psfFluxErr"],
        )
        magnitudes = 31.4 - 2.5 * np.log10([100.0, 10.0, 1.0])
        errors = 2.5 / np.log(10.0) * np.array([0.1, 0.2, 0.1])
        np.testing.assert_array_equal(
            results["magnitudes"]["histograms"]["g_psfFlux"],
            np.histogram(magnitudes, bins=np.linspace(25.0, 30.0, 6))[0],
        )
        np.testing.assert_array_equal(
            results["magnitude_errors"]["histograms"]["g_psfFluxErr"],
            np.histogram(errors, bins=np.linspace(0.0, 0.2, 5))[0],
        )
        trend = results["magnitude_error_trends"]
        pair = ("g_psfFlux", "g_psfFluxErr")
        counts, exact_mean, _ = self.namespace["summarize_binned_relation"](
            trend["histograms"][pair],
            trend["error_sums"][pair],
            trend["error_edges"],
        )
        self.assertEqual(counts[0], 2)
        self.assertAlmostEqual(exact_mean[0], errors[:2].mean())

    def test_trend_reduction_includes_upper_edges_and_keeps_rows_paired(self):
        partition = pd.DataFrame(
            {
                "magnitude": [0.0, 1.0, 2.0, np.nan, 1.5],
                "error": [0.0, 0.2, 0.3, 0.1, np.nan],
            }
        )
        x_edges = np.array([0.0, 1.0, 2.0])
        y_edges = np.array([0.0, 0.15, 0.3])
        products = self.namespace["_qa_partition_binned_relation_array"](
            partition,
            [("magnitude", "error")],
            x_edges,
            y_edges,
        )
        expected_histogram = np.histogram2d(
            [0.0, 1.0, 2.0],
            [0.0, 0.2, 0.3],
            bins=[x_edges, y_edges],
        )[0]

        np.testing.assert_array_equal(products[0, :, :2], expected_histogram)
        np.testing.assert_allclose(products[0, :, 2], [0.0, 0.5])

    def test_histogram_quantile_interpolates_within_the_selected_bin(self):
        result = self.namespace["qa_histogram_quantiles"](
            np.array([[0, 1, 2, 1]]),
            np.array([0.0, 1.0, 2.0, 3.0, 4.0]),
            [0.5],
        )

        self.assertAlmostEqual(result[0, 0], 2.5)


class GroupedBasicStatisticsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
        source = next(
            cell.source
            for cell in notebook.cells
            if cell.get("id") == "catalog-qa-sections"
        )
        tree = ast.parse(source)
        function_names = {
            "get_basic_statistics_group_config",
            "get_basic_statistics_group_files",
            "promote_low_precision_float_columns",
            "compute_grouped_basic_statistics",
        }
        selected_nodes = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name in function_names
        ]
        cls.namespace = {"dask": dask, "dd": dd, "np": np, "pd": pd}
        exec(
            compile(
                ast.Module(selected_nodes, type_ignores=[]), str(NOTEBOOK_PATH), "exec"
            ),
            cls.namespace,
        )

    def test_column_groups_share_one_catalog_read_and_keep_bands_separate(self):
        frame = dd.from_pandas(
            pd.DataFrame(
                {
                    "band": ["u", "u", "g", "g"],
                    "flux": [1.0, 3.0, 10.0, 14.0],
                    "fluxErr": [0.1, 0.3, 1.0, 1.4],
                }
            ),
            npartitions=2,
        )
        reader = mock.Mock(return_value=frame)
        self.namespace["read_catalog_columns"] = reader
        group_config = self.namespace["get_basic_statistics_group_config"](
            frame.columns,
            {
                "group_by": {
                    "column": "band",
                    "values": ["u", "g"],
                    "label": "Band",
                }
            },
        )

        result = self.namespace["compute_grouped_basic_statistics"](
            {"kind": "parquet"}, ["flux", "fluxErr"], group_config
        )

        reader.assert_called_once_with(
            {"kind": "parquet"}, columns=["flux", "fluxErr", "band"]
        )
        self.assertEqual([value for value, _ in result], ["u", "g"])
        self.assertEqual(result[0][1].loc["mean", "flux"], 2.0)
        self.assertEqual(result[1][1].loc["mean", "flux"], 12.0)
        self.assertEqual(
            list(result[0][1].index), ["count", "mean", "std", "min", "max"]
        )

    def test_path_template_selects_only_files_below_each_band_directory(self):
        context = {
            "path": Path("/catalog/source"),
            "parquet_files": [
                Path("/catalog/source/u/part-1.parq"),
                Path("/catalog/source/u/nested/part-2.parq"),
                Path("/catalog/source/g/part-1.parq"),
            ],
        }

        files = self.namespace["get_basic_statistics_group_files"](
            context, "u", "{value}"
        )

        self.assertEqual(files, context["parquet_files"][:2])

    def test_hats_grouping_converts_only_the_projected_catalog(self):
        frame = dd.from_pandas(
            pd.DataFrame(
                {
                    "band": ["u", "u", "g"],
                    "flux": [1.0, 3.0, 10.0],
                    "fluxErr": [0.1, 0.3, 1.0],
                }
            ),
            npartitions=2,
        )
        projected_catalog = mock.Mock()
        projected_catalog.to_dask_dataframe.return_value = frame
        reader = mock.Mock(return_value=projected_catalog)
        self.namespace["read_catalog_columns"] = reader
        group_config = {
            "column": "band",
            "values": ["u", "g"],
            "label": "Band",
            "path_template": None,
            "split_every": 8,
        }

        result = self.namespace["compute_grouped_basic_statistics"](
            {"kind": "hats"}, ["flux", "fluxErr"], group_config
        )

        reader.assert_called_once_with(
            {"kind": "hats"}, columns=["flux", "fluxErr", "band"]
        )
        projected_catalog.to_dask_dataframe.assert_called_once_with()
        self.assertEqual(result[0][1].loc["mean", "flux"], 2.0)
        self.assertEqual(result[1][1].loc["mean", "flux"], 10.0)

    def test_arrow_float32_is_promoted_before_large_group_reductions(self):
        frame = dd.from_pandas(
            pd.DataFrame(
                {
                    "band": ["u", "g"],
                    "flux": pd.Series([1.0, 2.0], dtype="float32[pyarrow]"),
                    "identifier": pd.Series([1, 2], dtype="int64[pyarrow]"),
                }
            ),
            npartitions=1,
        )

        promoted = self.namespace["promote_low_precision_float_columns"](
            frame, ["flux", "identifier"]
        )

        self.assertEqual(promoted.dtypes["flux"], np.dtype("float64"))
        self.assertEqual(str(promoted.dtypes["identifier"]), "int64[pyarrow]")

    def test_path_template_cannot_escape_catalog_root(self):
        context = {
            "path": Path("/catalog/source"),
            "parquet_files": [Path("/catalog/u/part-1.parq")],
        }

        with self.assertRaisesRegex(ValueError, "escapes the catalog root"):
            self.namespace["get_basic_statistics_group_files"](
                context, "u", "../{value}"
            )

    def test_group_configuration_requires_existing_column_and_unique_values(self):
        get_config = self.namespace["get_basic_statistics_group_config"]

        with self.assertRaisesRegex(ValueError, "not present"):
            get_config(
                ["flux"],
                {"group_by": {"column": "band", "values": ["u"]}},
            )
        with self.assertRaisesRegex(ValueError, "must not contain duplicates"):
            get_config(
                ["band", "flux"],
                {"group_by": {"column": "band", "values": ["u", "u"]}},
            )


if __name__ == "__main__":
    unittest.main()
