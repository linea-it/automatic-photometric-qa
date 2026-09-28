import ast
import importlib.util
import os
from pathlib import Path
import re
import tempfile
import unittest
from unittest import mock

import nbformat
import yaml


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "generate_automatic_photometric_qa.py"
)
SPEC = importlib.util.spec_from_file_location("generate_automatic_photometric_qa", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
NOTEBOOK_PATH = Path(__file__).resolve().parents[1] / "notebooks" / "automatic_photometric_qa.ipynb"


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
            "catalogs": [
                {"title": "object", "schema": "lsst_dp1", "table": "object"}
            ],
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
            credentials = self.namespace["read_database_credentials"](
                database_config
            )

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
        credentials = self.read(
            r"db.example:5432:catalog:user:pa\:ss\\word" + "\n"
        )

        self.assertEqual(credentials["password"], r"pa:ss\word")


if __name__ == "__main__":
    unittest.main()
