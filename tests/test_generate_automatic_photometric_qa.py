import importlib.util
from pathlib import Path
import tempfile
import unittest

import yaml


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "generate_automatic_photometric_qa.py"
)
SPEC = importlib.util.spec_from_file_location("generate_automatic_photometric_qa", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


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


if __name__ == "__main__":
    unittest.main()
