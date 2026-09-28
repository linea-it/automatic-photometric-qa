#!/usr/bin/env python
"""Execute the automatic photometric QA notebook and export it as HTML."""

from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path

import nbformat
import yaml
from nbclient import NotebookClient
from nbconvert import HTMLExporter


def log_step(message: str) -> None:
    """Write a progress message to the terminal immediately."""

    print(message, flush=True)


class ProgressNotebookClient(NotebookClient):
    """Notebook client that prints configured QA step names before execution."""

    progress_prefix = "QA_PROGRESS:"

    async def async_execute_cell(self, cell, cell_index, execution_count=None, store_history=True):
        step = cell.get("metadata", {}).get("qa_step")

        if step:
            log_step(f"[{cell_index + 1:02d}/{len(self.nb.cells):02d}] {step}")

        return await super().async_execute_cell(
            cell,
            cell_index,
            execution_count=execution_count,
            store_history=store_history,
        )

    @staticmethod
    def is_expected_numeric_warning_line(line: str) -> bool:
        """Return True for known non-fatal quantile warnings from invalid values."""

        stripped = line.strip()

        return (
            "RuntimeWarning: invalid value encountered in subtract" in stripped
            or stripped in {"d = A - u", "diff_b_a = b - a"}
        )

    def process_message(self, msg, cell, cell_index):
        """Mirror notebook progress lines and hide known non-fatal warning noise."""

        if msg.get("msg_type") == "stream":
            content = msg.get("content", {})
            text = content.get("text", "")
            kept_lines = []

            for line in text.splitlines(keepends=True):
                stripped = line.strip()

                if stripped.startswith(self.progress_prefix):
                    log_step(stripped.removeprefix(self.progress_prefix).strip())
                elif self.is_expected_numeric_warning_line(line):
                    continue
                else:
                    kept_lines.append(line)

            if not kept_lines:
                return None

            content["text"] = "".join(kept_lines)

        return super().process_message(msg, cell, cell_index)


def check_runtime_dependencies() -> None:
    """Fail early when the active environment cannot start a notebook kernel."""

    missing_packages = [
        package
        for package in ("ipykernel",)
        if importlib.util.find_spec(package) is None
    ]

    if missing_packages:
        packages = " ".join(missing_packages)
        raise RuntimeError(
            "Missing package(s) in the active Python environment: "
            f"{packages}. Install them with: conda install -c conda-forge {packages}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Execute automatic_photometric_qa.ipynb from a YAML configuration and export HTML."
    )
    parser.add_argument(
        "config",
        type=Path,
        help="Path to the automatic photometric QA YAML configuration.",
    )
    parser.add_argument(
        "-n",
        "--notebook",
        type=Path,
        help="Notebook template to execute. Default: notebooks/automatic_photometric_qa.ipynb.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Output HTML path. Default: <config-stem>.html.",
    )
    parser.add_argument(
        "--hide-code",
        action="store_true",
        help="Export HTML without code cell inputs.",
    )
    parser.add_argument(
        "--executed-notebook",
        type=Path,
        help="Optional path for saving the executed notebook.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="Per-cell execution timeout in seconds. Default: 3600.",
    )
    parser.add_argument(
        "--kernel-name",
        default="python3",
        help="Jupyter kernel name. Default: python3.",
    )
    return parser.parse_args()


def load_config(config_path: Path) -> dict:
    with config_path.open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    if not isinstance(config, dict):
        raise ValueError("The automatic photometric QA configuration must be a YAML mapping.")

    from_database = config.get("from_database", False)

    if not isinstance(from_database, bool):
        raise ValueError("from_database must be true or false.")

    required_sections = ["notebook"]
    required_sections.append("database" if from_database else "cluster")

    for required_section in required_sections:
        if required_section not in config:
            raise ValueError(f"Missing required configuration section: {required_section}")

        if not isinstance(config[required_section], dict):
            raise ValueError(
                f"Configuration section '{required_section}' must be a YAML mapping."
            )

    if "catalogs" not in config and "catalog" not in config:
        raise ValueError("Missing required configuration section: catalogs")

    if "catalogs" in config:
        catalogs = config["catalogs"]

        if not isinstance(catalogs, list) or not catalogs:
            raise ValueError("catalogs must be a non-empty list.")
    else:
        catalogs = [config["catalog"]]

    for catalog_index, catalog_config in enumerate(catalogs, start=1):
        if not isinstance(catalog_config, dict):
            raise ValueError(f"catalogs[{catalog_index}] must be a YAML mapping.")

        status = catalog_config.get("status", "available")

        if status not in {"available", "planned"}:
            raise ValueError(
                f"catalogs[{catalog_index}].status must be either 'available' "
                f"or 'planned', got: {status!r}"
            )

        omit_paths = catalog_config.get("omit_paths", False)

        if not isinstance(omit_paths, bool):
            raise ValueError(
                f"catalogs[{catalog_index}].omit_paths must be true or false, "
                f"got: {omit_paths!r}"
            )

        if status == "available":
            if from_database:
                missing_keys = [
                    key for key in ("schema", "table") if key not in catalog_config
                ]
                if missing_keys:
                    raise ValueError(
                        f"catalogs[{catalog_index}] is a database catalog and is "
                        "missing required key(s): " + ", ".join(missing_keys)
                    )
            elif "path" not in catalog_config:
                raise ValueError(
                    f"catalogs[{catalog_index}] is available and is missing "
                    "required key: path. Use status: planned for placeholder "
                    "catalog sections without data files."
                )

    return config



def execute_notebook(
    nb: nbformat.NotebookNode,
    notebook_path: Path,
    config_path: Path,
    timeout: int,
    kernel_name: str,
) -> nbformat.NotebookNode:
    previous_config = os.environ.get("AUTOMATIC_PHOTOMETRIC_QA_CONFIG")
    os.environ["AUTOMATIC_PHOTOMETRIC_QA_CONFIG"] = str(config_path)

    try:
        client = ProgressNotebookClient(
            nb,
            timeout=timeout,
            kernel_name=kernel_name,
            resources={"metadata": {"path": str(notebook_path.parent.resolve())}},
            extra_arguments=["--IPKernelApp.log_level=ERROR"],
        )
        client.execute()
    finally:
        if previous_config is None:
            os.environ.pop("AUTOMATIC_PHOTOMETRIC_QA_CONFIG", None)
        else:
            os.environ["AUTOMATIC_PHOTOMETRIC_QA_CONFIG"] = previous_config

    return nb


def export_html(nb: nbformat.NotebookNode, output_path: Path, hide_code: bool) -> None:
    exporter = HTMLExporter()
    exporter.exclude_input = hide_code
    exporter.exclude_input_prompt = hide_code
    exporter.exclude_output_prompt = hide_code

    body, _ = exporter.from_notebook_node(nb)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(body, encoding="utf-8")


def main() -> None:
    args = parse_args()
    check_runtime_dependencies()

    project_root = Path(__file__).resolve().parents[1]
    config_path = args.config.resolve()
    notebook_path = (
        args.notebook.resolve()
        if args.notebook
        else project_root / "notebooks" / "automatic_photometric_qa.ipynb"
    )
    output_path = args.output or Path(f"{config_path.stem}.html")
    output_path = output_path.resolve()

    log_step(f"Loading configuration: {config_path}")
    load_config(config_path)
    log_step(f"Loading notebook template: {notebook_path}")
    nb = nbformat.read(notebook_path, as_version=4)
    log_step("Executing notebook")
    execute_notebook(
        nb,
        notebook_path=notebook_path,
        config_path=config_path,
        timeout=args.timeout,
        kernel_name=args.kernel_name,
    )

    log_step("Notebook execution finished")

    if args.executed_notebook:
        executed_notebook_path = args.executed_notebook.resolve()
        executed_notebook_path.parent.mkdir(parents=True, exist_ok=True)
        nbformat.write(nb, executed_notebook_path)
        log_step(f"Wrote executed notebook: {executed_notebook_path}")

    log_step(f"Exporting HTML: {output_path}")
    export_html(nb, output_path, hide_code=args.hide_code)
    log_step(f"Wrote HTML report: {output_path}")


if __name__ == "__main__":
    main()
