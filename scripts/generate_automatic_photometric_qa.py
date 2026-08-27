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


OPTIONAL_SECTIONS = {
    "basic_statistics",
    "unique_count",
    "spatial_distribution",
    "magnitudes",
    "magnitude_errors",
}

PLOT_SECTIONS = {
    "spatial_distribution",
    "magnitudes",
    "magnitude_errors",
}


def log_step(message: str) -> None:
    """Write a progress message to the terminal immediately."""

    print(message, flush=True)


class ProgressNotebookClient(NotebookClient):
    """Notebook client that prints configured QA step names before execution."""

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

    for required_section in ("notebook", "catalog", "cluster"):
        if required_section not in config:
            raise ValueError(f"Missing required configuration section: {required_section}")

    return config


def configured_optional_sections(config: dict) -> set[str]:
    return {
        section
        for section in OPTIONAL_SECTIONS
        if section in config and config[section] is not None
    }


def filter_notebook(nb: nbformat.NotebookNode, enabled_sections: set[str]) -> None:
    keep_plots_heading = bool(enabled_sections & PLOT_SECTIONS)
    filtered_cells = []

    for cell in nb.cells:
        if cell.get("id") == "plots-heading" and not keep_plots_heading:
            continue

        optional_section = cell.get("metadata", {}).get("qa_optional_section")

        if optional_section and optional_section not in enabled_sections:
            continue

        filtered_cells.append(cell)

    nb.cells = filtered_cells


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
    config = load_config(config_path)
    enabled_sections = configured_optional_sections(config)

    log_step(f"Loading notebook template: {notebook_path}")
    nb = nbformat.read(notebook_path, as_version=4)
    filter_notebook(nb, enabled_sections)
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
