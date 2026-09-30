#!/bin/bash
#SBATCH --job-name=rubin-dp2-qa-parquet
#SBATCH --partition=cpu_pipelines
#SBATCH --account=hpc-pipelines
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=32G
#SBATCH --time=12:00:00
#SBATCH --output=rubin-dp2-qa-parquet-%j.out
#SBATCH --error=rubin-dp2-qa-parquet-%j.err

set -euo pipefail

SCRIPT_DIRECTORY="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPOSITORY_DIRECTORY="$(cd -- "${SCRIPT_DIRECTORY}/../.." && pwd)"
PYTHON_EXECUTABLE="${QA_PYTHON:-/scripts/luigi.silva/ondemand/automatic-photometric-qa/bin/python}"
DRIVER_CPUS="${SLURM_CPUS_PER_TASK:-2}"

if [[ ! -x "${PYTHON_EXECUTABLE}" ]]; then
    echo "Python executable is unavailable: ${PYTHON_EXECUTABLE}" >&2
    echo "Set QA_PYTHON to the desired environment's Python executable." >&2
    exit 1
fi

cd "${REPOSITORY_DIRECTORY}"
mkdir -p outputs

# Prevent numerical libraries from oversubscribing the driver allocation.
export OMP_NUM_THREADS="${DRIVER_CPUS}"
export OPENBLAS_NUM_THREADS="${DRIVER_CPUS}"
export MKL_NUM_THREADS="${DRIVER_CPUS}"
export NUMEXPR_NUM_THREADS="${DRIVER_CPUS}"
export MPLBACKEND=Agg
export PYTHONUNBUFFERED=1

echo "Starting Rubin DP2 Parquet QA"
echo "Repository: ${REPOSITORY_DIRECTORY}"
echo "Python: ${PYTHON_EXECUTABLE}"
echo "Driver resources: ${DRIVER_CPUS} CPUs, ${SLURM_MEM_PER_NODE:-unknown} MiB"

"${PYTHON_EXECUTABLE}" scripts/generate_automatic_photometric_qa.py \
    configs/production/rubin_dp2_QA_parquet.yaml \
    --output outputs/rubin_dp2_QA_parquet.html \
    --executed-notebook outputs/rubin_dp2_QA_parquet.ipynb \
    --hide-code
