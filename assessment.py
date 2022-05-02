import dask
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from pathlib import Path
import subprocess


TARGETS_DIR = 'targets'
DISEASES_DIR = 'diseases'
EVIDENCE_DIR = 'evidence'
local_data_base_path = Path('./data')
local_data_base_path.mkdir(exist_ok=True)


def fetch_data(release, fmt, dir):
    # TODO parameters validation
    if not dir.endswith('/'):
        dir += '/'

    fetch_url = f'rsync://ftp.ebi.ac.uk/pub/databases/opentargets/platform/' \
                f'{release}/output/etl/{fmt}/{dir}'

    local_path = local_data_base_path / release / 'input' / fmt / dir
    local_path.mkdir(parents=True, exist_ok=True)

    # TODO keep status of successfully fetched data
    p = subprocess.run(["rsync", "-rtv", f"{fetch_url}", f"{local_path.resolve()}"])
    
    if p.returncode != 0:
        raise IOError(f"Could not sync '{dir}''{fmt}' data in '{release}' release")


def run_pipeline(data_path, fmt, evidence_subdir=""):
    if fmt != "parquet":
        raise NotImplementedError("Only 'parquet' input data format is currently supported")

    with Client() as dask_client:
        evidence_path = data_path / fmt / EVIDENCE_DIR / evidence_subdir
        targets_path = data_path / fmt / TARGETS_DIR
        diseases_path = data_path / fmt / DISEASES_DIR

        evidence_df, targets_df, diseases_df = read_parquet_data(
            evidence_path, targets_path, diseases_path)


def read_parquet_data(evidence_path, targets_path, diseases_path):
    try:
        evidence_df = dd.read_parquet(evidence_path)
        targets_df = dd.read_parquet(targets_path)
        diseases_df = dd.read_parquet(diseases_path)
    except (IndexError, AttributeError) as e:
        raise IOError(e, "No parquet data found in one of directories/files")

    return evidence_df, targets_df, diseases_df


if __name__ == "__main__":
    release = '21.11'
    fmt = 'parquet'
    evidence_dataset = 'sourceId=eva'

    fetch_data(release, fmt, DISEASES_DIR)
    fetch_data(release, fmt, TARGETS_DIR)
    fetch_data(release, fmt, f'{EVIDENCE_DIR}/{evidence_dataset}')

    run_pipeline(local_data_base_path / release / 'input', fmt, evidence_dataset)
