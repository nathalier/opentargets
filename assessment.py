#!/usr/bin/env python3

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
        # 1. Parse each evidence object and the `diseaseId`, `targetId`, and `score` fields.
        evidence_path = data_path / "input" / fmt / EVIDENCE_DIR / evidence_subdir
        targets_path = data_path / "input" / fmt / TARGETS_DIR
        diseases_path = data_path / "input" / fmt / DISEASES_DIR

        evidence_df, targets_df, diseases_df = read_parquet_data(
            evidence_path, targets_path, diseases_path)

        evidence_df = evidence_df[['targetId', 'diseaseId', 'score']]

        # 2. For each `targetId` and `diseaseId` pair, calculate the median and 3 greatest `score`
        evidence_median_df = target_disease_aggr_median(evidence_df)
        evidence_top_scores_df = target_disease_aggr_top_scores(evidence_df)

        evidence_aggregated_df = evidence_median_df \
            .merge(evidence_top_scores_df, how="left", on=['targetId', 'diseaseId'])

        # 3. Join the targets and diseases datasets on the `targetId` = `target.id`
        # and `diseaseId` = `disease.id` fields.
        # 4. Add the `target.approvedSymbol` and `disease.name` fields to your table
        targets_df = targets_df[["id", "approvedSymbol"]]
        targets_df = targets_df.rename(columns={"id": "targetId"}).compute()

        diseases_df = diseases_df[["id", "name"]]
        diseases_df = diseases_df.rename(
            columns={"id": "diseaseId", "name": "diseaseName"}).compute()

        joined_df = diseases_df.merge(evidence_aggregated_df, how="right",
                                      on='diseaseId', left_index=False)
        joined_df = targets_df.merge(joined_df, how="right",
                                     on='targetId', left_index=False)

        # 5. Output the resulting table in JSON format,
        # sorted in ascending order by the median value of the `score`.
        sorted_df = joined_df.sort_values(by="score_median", ascending=False)\
                             .reset_index(drop=True)

        output_path = data_path / "output"
        output_path.mkdir(parents=True, exist_ok=True)
        sorted_df.to_json(output_path / "result.json", orient="records")


def read_parquet_data(evidence_path, targets_path, diseases_path):
    try:
        evidence_df = dd.read_parquet(evidence_path, engine='pyarrow')
        targets_df = dd.read_parquet(targets_path, engine='pyarrow')
        diseases_df = dd.read_parquet(diseases_path, engine='pyarrow')
    except (IndexError, AttributeError) as e:
        raise IOError(e, "No parquet data found in one of directories/files")

    return evidence_df, targets_df, diseases_df


def target_disease_aggr_median(evidence_df):
    res_df = evidence_df.groupby(['targetId', 'diseaseId'])['score']\
                .apply(pd.Series.median, meta=('score_median', 'f8'))\
                .to_frame()\
                .reset_index()\
                .compute()
    return res_df


def target_disease_aggr_top_scores(evidence_df):
    res_df = evidence_df.groupby(['targetId', 'diseaseId'])['score'] \
                .apply(lambda x: x.nlargest(3).to_list(), meta=("top_scores", "object")) \
                .to_frame() \
                .reset_index() \
                .compute()
    return res_df


if __name__ == "__main__":
    release = '21.11'
    fmt = 'parquet'
    evidence_dataset = 'sourceId=eva'

    fetch_data(release, fmt, DISEASES_DIR)
    fetch_data(release, fmt, TARGETS_DIR)
    fetch_data(release, fmt, f'{EVIDENCE_DIR}/{evidence_dataset}')

    run_pipeline(local_data_base_path / release, fmt, evidence_dataset)
