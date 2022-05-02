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


if __name__ == "__main__":
    release = '21.11'
    fmt = 'parquet'
    evidence_dataset = 'sourceId=eva'

    fetch_data(release, fmt, DISEASES_DIR)
    fetch_data(release, fmt, TARGETS_DIR)
    fetch_data(release, fmt, f'{EVIDENCE_DIR}/{evidence_dataset}')
