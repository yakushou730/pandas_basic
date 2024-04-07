# pathconfig.py
import time
from pathlib import Path

import yaml

ROOT = Path(__file__).parent
DATA_LAKE = ROOT.joinpath("data")
RAW_DATA = DATA_LAKE.joinpath("raw")
SNAPSHOT = DATA_LAKE.joinpath("snapshot")
PATH_TOY_GAME_DATA = "/Toys_and_Games_5.json"


def create_path_snapshot() -> Path:
    """
    Create path to shapshot wit time stamp
    :return: Path like object
    """
    path_fixed = 'snapshot_{}'.format(int(time.time()))
    return SNAPSHOT.joinpath(path_fixed)


# TODO: Store The Snapshot
def create_path_snapshot_spark():
    path_fixed = 'data/snapshot/pyspark/snapshot_{}'
    current_unix_time = int(time.time())
    return path_fixed.format(current_unix_time)


def load_latest_parquet_path() -> Path:
    with open(SNAPSHOT.joinpath('pyspark', 'versions.yaml')) as f:
        content = yaml.safe_load(f)
        print(content)
        latest = content['latest']
        path = content[latest]['path']
        return SNAPSHOT.joinpath(path)
