import pandas as pd

from config import set_pd_display
from pathconfig import PATH_TOY_GAME_DATA
from utils.pandas_utils import read_json_to_pdf, select_columns
from utils.tableschema import COL_NAME_MAP, SELECTED_COLUMNS

# Pandas Display Setup
assert set_pd_display(pd)
# Read the Data
raw_pdf = read_json_to_pdf(PATH_TOY_GAME_DATA, COL_NAME_MAP)
raw_pdf = select_columns(raw_pdf, SELECTED_COLUMNS)
print(raw_pdf.head())
