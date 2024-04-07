import pandas as pd

PATH_BIGDATA = 'Toys_and_Games_5.json'
raw_pdf = pd.read_json(PATH_BIGDATA, orient='records', lines=True)
print(raw_pdf.head())
print('Code Executed Successfully')

COL_NAME_MAP = {
    "overall": "overall",
    "verified": "verified",
    "reviewTime": "review_time",
    "reviewerID": "reviewer_id",
    "asin": "asin",
    "reviewerName": "reviewer_name",
    "reviewText": "review_text",
    "summary": "summary",
    "unixReviewTime": "unix_review_time",
    "style": "style",
    "vote": "vote",
    "image": "image"
}
print('Initial Columns names:')
i = 1
for col_name in raw_pdf.columns:
    print(f'{i}: {col_name}')
    i = i + 1

## renaming column names
raw_pdf = raw_pdf.rename(columns=COL_NAME_MAP)

print('___________________________')
print('Columns names after rename:')
i = 1
for col_name in raw_pdf.columns:
    print(f'{i}: {col_name}')
    i = i + 1

print('___________________________')
print('Code Executed Successfully')

SELECTED_COLUMNS = [
    "reviewer_id",
    "asin",
    "review_text",
    "summary",
    "verified",
    "overall",
    "vote",
    "unix_review_time",
    "review_time",
]

raw_pdf = raw_pdf[SELECTED_COLUMNS]
print(raw_pdf.head())
print('Code Executed Successfully')

# def create_path_snapshot():
#     path_fixed = 'data/snapshot/pandas/data_{}.json'
#     current_unix_time = int(time.time())
#     return path_fixed.format(current_unix_time)
#
#
# PATH_SNAPSHOT = create_path_snapshot()
# raw_pdf.to_json(PATH_SNAPSHOT)
# print('Snapshot Saved')
# print('Code Executed Successfully')
