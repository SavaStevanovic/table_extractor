import pandas as pd

from extractors.dataframe import spliter
from extractors.extractor import Processor

test = pd.read_excel("data/Table#1.xlsx", sheet_name=None, header=None)

out = Processor(
    [
        spliter.RowSpaceSpliter(2),
        spliter.PaddingRemover(),
        spliter.AutoHeader(),
        spliter.Footer(["TOTAL"]),
    ]
)(list(test.values()))
for i, o in enumerate(out):
    o.to_csv(f"{i}.csv")
print(out)
