import pandas as pd

from extractors.dataframe import spliter
from extractors.dataframe.preheader import DownRight
from extractors.extractor import Merger, Processor

test = pd.read_excel("data/Table#1.xlsx", sheet_name=None, header=None)

out = Processor(
    [
        spliter.RowSpaceSpliter(2),
        spliter.PaddingRemover(),
        spliter.AutoHeader(DownRight("\d{4}[-/]\d{2}")),
        spliter.ColumnClean(),
        spliter.Footer(["TOTAL"]),
        spliter.IndexSet(["NAME", "CITY"]),
        spliter.HeaderDataExtractor(
            "SEASON",
            {
                "2020 21": 2020,
                "2021 22": 2021,
                "2022 23": 2022,
                "2023 24": 2023,
            },
        ),
        spliter.ColumnClean(),
    ]
)(list(test.values()))

for i, o in enumerate(out):
    o.to_csv(f"{i}.csv")
total_out = Merger(["NAME", "CITY"], ["SEASON"])(out)
total_out.to_csv("total.csv")
