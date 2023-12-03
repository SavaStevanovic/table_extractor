import pandas as pd

from frame_extractors.dataframe import spliter
from frame_extractors.dataframe.preheader import DownRight
from frame_extractors.extractor import Merger, Processor

tables = ["data/Table#1.xlsx"]
test = sum(
    [list(pd.read_excel(t, sheet_name=None, header=None).values()) for t in tables], []
)

out = Processor(
    [
        spliter.RowSpaceSpliter(2),
        spliter.PaddingRemover(),
        spliter.AutoHeader(DownRight("\d{4}[-/]\d{2}", ["Zip", "Sq. Ft."])),
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
)(test)

tables = ["data/Table#2.xlsx"]
test = sum(
    [list(pd.read_excel(t, sheet_name=None, header=None).values()) for t in tables], []
)

out += Processor(
    [
        spliter.PaddingRemover(),
        spliter.AutoHeader(DownRight("\d{4}[-/]\d{2,4}", [])),
        spliter.ColumnClean(),
        spliter.ColumnRename(
            {"WAREHOUSE": "NAME", "LOCATION NAME IDENTIFIER": "NAME", "ADDRESS": "CITY"}
        ),
        spliter.IndexSet(["NAME", "CITY"]),
        spliter.HeaderDataExtractor(
            "SEASON",
            {
                "2020 21": 2020,
                "2021 22": 2021,
                "2022 23": 2022,
                "2023 24": 2023,
                "2020 2021": 2020,
                "2021 2022": 2021,
                "2022 2023": 2022,
                "2023 2024": 2023,
            },
        ),
        spliter.DropNanIndex(),
        spliter.ColumnClean(),
    ]
)(test)

for i, o in enumerate(out):
    o.to_csv(f"{i}.csv")
