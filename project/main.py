import pandas as pd

from frame_extractors.dataframe import extractor
from frame_extractors.dataframe.preheader import DownRight
from frame_extractors.extractor import Merger, Processor

tables = ["data/Table#1.xlsx"]
test = sum(
    [list(pd.read_excel(t, sheet_name=None, header=None).values()) for t in tables], []
)

out = Processor(
    [
        extractor.RowSpaceSpliter(2),
        extractor.PaddingRemover(),
        extractor.AutoHeader(DownRight("\d{4}[-/]\d{2}", ["Zip", "Sq. Ft."])),
        extractor.ColumnClean(),
        extractor.Footer(["TOTAL"]),
        extractor.IndexSet(["NAME", "CITY"]),
        extractor.HeaderDataExtractor(
            "SEASON",
            {
                "2020 21": 2020,
                "2021 22": 2021,
                "2022 23": 2022,
                "2023 24": 2023,
            },
        ),
        extractor.ColumnClean(),
    ]
)(test)

tables = ["data/Table#2.xlsx"]
test = sum(
    [list(pd.read_excel(t, sheet_name=None, header=None).values()) for t in tables], []
)

out += Processor(
    [
        extractor.PaddingRemover(),
        extractor.AutoHeader(DownRight("\d{4}[-/]\d{2,4}", [])),
        extractor.ColumnClean(),
        extractor.ColumnRename(
            {"WAREHOUSE": "NAME", "LOCATION NAME IDENTIFIER": "NAME", "ADDRESS": "CITY"}
        ),
        extractor.IndexSet(["NAME", "CITY"]),
        extractor.HeaderDataExtractor(
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
        extractor.DropNanIndex(),
        extractor.ColumnClean(),
    ]
)(test)

for i, o in enumerate(out):
    o.to_csv(f"{i}.csv")
