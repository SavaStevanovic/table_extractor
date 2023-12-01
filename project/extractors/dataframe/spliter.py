import typing
import pandas as pd

from extractors.extractor import TableExtactor


class RowSpaceSpliter(TableExtactor):
    def __init__(self, empty_space: int):
        self._empty_space = empty_space

    def __call__(self, table: pd.DataFrame) -> typing.List[pd.DataFrame]:
        split_indices = [0]
        empty_row_count = 0

        for idx, row in table[:-1].iterrows():
            if row.isna().all():
                empty_row_count += 1
                if empty_row_count == self._empty_space:
                    split_indices.append(idx)
            else:
                empty_row_count = 0
        split_indices.append(len(table))
        split_dfs = [
            table.iloc[split_indices[i - 1] : split_indices[i], :]
            for i in range(1, len(split_indices))
        ]

        return split_dfs


class PaddingRemover(TableExtactor):
    def __call__(self, table: pd.DataFrame) -> typing.List[pd.DataFrame]:
        table = table.dropna(axis=0, how="all")
        table = table.dropna(axis=1, how="all")

        return [table]


class AutoHeader(TableExtactor):
    def __call__(self, table: pd.DataFrame) -> typing.List[pd.DataFrame]:
        row_strs = table.apply(
            lambda row: sum(isinstance(cell, str) for cell in row), axis=1
        )
        header_col = row_strs.to_numpy().argmax()
        table.columns = table.iloc[header_col]
        table = table.iloc[header_col + 1 :]
        return [table]
