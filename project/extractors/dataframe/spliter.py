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
        split_dfs = [table.iloc[split_indices[i-1]:split_indices[i], :] for i in range(1, len(split_indices))]

        return split_dfs