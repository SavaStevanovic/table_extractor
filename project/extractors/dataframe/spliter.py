import re
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


class Footer(TableExtactor):
    def __init__(self, footer_words: typing.List[str]):
        self._footer_words = footer_words

    def __call__(self, table: pd.DataFrame) -> typing.List[pd.DataFrame]:
        row_strs = table.apply(
            lambda row: sum(
                any(w in str(cell) for w in self._footer_words) for cell in row
            ),
            axis=1,
        )
        footer_col = row_strs.to_numpy().argmax()
        table = table.iloc[:footer_col]
        return [table]


class IndexSet(TableExtactor):
    def __init__(self, index_cols: typing.List[str]):
        self._index_cols = index_cols

    def __call__(self, table: pd.DataFrame) -> typing.List[pd.DataFrame]:
        table = table.set_index(self._index_cols)
        return [table]


class HeaderDataExtractor(TableExtactor):
    def __init__(self, column_name: str, header_column_map: typing.Dict[str, str]):
        self._column_name = column_name
        self._header_column_map = header_column_map

    def __call__(self, table: pd.DataFrame) -> typing.List[pd.DataFrame]:
        subtables = [
            self._get_subtable(table.copy(deep=True), source, target)
            for source, target in self._header_column_map.items()
        ]
        subtables = [t for t in subtables if not t.empty]
        if not subtables:
            return [table]

        table = pd.concat(subtables)
        return [table]

    def _get_subtable(
        self, table: pd.DataFrame, source: list, target: list
    ) -> pd.DataFrame:
        common_cells = [
            cell
            for cell in table.columns
            if not any(w in str(cell) for w in self._header_column_map)
        ]
        cols = [cell for cell in table.columns if source in cell]
        if not cols:
            return pd.DataFrame(columns=common_cells)
        subtable = table[common_cells + cols]
        subtable.columns = [col.replace(source, "") for col in subtable.columns]
        subtable = subtable.assign(**{self._column_name: target})

        return subtable


class ColumnClean(TableExtactor):
    def __call__(self, table: pd.DataFrame) -> typing.List[pd.DataFrame]:
        table.columns = [
            re.sub(r"[^a-zA-Z\s]", "", col).upper().strip() for col in table.columns
        ]
        table.columns = [re.sub(r"\s+", " ", col) for col in table.columns]
        return [table]
