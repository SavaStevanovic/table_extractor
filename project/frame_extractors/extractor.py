from abc import abstractmethod
import pandas as pd
import typing


class TableExtactor:
    @abstractmethod
    def __call__(self, table: pd.DataFrame) -> typing.List[pd.DataFrame]:
        pass


class ListExtactor:
    def __init__(self, extractor: TableExtactor) -> None:
        self._extractor = extractor

    def __call__(self, tables: typing.List[pd.DataFrame]) -> typing.List[pd.DataFrame]:
        return [self._extractor(table) for table in tables]


class Processor:
    def __init__(self, extractors: typing.List[TableExtactor]) -> None:
        self._extractors = extractors

    def __call__(self, tables: typing.List[pd.DataFrame]) -> typing.List[pd.DataFrame]:
        for extractor in self._extractors:
            tables = ListExtactor(extractor)(tables)
            tables = sum(tables, [])

        return tables


class Merger:
    def __init__(self, mandatory_cols: list, concat_cols: list) -> None:
        self._mandatory_cols = mandatory_cols
        self._concat_cols = concat_cols

    def __call__(self, tables: typing.List[pd.DataFrame]) -> pd.DataFrame:
        total_table = tables[0]
        for table in tables[1:]:
            join_cols = (
                total_table.columns.intersection(table.columns)
                .intersection(self._concat_cols)
                .to_list()
            )
            if join_cols:
                total_table = pd.concat([total_table, table])
            else:
                total_table = total_table.merge(
                    table, validate="m:1", on=self._mandatory_cols + join_cols
                )

        return total_table
