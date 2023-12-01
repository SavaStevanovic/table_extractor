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
