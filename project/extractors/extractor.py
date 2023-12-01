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