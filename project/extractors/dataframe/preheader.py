from abc import abstractmethod
import re
from typing import Any
import typing
import numpy as np

import pandas as pd


class PreheaderPropagator:
    @abstractmethod
    def __call__(self, prehader_data: pd.DataFrame, table: pd.DataFrame):
        pass


class DownRight(PreheaderPropagator):
    def __init__(self, regex: str) -> None:
        super().__init__()
        self._regex = regex

    def __call__(self, prehader_data: pd.DataFrame, table: pd.DataFrame):
        extracted_data = prehader_data.applymap(self.search_pattern)
        extracted_data = extracted_data.apply(self.last_non_none)
        extracted_data = extracted_data.ffill()
        new_cols = table.columns.copy().to_list()
        for i, col in enumerate(table.columns):
            if np.char.isnumeric(str(table[col].iloc[0])).all() and (
                self.search_pattern(table.columns[i]) is None
            ):
                new_cols[i] = table.columns[i] + " " + extracted_data[i]
        table.columns = new_cols
        return table

    def search_pattern(self, cell) -> typing.Optional[str]:
        match = re.search(self._regex, str(cell))
        return match.group(0) if match else None

    @staticmethod
    def last_non_none(series: pd.Series):
        non_none_values = series.dropna()
        return non_none_values.iloc[-1] if not non_none_values.empty else None
