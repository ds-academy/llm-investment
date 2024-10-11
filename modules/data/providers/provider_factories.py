from typing import Dict, Any
from abc import ABC, abstractmethod
from modules.data.core import DataProvider
from modules.data.providers.yahoo import YahooFinance
from modules.data.providers.finance_data_reader import FinanceDataReader


class DataProviderFactory(ABC):
    @abstractmethod
    def create(self, symbol: str, config: Dict[str, Any]) -> DataProvider:
        pass


class YahooFinanceFactory(DataProviderFactory):
    def create(self, symbol: str, config: Dict[str, Any]) -> DataProvider:
        params = [
            "interval",
            "period",
            "raise_errors",
            "keepna",
            "timeout",
            "convert_utc",
            "start_date",
            "end_date",
        ]
        provider_params = {k: config.get(k) for k in params if k in config}
        provider_params.setdefault("raise_errors", True)
        provider_params.setdefault("keepna", True)
        provider_params.setdefault("timeout", 100)
        provider_params.setdefault("convert_utc", False)
        return YahooFinance(symbol=symbol, **provider_params)


class FinanceDataReaderFactory(DataProviderFactory):
    def create(self, symbol: str, config: Dict[str, Any]) -> DataProvider:
        params = ["interval", "start_date", "end_date"]
        provider_params = {k: config.get(k) for k in params if k in config}
        provider_params.setdefault("interval", "1D")
        return FinanceDataReader(symbol=symbol, **provider_params)


PROVIDER_FACTORIES = {
    "YahooFinance": YahooFinanceFactory(),
    "FinanceDataReader": FinanceDataReaderFactory(),
    # New Provider 추가
}
