import yfinance as yf
import pandas as pd
import asyncio
import pytz
from yfinance.exceptions import YFPricesMissingError
from datetime import datetime, timedelta, time
from typing import Optional
from modules.data.core import DataProvider
from modules.data.constants import DATE_FORMAT
from modules.logger import get_logger


logger = get_logger(__name__)

KST_TIMEZONE = pytz.timezone("Asia/Seoul")
ET_TIMEZONE = pytz.timezone("America/New_York")
UTC_TIMEZONE = pytz.UTC
MARKET_OPEN_TIME = time(9, 30)
MARKET_CLOSE_TIME = time(16, 0)


def is_market_open(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(ET_TIMEZONE)
    if now.tzinfo is None:
        now = ET_TIMEZONE.localize(now)
    elif now.tzinfo != ET_TIMEZONE:
        now = now.astimezone(ET_TIMEZONE)

    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now <= market_close and now.weekday() < 5


class YahooFinance(DataProvider):
    """
    Get Yahoo Finance data
    Default returned time zone: datetime64[ns, America/New_York]
    Converted time zone: datetime64[ns, UTC]
    """

    def __init__(
        self,
        symbol: str,
        interval: str,
        period: str,
        raise_errors: bool = True,
        keepna: bool = True,
        timeout: int = 100,
        convert_utc: bool = True,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        super().__init__(start_date=start_date, end_date=end_date)
        self.symbol = symbol
        self.interval = interval
        self.period = period
        self.raise_errors = raise_errors
        self.keepna = keepna
        self.timeout = timeout
        self.convert_utc = convert_utc

        if self.interval == "1m":
            logger.warning(
                f"1-minute interval selected for {self.symbol}. Data will be limited to the last 7 days."
            )
            # 1분 간격일 경우 start_date를 7일 전으로 제한
            seven_days_ago = datetime.now(tz=pytz.UTC) - timedelta(days=7)
            if self.start_date is None or self.start_date < seven_days_ago:
                self.start_date = seven_days_ago
                logger.info(
                    f"Adjusted start_date to {self.start_date} for 1-minute interval"
                )

        self._start_date_str = self._format_date(self.start_date)
        self._end_date_str = self._format_date(self.end_date)

    async def get_data(self) -> pd.DataFrame:
        return await self._get_data_async()

    async def _get_data_async(self) -> pd.DataFrame:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_data_sync)

    def _get_data_sync(self) -> pd.DataFrame:
        now = datetime.now(ET_TIMEZONE)
        today = now.date()

        # 마켓이 열려 있는 경우, 현재 날짜와 요청된 날짜가 오늘이 아닌 경우에는 데이터를 가져온다.
        if self.interval == "1d" and is_market_open():
            if not (self.start_date and self.start_date.date() < today) and not (
                self.end_date and self.end_date.date() < today
            ):
                logger.warning(
                    "Market is open. Skipping 1d data fetch to avoid incomplete data."
                )
                return pd.DataFrame()

        # 우선 사용자가 지정한 start_date와 end_date를 우선적으로 사용하도록 수정
        if self.start_date and self.end_date:
            self._start_date_str = self.start_date.strftime("%Y-%m-%d")
            self._end_date_str = self.end_date.strftime("%Y-%m-%d")
        elif self.interval == "1m" and is_market_open():
            self._start_date_str = None
            self._end_date_str = None
        else:
            if not self._end_date_str:
                self._end_date_str = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%d")

        ticker = yf.Ticker(self.symbol)
        params = self._prepare_params()
        try:
            df = ticker.history(**params)
            if df.empty:
                logger.warning(f"No data found for {self.symbol}")
                return pd.DataFrame()

            return self._process_dataframe(df)

        except YFPricesMissingError as e:
            logger.error(f"YFPricesMissingError fetching data for {self.symbol}: {e}")
            return pd.DataFrame()
        except IndexError as e:
            logger.error(f"IndexError fetching data for {self.symbol}: {e}")
            return pd.DataFrame()
        except KeyError as e:
            logger.error(f"KeyError fetching data for {self.symbol}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching data for {self.symbol}: {e}", exc_info=True)
            return pd.DataFrame()

    def _prepare_params(self) -> dict:
        params = {
            "interval": self.interval,
            "raise_errors": self.raise_errors,
            "keepna": self.keepna,
            "timeout": self.timeout,
            "prepost": True,
        }

        if self.interval == "1m" and is_market_open():
            params["period"] = self.period
        else:
            if self.start_date:
                params["start"] = self._start_date_str
            if self.end_date:
                params["end"] = self._end_date_str

            if not self.start_date and not self.end_date:
                params["period"] = self.period

        return params

    def _process_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.reset_index()
        if self.interval == "1d":
            df = df.rename(columns={"Date": "date", "Volume": "volume"})
            now = datetime.now(ET_TIMEZONE)
            for idx in df.index:
                if df.loc[idx, "date"].date() == now.date():
                    if is_market_open():
                        df.loc[idx, "date"] = now
                    else:
                        df.loc[idx, "date"] = (
                            df.loc[idx, "date"]
                            .replace(
                                hour=MARKET_CLOSE_TIME.hour,
                                minute=MARKET_CLOSE_TIME.minute,
                                second=0,
                                microsecond=0,
                                tzinfo=ET_TIMEZONE,
                            )
                            .astimezone(UTC_TIMEZONE)
                        )
                else:
                    df.loc[idx, "date"] = (
                        df.loc[idx, "date"]
                        .replace(
                            hour=MARKET_CLOSE_TIME.hour,
                            minute=MARKET_CLOSE_TIME.minute,
                            second=0,
                            microsecond=0,
                            tzinfo=ET_TIMEZONE,
                        )
                        .astimezone(UTC_TIMEZONE)
                    )
        elif self.interval == "1m":
            df = df.rename(columns={"Datetime": "date", "Volume": "volume"})
        df["date"] = pd.to_datetime(df["date"], format=DATE_FORMAT, errors="coerce")
        df = df.drop_duplicates(subset=["date"], keep="last")
        if self.convert_utc:
            df["date"] = df["date"].dt.tz_convert("UTC")
        df = df.set_index("date")

        df.columns = df.columns.str.replace(" ", "_").str.lower()

        return df

    async def ping(self) -> bool:
        return await self._ping_async()

    async def _ping_async(self) -> bool:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._ping_sync)

    def _ping_sync(self) -> bool:
        try:
            yf.Ticker(self.symbol).info
            return True
        except Exception as e:
            logger.error(f"Error pinging {self.symbol}: {e}")
            return False

    # Synchronous wrappers for backward compatibility
    def get_data_sync(self) -> pd.DataFrame:
        return asyncio.run(self.get_data())

    def ping_sync(self) -> bool:
        return asyncio.run(self.ping())
