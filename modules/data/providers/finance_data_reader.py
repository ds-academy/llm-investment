import pytz
import asyncio
import FinanceDataReader as fdr
import pandas as pd
from typing import Optional
from datetime import datetime, timedelta, time
from modules.data.core import DataProvider
from modules.data.constants import DATE_FORMAT
from modules.logger import get_logger


logger = get_logger(__name__)

KST_TIMEZONE = pytz.timezone("Asia/Seoul")
UTC_TIMEZONE = pytz.UTC
MARKET_OPEN_TIME = time(9, 0)
MARKET_CLOSE_TIME = time(15, 30)


def is_market_open(now: Optional[datetime] = None) -> bool:
    now = now or datetime.now(KST_TIMEZONE)
    if now.tzinfo is None:
        now = KST_TIMEZONE.localize(now)
    elif now.tzinfo != KST_TIMEZONE:
        now = now.astimezone(KST_TIMEZONE)

    market_open = now.replace(
        hour=MARKET_OPEN_TIME.hour,
        minute=MARKET_OPEN_TIME.minute,
        second=0,
        microsecond=0,
    )
    market_close = now.replace(
        hour=MARKET_CLOSE_TIME.hour,
        minute=MARKET_CLOSE_TIME.minute,
        second=0,
        microsecond=0,
    )
    return (
        market_open <= now <= market_close and now.weekday() < 5
    )  # FIXME: 향후 휴일 여부도 추가 필요


def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()
    df.columns = df.columns.str.lower()
    df = df.rename(columns={"date": "date"})

    if "date" not in df.columns:
        logger.error("DataFrame does not have a 'date' column")
        return pd.DataFrame()

    now = datetime.now(KST_TIMEZONE)
    today = now.date()

    # datetime 열을 나노초 정밀도로 파싱하고 시간대 정보를 추가합니다
    df["date"] = pd.to_datetime(
        df["date"], format=DATE_FORMAT, errors="coerce", utc=False
    )

    df = df.dropna(subset=["date"])

    if df.empty:
        logger.warning("DataFrame is empty after processing")
        return pd.DataFrame()

    # datetime 열에 시간대 정보 추가 및 마이크로초 정보 추가
    df["date"] = df["date"].apply(
        lambda x: (
            x.tz_localize(KST_TIMEZONE).replace(microsecond=x.microsecond or 0)
            if x.tzinfo is None
            else x.replace(microsecond=x.microsecond or 0)
        )
    )

    # 날짜별로 처리
    for date, group in df.groupby(df["date"].dt.date):
        if date < today:
            # 과거 데이터는 장 마감 시간으로 설정
            df.loc[group.index, "date"] = pd.Timestamp(date).replace(
                hour=MARKET_CLOSE_TIME.hour,
                minute=MARKET_CLOSE_TIME.minute,
                second=0,
                microsecond=0,
                tzinfo=KST_TIMEZONE,
            )
        elif date == today:
            if is_market_open(now):
                # 오늘이고 시장이 열려있는 경우, 시간을 그대로 유지하되 가장 최근 데이터는 현재 시간으로 업데이트
                last_index = group.index[-1]
                df.loc[last_index, "date"] = pd.Timestamp(now).astimezone(KST_TIMEZONE)
            else:
                # 오늘이지만 시장이 닫혀있는 경우, 장 마감 시간으로 설정
                df.loc[group.index, "date"] = pd.Timestamp(date).replace(
                    hour=MARKET_CLOSE_TIME.hour,
                    minute=MARKET_CLOSE_TIME.minute,
                    second=0,
                    microsecond=0,
                    tzinfo=KST_TIMEZONE,
                )
        else:
            # 미래 날짜의 데이터는 그대로 유지 (이런 경우는 없어야 하지만, 안전을 위해 추가)
            pass

    # UTC로 변환 및 인덱스 설정
    df["date"] = df["date"].apply(lambda x: x.astimezone(UTC_TIMEZONE))

    # 마이크로초가 0인 경우에도 표시되도록 문자열로 변환 후 다시 datetime으로 변환
    df["date"] = pd.to_datetime(df["date"].dt.strftime(DATE_FORMAT))

    df = df.set_index("date")

    return df


class FinanceDataReader(DataProvider):
    def __init__(
        self,
        symbol: str,
        interval: str = "1D",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ):
        super().__init__(start_date=start_date, end_date=end_date)
        self.symbol = symbol
        self.interval = interval
        logger.info(f"FinanceDataReaderProvider initialized for symbol: {symbol}")

    @staticmethod
    def _remove_tz_info(dt: Optional[datetime]) -> Optional[datetime]:
        return dt.replace(tzinfo=None) if dt and dt.tzinfo else dt

    async def get_data(self) -> pd.DataFrame:
        logger.info(f"Fetching data for {self.symbol}")

        now = datetime.now(KST_TIMEZONE)
        today = now.date()

        # 마켓이 열려 있는 경우, 현재 날짜와 요청된 날짜가 오늘이 아닌 경우에는 데이터를 가져온다.
        if self.interval == "1d" and is_market_open():
            if not (self.start_date and self.start_date.date() < today) and not (
                    self.end_date and self.end_date.date() < today
            ):
                logger.warning(
                    "Market is open. Skipping 1D data fetch to avoid incomplete data."
                )
                return pd.DataFrame()

        start_date = self._remove_tz_info(self.start_date)
        end_date = self._remove_tz_info(self.end_date or datetime.now(KST_TIMEZONE))

        if self.interval == "1m" and is_market_open():
            end_date = datetime.now(KST_TIMEZONE)
            start_date = end_date  # 1분 간격일 경우 동일한 날짜로 설정

        if start_date and start_date.date() >= end_date.date():
            logger.warning(
                "Start date is greater than or equal to end date. Adjusting end date."
            )
            end_date = (start_date + timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        params = {
            "symbol": self.symbol,
            "start": start_date.strftime("%Y-%m-%d") if start_date else None,
            "end": end_date.strftime("%Y-%m-%d"),
            # "interval": self.interval,
        }

        try:
            logger.debug(f"Calling FinanceDataReader with params: {params}")
            df = await asyncio.to_thread(fdr.DataReader, **params)

            if df.empty:
                logger.warning(f"No data found for {self.symbol}")
                return pd.DataFrame()

            logger.info(f"Data fetched successfully for {self.symbol}")
            logger.debug(f"Raw data shape: {df.shape}")

            df = process_dataframe(df)

            logger.debug(f"Processed data shape: {df.shape}")
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {self.symbol}: {e}", exc_info=True)
            return pd.DataFrame()

    async def ping(self) -> bool:
        logger.info(f"Pinging FinanceDataReader for {self.symbol}")
        try:
            end_date = datetime.now(KST_TIMEZONE)
            start_date = end_date - timedelta(days=7)
            df = await asyncio.to_thread(
                fdr.DataReader,
                self.symbol,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
            )
            success = not df.empty
            logger.info(
                f"Ping {'successful' if success else 'failed'} for {self.symbol}"
            )
            return success
        except Exception as e:
            logger.error(f"Ping failed for {self.symbol}: {e}", exc_info=True)
            return False

    def get_data_sync(self) -> pd.DataFrame:
        return asyncio.run(self.get_data())

    def ping_sync(self) -> bool:
        return asyncio.run(self.ping())


