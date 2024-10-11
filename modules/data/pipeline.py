import asyncio
import pandas as pd
import pytz
from typing import Optional
from asyncio import Event
from pandas.tseries.offsets import BDay
from datetime import datetime, date, timedelta
from modules.data.core import DataProvider
from modules.data.core import DataPipeline
from modules.logger import get_logger


logger = get_logger(__name__)

# FIXME
# Yahoo Provider 에서 종가 기준으로 가져오기 때문에... 만약, 장이 오픈되고 있을 때 가져오는 데이터와,
# 장 마감 뒤에 가져오는 데이터의 날짜가 동일하다면, 이 부분에서 수정이 필요함.


class ProviderDataPipeline(DataPipeline):
    def __init__(
        self,
        data_provider: DataProvider,
        base_path: str,
        chunk_size: int = 10000,
        use_file_lock: bool = True,
        cache_days: int = 7,
        fetch_interval: int = 60,
        storage_type: str = "local",
        bucket_name: Optional[str] = None,
    ):
        super().__init__(
            data_provider=data_provider,
            base_path=base_path,
            chunk_size=chunk_size,
            use_file_lock=use_file_lock,
            cache_days=cache_days,
            storage_type=storage_type,
            bucket_name=bucket_name,
        )
        self.fetch_interval = fetch_interval

    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        if self.data_provider is None:
            return pd.DataFrame()

        logger.info(f"Fetching data for provider {self.data_provider}")
        new_data = await self.data_provider.get_data()

        if not new_data.empty:
            new_data["date"] = pd.to_datetime(new_data.index, utc=True)
            new_data = new_data.set_index("date")

            latest_datetime = await self.get_latest_datetime()
            if latest_datetime:
                new_data = new_data[new_data.index > latest_datetime]

        return new_data

    async def fetch_and_save_realtime(
        self, stop_event: Event, single_fetch: bool = False
    ):
        first_run = True
        try:
            while not stop_event.is_set():
                if first_run:
                    logger.info("Starting initial data fetch and save")
                    await self.update_to_latest()
                    first_run = False
                else:
                    logger.info("Starting real-time data fetch and save cycle")
                    latest_datetime = await self.get_latest_datetime()
                    if latest_datetime:
                        self.data_provider.start_date = latest_datetime + timedelta(
                            microseconds=1
                        )
                    new_data = await self.fetch_data()
                    if not new_data.empty:
                        await self._save_new_data(new_data)
                        updated_latest_datetime = new_data.index.max()
                        logger.info(
                            f"데이터를 {updated_latest_datetime}까지 업데이트 했습니다. {len(new_data)}행이 추가 되었습니다."
                        )
                    else:
                        logger.info("새로운 데이터가 없습니다.")

                if single_fetch:
                    logger.info("Single fetch completed, exiting loop")
                    break

                logger.info(
                    f"Waiting for {self.fetch_interval} seconds before next fetch"
                )
                await asyncio.sleep(self.fetch_interval)
        except Exception as e:
            logger.error(f"Error in fetch_and_save_realtime: {e}", exc_info=True)

    async def fetch_and_save_increment(self):
        try:
            new_data = await self.fetch_data()
            if not new_data.empty:
                await self._save_data(new_data)
                updated_latest_datetime = new_data.index.max()
                logger.info(
                    f"데이터를 {updated_latest_datetime}까지 업데이트 했습니다. {len(new_data)}행이 추가 되었습니다."
                )
            else:
                logger.info("새로운 데이터가 없습니다.")
        except Exception as e:
            logger.error(f"데이터 업데이트 중 오류 발생: {e}", exc_info=True)

    async def fetch_start(self, **kwargs):
        logger.info("Starting data fetch")
        try:
            latest_datetime = await self.get_latest_datetime()
            if latest_datetime is None:
                logger.info("No existing data found, fetching new data")
                await self.update_to_latest()
            else:
                logger.info(f"Existing data found. Last date: {latest_datetime}")
                self.data_provider.start_date = latest_datetime + timedelta(
                    microseconds=1
                )

            logger.info(
                f"Data provider start date set to: {self.data_provider.start_date}"
            )
        except Exception as e:
            logger.error(f"Error in fetch_start: {e}", exc_info=True)
            raise
