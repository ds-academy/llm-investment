import os
import aiofiles
import asyncio
import pytz
import glob
import pandas as pd
from aiofiles.os import path as aiopath
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
from abc import ABCMeta, abstractmethod
from contextlib import asynccontextmanager
from io import StringIO
from google.cloud import storage
from modules.data.filelock import AsyncFileLock
from modules.data.constants import DATE_FORMAT
from modules.logger import get_logger


logger = get_logger(__name__)


class DataProvider(metaclass=ABCMeta):
    def __init__(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ):
        self._start_date = start_date
        self._end_date = end_date

    @property
    def start_date(self) -> datetime:
        return self._start_date

    @property
    def end_date(self) -> datetime:
        return self._end_date

    @start_date.setter
    def start_date(self, start_date: datetime):
        self._start_date = start_date

    @end_date.setter
    def end_date(self, end_date: datetime):
        self._end_date = end_date

    @staticmethod
    def _format_date(dt: Optional[datetime]) -> Optional[str]:
        return dt.date().isoformat() if dt else None

    @abstractmethod
    async def get_data(self) -> pd.DataFrame:
        pass

    @abstractmethod
    async def ping(self) -> bool:
        pass

    def get_data_sync(self) -> pd.DataFrame:
        return asyncio.run(self.get_data())

    def ping_sync(self) -> bool:
        return asyncio.run(self.ping())


class DataPipeline(metaclass=ABCMeta):
    def __init__(
        self,
        data_provider: DataProvider,
        base_path: str,
        chunk_size: int = 10000,  # Default Chunk Size
        use_file_lock: bool = True,
        cache_days: int = 7,
        storage_type: str = "local",  # 'local' or 'gcs'
        bucket_name: Optional[str] = None,
    ):
        self.data_provider = data_provider
        self.base_path = base_path
        self.chunk_size = chunk_size
        self.use_file_lock = use_file_lock
        self.cache_days = cache_days
        self.storage_type = storage_type
        self.bucket_name = bucket_name

        if self.storage_type == "local":
            os.makedirs(base_path, exist_ok=True)
        elif self.storage_type == "gcs":
            if not bucket_name:
                raise ValueError("Bucket name must be provided for GCS storage")
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(bucket_name)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")

    def get_params(self) -> Dict[str, Any]:
        return {
            "data_provider": self.data_provider if self.data_provider else "None",
            "base_path": self.base_path,
            "use_file_lock": self.use_file_lock,
            "cache_days": self.cache_days,
            "storage_type": self.storage_type,
            "bucket_name": self.bucket_name,
        }

    def _get_file_path(self, chunk_num: int = 0) -> str:
        logger.debug(f"Getting file path for chunk {self.base_path}/{chunk_num}")
        if self.storage_type == "local":
            return os.path.join(self.base_path, f"chunk{chunk_num}.csv")
        elif self.storage_type == "gcs":
            return f"{self.base_path}/chunk{chunk_num}.csv"

    @asynccontextmanager
    async def _file_lock(self, file_path: str):
        if self.storage_type == "local" and self.use_file_lock:
            lock = AsyncFileLock(file_path + ".lock")
            async with lock.acquire():
                yield
        else:
            yield

    @abstractmethod
    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    async def fetch_start(self, **kwargs):
        pass

    async def _load_cache(self) -> pd.DataFrame:
        end_date = datetime.now(tz=pytz.UTC)
        start_date = end_date - timedelta(days=self.cache_days)
        return await self._load_date_range(start_date, end_date)

    async def _load_date_range(
        self, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        all_data = []
        chunk_num = 0
        while True:
            file_path = self._get_file_path(chunk_num)
            if not await self._file_exists(file_path):
                break
            data = await self._read_csv(file_path)
            logger.debug(f"Loaded chunk {chunk_num} from {file_path} / {data.shape}")
            filtered_data = data[(data.index >= start_date) & (data.index <= end_date)]
            if not filtered_data.empty:
                all_data.append(filtered_data)
            chunk_num += 1

        return pd.concat(all_data) if all_data else pd.DataFrame()

    async def _file_exists(self, file_path: str) -> bool:
        if self.storage_type == "local":
            return await asyncio.to_thread(os.path.exists, file_path)
        elif self.storage_type == "gcs":
            blob = self.bucket.blob(file_path)
            return await asyncio.to_thread(blob.exists)

    async def _read_csv(self, file_path: str) -> pd.DataFrame:
        logger.info(f"Attempting to read CSV file from {file_path}")
        try:
            async with self._file_lock(file_path):
                if self.storage_type == "local":
                    file_exists = await aiopath.exists(file_path)
                    if not file_exists:
                        logger.warning(f"CSV file does not exist: {file_path}")
                        return pd.DataFrame()
                    async with aiofiles.open(file_path, mode="r") as f:
                        content = await f.read()
                elif self.storage_type == "gcs":
                    blob = self.bucket.blob(file_path)
                    file_exists = await asyncio.to_thread(blob.exists)
                    if not file_exists:
                        logger.warning(f"CSV file does not exist: {file_path}")
                        return pd.DataFrame()
                    content = await asyncio.to_thread(blob.download_as_text)
                else:
                    raise ValueError(f"Unsupported storage type: {self.storage_type}")

            try:
                df = pd.read_csv(StringIO(content), parse_dates=["date"])
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(
                        df["date"], format=DATE_FORMAT, errors="coerce"
                    )
                    df.set_index("date", inplace=True)
                else:
                    logger.error("No 'date' column found in the CSV file")
                    return pd.DataFrame()
            except KeyError:
                logger.warning(
                    "'date' column not found, attempting to read without specifying index"
                )
                df = pd.read_csv(StringIO(content))
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(
                        df["date"], format=DATE_FORMAT, errors="coerce"
                    )
                    df.set_index("date", inplace=True)
                else:
                    logger.error("No 'date' column found in the CSV file")
                    return pd.DataFrame()

            if df.empty:
                logger.warning(f"CSV file is empty: {file_path}")
                return pd.DataFrame()

            if not isinstance(df.index, pd.DatetimeIndex):
                logger.warning("Index is not DatetimeIndex, attempting to convert")
                df.index = pd.to_datetime(df.index, errors="coerce")

            df = df[~df.index.isna()]

            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")

            return df

        except FileNotFoundError:
            logger.warning(f"CSV file not found: {file_path}")
            return pd.DataFrame()
        except pd.errors.EmptyDataError:
            logger.warning(f"CSV file is empty: {file_path}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error reading CSV file {file_path}: {e}", exc_info=True)
            return pd.DataFrame()

    async def _save_data(self, new_data: pd.DataFrame):
        if new_data.empty:
            return

        last_chunk_num = await self._get_last_chunk_number()
        all_existing_data = await self._read_all_chunks(last_chunk_num)

        # 기존 데이터와 새 데이터를 병합하고 중복 제거
        combined_data = (
            pd.concat([all_existing_data, new_data]).drop_duplicates().sort_index()
        )

        chunk_size = self.chunk_size
        total_rows = len(combined_data)
        num_chunks = (total_rows - 1) // chunk_size + 1

        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min((i + 1) * chunk_size, total_rows)
            chunk_data = combined_data.iloc[start_idx:end_idx]

            file_path = self._get_file_path(i)
            await self._write_csv(file_path, chunk_data)

            logger.info(f"{'Updated' if i <= last_chunk_num else 'Created'} chunk {i}")

        # 불필요한 이전 청크들 삭제
        for i in range(num_chunks, last_chunk_num + 1):
            file_path = self._get_file_path(i)
            if await self._file_exists(file_path):
                await self._delete_file(file_path)
                logger.info(f"Removed unnecessary chunk {i}")

        logger.info(f"Saved {total_rows} rows in {num_chunks} chunks")

    async def _save_new_data(self, new_data: pd.DataFrame):
        if new_data.empty:
            return

        last_chunk_num = await self._get_last_chunk_number()
        last_chunk_data = (
            await self._read_csv(self._get_file_path(last_chunk_num))
            if last_chunk_num >= 0
            else pd.DataFrame()
        )

        # NaT 값 제거
        last_chunk_data = last_chunk_data[~last_chunk_data.index.isna()]
        new_data = new_data[~new_data.index.isna()]

        if not last_chunk_data.empty:
            last_date = last_chunk_data.index[-1]
            new_data = new_data[new_data.index > last_date]

        combined_data = pd.concat([last_chunk_data, new_data]).sort_index()
        chunk_size = self.chunk_size

        current_chunk = last_chunk_num
        start_idx = 0

        while start_idx < len(combined_data):
            end_idx = min(start_idx + chunk_size, len(combined_data))
            chunk_data = combined_data.iloc[start_idx:end_idx]

            await self._write_csv(self._get_file_path(current_chunk), chunk_data)
            logger.info(
                f"{'Updated' if current_chunk == last_chunk_num else 'Created'} chunk {current_chunk}"
            )

            start_idx = end_idx
            current_chunk += 1

        logger.info(
            f"Saved {len(new_data)} new rows across {current_chunk - last_chunk_num} chunks"
        )

    async def _read_all_chunks(self, last_chunk_num: int) -> pd.DataFrame:
        all_data = []
        for chunk_num in range(last_chunk_num + 1):
            chunk_data = await self._read_csv(self._get_file_path(chunk_num))
            if not chunk_data.empty:
                all_data.append(chunk_data)
        return pd.concat(all_data) if all_data else pd.DataFrame()

    async def _read_last_chunk(self, chunk_num: int) -> pd.DataFrame:
        file_path = self._get_file_path(chunk_num)
        return await self._read_csv(file_path)

    async def _get_last_chunk_number(self) -> int:
        chunk_num = 0
        while True:
            file_path = self._get_file_path(chunk_num)
            if not await self._file_exists(file_path):
                return chunk_num - 1 if chunk_num > 0 else 0
            chunk_num += 1

    async def get_all_data(self) -> pd.DataFrame:
        logger.info(f"Loading all data from {self.base_path}")
        all_data = []
        chunk_num = 0
        while True:
            file_path = self._get_file_path(chunk_num)
            if not await self._file_exists(file_path):
                break
            data = await self._read_csv(file_path)
            if not data.empty:
                all_data.append(data)
            chunk_num += 1
        return (
            pd.concat(all_data).sort_index().drop_duplicates(keep="last")
            if all_data
            else pd.DataFrame()
        )

    async def get_data_range(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        logger.info(f"Getting data from {start_date} to {end_date}")
        all_data = await self.get_all_data()
        if all_data.empty:
            logger.warning(f"No data found in the range {start_date} to {end_date}")
            return pd.DataFrame()

        if start_date:
            start_ts = pd.Timestamp(start_date)
            if start_ts.tzinfo is None:
                start_ts = start_ts.tz_localize(pytz.UTC)
            all_data = all_data[all_data.index >= start_ts]

        if end_date:
            end_ts = pd.Timestamp(end_date)
            if end_ts.tzinfo is None:
                end_ts = end_ts.tz_localize(pytz.UTC)
            all_data = all_data[all_data.index <= end_ts]

        return all_data

    async def get_latest_n_days(self, n: int) -> pd.DataFrame:
        logger.info(f"Getting latest {n} days of data")
        end_date = datetime.now(tz=pytz.UTC)
        start_date = end_date - timedelta(days=n)
        return await self.get_data_range(start_date, end_date)

    async def _delete_file(self, file_path: str):
        if self.storage_type == "local":
            os.remove(file_path)
        elif self.storage_type == "gcs":
            blob = self.bucket.blob(file_path)
            await asyncio.to_thread(blob.delete)

    async def clean_old_data(self, days: int):
        logger.info(f"Cleaning data older than {days} days")
        cutoff_date = datetime.now(tz=pytz.UTC) - timedelta(days=days)
        chunk_num = 0
        while True:
            file_path = self._get_file_path(chunk_num)
            if not await self._file_exists(file_path):
                break
            data = await self._read_csv(file_path)
            if not data.empty and data.index.max() < cutoff_date:
                await self._delete_file(file_path)
                logger.info(f"Deleted old data file {file_path}")
            chunk_num += 1

    async def get_latest_datetime(self) -> Optional[datetime]:
        """
        return UTC[datetime.datetime]
        """
        logger.info("Getting latest datetime from the last chunk")
        last_chunk_num = await self._get_last_chunk_number()
        if last_chunk_num < 0:
            return None

        last_chunk_data = await self._read_last_chunk(last_chunk_num)
        if not last_chunk_data.empty:
            latest_timestamp = last_chunk_data.index.max()

            if isinstance(latest_timestamp, pd.Timestamp):
                latest_datetime = latest_timestamp.to_pydatetime()
            elif isinstance(latest_timestamp, datetime):
                latest_datetime = latest_timestamp
            else:
                latest_datetime = pd.to_datetime(latest_timestamp).to_pydatetime()

            if latest_datetime.tzinfo is None:
                latest_datetime = latest_datetime.replace(tzinfo=pytz.UTC)

            logger.info(f"Latest datetime in data: {latest_datetime}")
            return latest_datetime
        return None

    async def update_to_latest(self):
        """
        Control function to update data to the latest available
        """
        logger.info("Updating to latest data")
        if self.data_provider is None:
            logger.error("데이터 제공자가 설정되지 않았습니다.")
            return

        try:
            latest_datetime = await self.get_latest_datetime()  # return: datetime
            original_end_date = self.data_provider.end_date

            # If existing data is empty, fetch all data
            if latest_datetime is None:
                logger.info("데이터가 없습니다. 전체 데이터를 가져옵니다.")
                new_start_date = (
                    self.data_provider.start_date or None
                )  # config에서 지정한 start_date 사용
            else:
                if latest_datetime.tzinfo is None:
                    latest_datetime = latest_datetime.replace(tzinfo=pytz.UTC)

                new_start_date = max(
                    self.data_provider.start_date,
                    latest_datetime + timedelta(microseconds=1),
                )

            # Use the original end_date, but ensure it's not in the future
            now_utc = datetime.now(tz=pytz.UTC)
            new_end_date = (
                min(original_end_date, now_utc) if original_end_date else now_utc
            )

            self.data_provider.start_date = new_start_date
            self.data_provider.end_date = new_end_date

            # Check if new_start_date is greater than new_end_date
            if new_start_date > new_end_date:
                logger.info(
                    f"업데이트할 데이터가 이미 최신 상태입니다: {latest_datetime}"
                )
                return

            logger.info(
                f"데이터를 {self.data_provider.start_date or '처음'}부터 {self.data_provider.end_date}까지 업데이트 합니다."
            )

            new_data = await self.fetch_data()
            if not new_data.empty:
                await self._save_new_data(new_data)
                updated_latest_datetime = new_data.index.max()
                logger.info(
                    f"데이터를 {updated_latest_datetime}까지 업데이트 했습니다. {len(new_data)}행이 추가 되었습니다."
                )
            else:
                logger.info(
                    f"새로운 데이터가 없습니다. 마지막 데이터: {latest_datetime or '없음'}"
                )
            logger.info("데이터 업데이트가 완료되었습니다.")
        except Exception as e:
            logger.error(f"데이터 업데이트 중 오류 발생: {e}", exc_info=True)

    async def save(self):
        logger.info("Saving cached data")
        await self._save_data(self._cached_data)

    async def load(self, days_back: int = 30) -> pd.DataFrame:
        logger.info(f"Loading data from the past {days_back} days")
        end_date = datetime.now(tz=pytz.UTC)
        start_date = end_date - timedelta(days=days_back)
        return await self.get_data_range(start_date, end_date)

    async def _write_csv(self, file_path: str, data: pd.DataFrame):
        logger.info(f"Writing CSV to {file_path}")
        if self.storage_type == "local":
            async with self._file_lock(file_path):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                await asyncio.to_thread(
                    data.to_csv, file_path, index=True, header=True, mode="w"
                )
        elif self.storage_type == "gcs":
            csv_buffer = StringIO()
            data.to_csv(csv_buffer, index=True, header=True)
            csv_string = csv_buffer.getvalue()
            blob = self.bucket.blob(file_path)
            await asyncio.to_thread(
                blob.upload_from_string, csv_string, content_type="text/csv"
            )

    async def _append_csv(self, file_path: str, data: pd.DataFrame):
        logger.info(f"Appending CSV to {file_path}")
        if self.storage_type == "local":
            async with self._file_lock(file_path):
                await asyncio.to_thread(
                    data.to_csv, file_path, mode="a", header=False, index=True
                )
        elif self.storage_type == "gcs":
            existing_data = await self._read_csv(file_path)
            combined_data = (
                pd.concat([existing_data, data]).drop_duplicates().sort_index()
            )
            await self._write_csv(file_path, combined_data)

    async def close(self):
        """
        리소스 정리
        """
        logger.info(f"Closing data pipeline for {self.data_provider}")

        try:
            if hasattr(self.data_provider, "close"):
                await self.data_provider.close()
                logger.info("Data provider connection closed")

            if self.storage_type == "gcs" and hasattr(self, "storage_client"):
                self.storage_client.close()
                logger.info("GCS client closed")

            if self.use_file_lock:
                await self._release_all_locks()
                logger.info("All file locks released")

            logger.info(f"Data pipeline for {self.data_provider} closed successfully")
        except Exception as e:
            logger.error(f"Error while closing data pipeline: {e}", exc_info=True)

    async def _release_all_locks(self):
        if self.storage_type == "local":
            lock_pattern = os.path.join(self.base_path, "*.lock")
            lock_files = glob.glob(lock_pattern)
            for lock_file in lock_files:
                try:
                    os.remove(lock_file)
                    logger.info(f"Removed lock file: {lock_file}")
                except Exception as e:
                    logger.error(f"Failed to remove lock file {lock_file}: {e}")

        # gcs는 file lock 하지 않음
        # elif self.storage_type == "gcs":
        #     # GCS의 경우 lock 파일 목록을 가져와서 삭제
        #     prefix = f"{self.base_path}/"
        #     blobs = self.bucket.list_blobs(prefix=prefix)
        #     for blob in blobs:
        #         if blob.name.endswith(".lock"):
        #             try:
        #                 blob.delete()
        #                 logger.info(f"Removed lock file: {blob.name}")
        #             except Exception as e:
        #                 logger.error(f"Failed to remove lock file {blob.name}: {e}")

        logger.info("All file locks released")

    # Synchronous wrappers for backward compatibility
    def fetch_data_sync(self, **kwargs) -> pd.DataFrame:
        return asyncio.run(self.fetch_data(**kwargs))

    def fetch_start_sync(self, **kwargs):
        asyncio.run(self.fetch_start(**kwargs))

    def get_all_data_sync(self) -> pd.DataFrame:
        return asyncio.run(self.get_all_data())

    def get_data_range_sync(
        self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None
    ) -> pd.DataFrame:
        return asyncio.run(self.get_data_range(start_date, end_date))

    def get_latest_n_days_sync(self, n: int) -> pd.DataFrame:
        return asyncio.run(self.get_latest_n_days(n))

    def clean_old_data_sync(self, days: int):
        asyncio.run(self.clean_old_data(days))

    def get_latest_datetime_sync(self) -> Optional[datetime]:
        return asyncio.run(self.get_latest_datetime())

    def update_to_latest_sync(self):
        asyncio.run(self.update_to_latest())

    def save_sync(self):
        asyncio.run(self.save())

    def load_sync(self, days_back: int = 30) -> pd.DataFrame:
        return asyncio.run(self.load(days_back))
