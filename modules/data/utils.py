import os
import yaml
import asyncio
import aiofiles
import pandas as pd
import pytz
import importlib
import time
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from functools import reduce
from typing import List, Optional, Dict, Any, Callable
from modules.data.pipeline import ProviderDataPipeline, DataProvider
from modules.data.providers.provider_factories import PROVIDER_FACTORIES
from modules.logger import get_logger


logger = get_logger(__name__)

# Constants
CONFIG_KEY_STRATEGY = "strategy"
CONFIG_KEY_ALGORITHM = "algorithm"
CONFIG_KEY_DATA_PIPELINES = "data_pipelines"
CONFIG_KEY_NAME = "name"

CONFIG_KEY_STOCKS = "stocks"
CONFIG_KEY_BASE_PATH = "base_path"
CONFIG_KEY_STOCKS_FILE = "stocks_file"


def find_project_root(current_path: str) -> str:
    logger.info(f"Searching for project root from: {current_path}")
    while True:
        if os.path.exists(os.path.join(current_path, ".git")):
            logger.info(f"Project root found: {current_path}")
            return current_path
        parent = os.path.dirname(current_path)
        if parent == current_path:
            logger.error("Project root not found")
            raise ValueError("Project root not found")
        current_path = parent


async def read_config(config_path: str) -> Dict[str, Any]:
    logger.info(f"Reading config file: {config_path}")
    try:
        async with aiofiles.open(config_path, "r") as file:
            config = yaml.safe_load(await file.read())
        logger.debug("Config file loaded successfully")
    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        raise FileNotFoundError(f"Config file not found: {config_path}")
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        raise ValueError(f"Error parsing YAML file: {e}")

    project_root = find_project_root(os.path.dirname(os.path.abspath(config_path)))

    # 새로운 구조로 config 재구성
    new_config = {
        CONFIG_KEY_STRATEGY: {},
        CONFIG_KEY_ALGORITHM: {},
        CONFIG_KEY_DATA_PIPELINES: {},
    }

    # Strategy 정보 처리
    if CONFIG_KEY_STRATEGY in config:
        new_config[CONFIG_KEY_STRATEGY] = config[CONFIG_KEY_STRATEGY]
        # Strategy 내의 data_pipelines 정보를 별도로 이동
        # FIXME : 이 기능이 꼭 필요해?
        if CONFIG_KEY_DATA_PIPELINES in new_config[CONFIG_KEY_STRATEGY]:
            new_config[CONFIG_KEY_DATA_PIPELINES] = new_config[CONFIG_KEY_STRATEGY].pop(
                CONFIG_KEY_DATA_PIPELINES
            )

    # Algorithm 정보 처리
    if CONFIG_KEY_ALGORITHM in config:
        new_config[CONFIG_KEY_ALGORITHM] = config[CONFIG_KEY_ALGORITHM]

    # Data Pipelines
    if CONFIG_KEY_DATA_PIPELINES in config:
        new_config[CONFIG_KEY_DATA_PIPELINES] = config[CONFIG_KEY_DATA_PIPELINES]

        # start_date 처리
        start_date = new_config[CONFIG_KEY_DATA_PIPELINES].get(
            "start_date", "1970-01-01"
        )

        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)

        start_date = start_date.replace(tzinfo=pytz.UTC)
        new_config[CONFIG_KEY_DATA_PIPELINES]["start_date"] = start_date

        # end_date 처리
        end_date = new_config[CONFIG_KEY_DATA_PIPELINES].get("end_date", "TODAY")

        if end_date == "TODAY" or isinstance(end_date, str):
            if end_date == "TODAY":
                end_date = datetime.now(tz=pytz.UTC)
            else:
                end_date = datetime.fromisoformat(end_date)
                end_date = end_date.replace(tzinfo=pytz.UTC)

        new_config[CONFIG_KEY_DATA_PIPELINES]["end_date"] = end_date

    # Data Pipelines 정보가 없는 경우 처리
    if not new_config[CONFIG_KEY_DATA_PIPELINES]:
        logger.warning("No data pipeline configuration found")
        new_config[CONFIG_KEY_DATA_PIPELINES] = {}

        new_config[CONFIG_KEY_DATA_PIPELINES] = {
            "start_date": datetime(1970, 1, 1, tzinfo=pytz.UTC),
            "end_date": datetime.now(tz=pytz.UTC),
        }

    # base_path 설정
    if CONFIG_KEY_BASE_PATH in new_config[CONFIG_KEY_DATA_PIPELINES]:
        storage_type = new_config[CONFIG_KEY_DATA_PIPELINES].get(
            "storage_type", "local"
        )
        if storage_type == "gcs":
            # GCS의 경우 base_path를 그대로 사용
            new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH] = new_config[
                CONFIG_KEY_DATA_PIPELINES
            ][CONFIG_KEY_BASE_PATH]
        elif storage_type == "local":
            # local의 경우 project_root와 결합
            new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH] = (
                os.path.normpath(
                    os.path.join(
                        project_root,
                        new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH],
                    )
                )
            )
        else:
            logger.warning(
                f"Unknown storage type: {storage_type}. Using base_path as is."
            )
    else:
        # base_path가 설정되지 않은 경우 기본값 설정
        new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_BASE_PATH] = os.path.join(
            project_root, "data"
        )

    # bucket_name 처리 (GCS를 위해 추가)
    if storage_type == "gcs":
        bucket_name = new_config[CONFIG_KEY_DATA_PIPELINES].get("bucket_name")
        if not bucket_name:
            logger.error("bucket_name must be provided for GCS storage")
            raise ValueError("bucket_name must be provided for GCS storage")
        new_config[CONFIG_KEY_DATA_PIPELINES]["bucket_name"] = bucket_name

    # stocks_file 처리
    if CONFIG_KEY_STOCKS_FILE in new_config[CONFIG_KEY_DATA_PIPELINES]:
        stocks_file = new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_STOCKS_FILE]
        stocks_path = os.path.join(os.path.dirname(config_path), stocks_file)
        try:
            with open(stocks_path, "r") as file:
                stocks_config = yaml.safe_load(file)
            new_config[CONFIG_KEY_DATA_PIPELINES][CONFIG_KEY_STOCKS] = stocks_config[
                CONFIG_KEY_STOCKS
            ]
        except FileNotFoundError:
            logger.warning(
                f"Stocks file '{stocks_file}' not found. Using stocks defined in main config."
            )
        except KeyError:
            logger.warning(
                f"Invalid structure in stocks file '{stocks_file}'. Using stocks defined in main config."
            )

    logger.info("Config processing completed")
    return new_config


async def load_module(config: Dict, type_key: str):
    name = config[type_key]["name"]
    if "module" in config[type_key]:
        module_path = config[type_key]["module"]
    else:
        raise NotImplementedError  # FIXME : 모듈 경로 직접 입력
    try:
        module = importlib.import_module(module_path)
        return getattr(module, name)
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to import {type_key} {name}: {e}")
        raise


async def create_data_providers(config: Dict[str, Any]) -> List[DataProvider]:
    logger.info("Creating data providers")
    data_pipelines = config[CONFIG_KEY_DATA_PIPELINES]
    stocks = data_pipelines[CONFIG_KEY_STOCKS]

    if CONFIG_KEY_NAME not in data_pipelines:
        logger.error(f"{CONFIG_KEY_NAME} not found in data_pipelines configuration")
        raise ValueError(f"{CONFIG_KEY_NAME} must be specified in the configuration")

    provider_name = data_pipelines[CONFIG_KEY_NAME]

    try:
        provider_class = await load_module(config, CONFIG_KEY_DATA_PIPELINES)
    except Exception as e:
        logger.error(f"Failed to load provider {provider_name}: {e}")
        raise

    logger.info(f"Using provider class: {provider_class.__name__}")

    if provider_class.__name__ not in PROVIDER_FACTORIES:
        logger.error(f"Unknown provider class: {provider_class.__name__}")
        raise ValueError(f"Unknown provider class: {provider_class.__name__}")

    factory = PROVIDER_FACTORIES[provider_class.__name__]

    providers = []
    for stock in stocks:
        symbol = stock["symbol"]
        stock_config = {
            **data_pipelines,
            **stock,
        }  # Merge global and stock-specific configs
        provider = factory.create(symbol, stock_config)
        providers.append(provider)
        logger.debug(f"Created provider for symbol: {symbol}")

    logger.info(f"Created {len(providers)} data providers")
    return providers


async def load_data(
    dp: ProviderDataPipeline, n_days_before: Optional[int] = None
) -> Optional[pd.DataFrame]:
    logger.info(f"Loading data for symbol: {dp.data_provider.symbol}")
    try:
        if n_days_before is not None:
            end_date = datetime.now(tz=pytz.UTC)
            start_date = end_date - timedelta(days=n_days_before)
            logger.debug(f"Loading data from {start_date} to {end_date}")
            data = await dp.get_data_range(start_date, end_date)
        else:
            logger.debug("Loading all available data")
            data = await dp.get_all_data()

        if data.empty:
            logger.warning(f"No data found for symbol: {dp.data_provider.symbol}")
            return None
        logger.info(f"Successfully loaded data for symbol: {dp.data_provider.symbol}")
        return data
    except Exception as e:
        logger.error(f"Error loading data for symbol {dp.data_provider.symbol}: {e}")
        return None


async def process_data(
    dp: ProviderDataPipeline,
    n_days_before: Optional[int] = None,
    read_mode: bool = False,
) -> Optional[Dict[str, pd.DataFrame]]:
    symbol = dp.data_provider.symbol
    logger.info(f"Processing data for symbol: {symbol}")
    try:
        if not read_mode:
            await dp.update_to_latest()
        data = await load_data(dp, n_days_before)
        if data is not None:
            logger.info(f"Loaded data for {symbol}:")
            logger.info(f"Shape: {data.shape}")
            logger.info(f"Date range: {data.index.min()} to {data.index.max()}")
            # Additional data processing logic can be implemented here.
        return {symbol: data}
    except Exception as e:
        logger.error(f"Error processing data for symbol {symbol}: {e}")
        return None


async def create_pipelines(config: Dict[str, Any]) -> List[ProviderDataPipeline]:
    # FIXME 이 부분부터 전부 변경해야 함...
    # FIXME 투웰브 데이터 포함해서 진행하든지...
    logger.info("Creating data pipelines")
    providers = await create_data_providers(config)
    data_pipelines_config = config[CONFIG_KEY_DATA_PIPELINES]
    base_path = data_pipelines_config[CONFIG_KEY_BASE_PATH]
    storage_type = data_pipelines_config.get("storage_type", "local")
    bucket_name = data_pipelines_config.get("bucket_name")

    pipelines = []
    for provider in providers:
        symbol_base_path = os.path.join(base_path, provider.symbol)
        pipeline = ProviderDataPipeline(
            data_provider=provider,
            base_path=symbol_base_path,
            storage_type=storage_type,
            bucket_name=bucket_name,
        )
        pipelines.append(pipeline)
        logger.debug(f"Created pipeline for symbol: {provider.symbol}")

    logger.info(f"Created {len(pipelines)} data pipelines")
    return pipelines


async def parallel_process(
    func: Callable,
    items: List[Any],
    n_days_before: Optional[int] = None,
    read_mode: bool = False,
) -> List[Dict[str, pd.DataFrame]]:
    logger.info("Starting parallel processing")
    results = []
    tasks = [asyncio.create_task(func(item, n_days_before, read_mode=read_mode)) for item in items]

    completed, _ = await asyncio.wait(tasks)

    for task in completed:
        try:
            result = task.result()
            if result is not None:
                results.append(result)
        except Exception as e:
            logger.error(f"An error occurred during parallel processing: {e}")

    logger.info("All data processing completed.")
    logger.info(f"Successfully processed {len(results)} items.")

    return results


async def run_data_pipeline(config: Dict[str, Any]):
    pipelines: List[ProviderDataPipeline] = await create_pipelines(config)

    stop_event = asyncio.Event()

    try:
        # Continuous update (including initial fetch)
        logger.info("Starting data update for all stocks")
        update_tasks = [
            asyncio.create_task(pipeline.fetch_and_save_realtime(stop_event))
            for pipeline in pipelines
        ]

        # Wait for all tasks to complete or for an interruption
        done, pending = await asyncio.wait(
            update_tasks, return_when=asyncio.FIRST_EXCEPTION
        )

        # Check if any task raised an exception
        for task in done:
            if task.exception():
                raise task.exception()

    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt. Stopping all tasks...")
    except Exception as e:
        logger.error(
            f"An error occurred during data pipeline execution: {e}", exc_info=True
        )
        raise
    finally:
        # Stop all tasks
        stop_event.set()
        # Wait for all tasks to complete
        if "update_tasks" in locals():
            await asyncio.gather(*update_tasks, return_exceptions=True)
        # Close all pipelines
        close_tasks = [pipeline.close() for pipeline in pipelines]
        await asyncio.gather(*close_tasks, return_exceptions=True)
        logger.info("All data pipelines closed")


async def create_strategy(config: Dict[str, Any]):
    strategy_config = config.get(CONFIG_KEY_STRATEGY, {})
    algorithm_config = config.get(CONFIG_KEY_ALGORITHM, {})

    if not strategy_config:
        logger.warning("No strategy configuration found")
        return None

    strategy_name = strategy_config.get("name")
    if not strategy_name:
        logger.error("Strategy name is not specified in the configuration")
        return None

    try:
        strategy_class = await load_module(config, CONFIG_KEY_STRATEGY)
    except (ImportError, AttributeError) as e:
        logger.error(f"Failed to import strategy {strategy_name}: {e}")
        return None

    pipelines = await create_pipelines(config)

    algorithm = None
    if algorithm_config:
        algorithm_name = algorithm_config.get("name")
        if algorithm_name:
            try:
                algorithm_class = await load_module(config, CONFIG_KEY_ALGORITHM)
                algorithm = algorithm_class(**algorithm_config)
            except (ImportError, AttributeError) as e:
                logger.error(f"Failed to import algorithm {algorithm_name}: {e}")

    strategy_params = {k: v for k, v in strategy_config.items() if k != "name"}
    strategy_params["data_pipelines"] = pipelines
    if algorithm:
        strategy_params["algorithm"] = algorithm

    return strategy_class(**strategy_params)


def process_dataframe(
    data: Dict[str, Optional[pd.DataFrame]], value: str
) -> List[Optional[pd.Series]]:
    results = []
    for k, df in data.items():
        if df is None or df.empty or value not in df.columns:
            logger.warning(
                f"Invalid data for {k}: {'None' if df is None else 'Empty' if df.empty else f'No {value} column'}"
            )
            continue
        value_series = df[value]
        if value_series.empty:
            logger.warning(f"'{value}' price data is empty for {k}")
            continue
        logger.info(f"{k}: shape {value_series.shape}")
        results.append(value_series.rename(k))
    return results


def process_wrapper(args):
    return process_dataframe(*args)


def prepare_data(
    dp_result: List[Dict[str, Optional[pd.DataFrame]]],
    freq: str = "1D",
    value: str = "close",
) -> pd.DataFrame:
    logger.info(f"Preparing data for strategy execution with frequency: {freq}")
    start_time = time.time()

    try:
        with ProcessPoolExecutor() as executor:
            aggregated_data = list(
                executor.map(process_wrapper, [(data, value) for data in dp_result])
            )

        aggregated_data = [
            item for sublist in aggregated_data for item in sublist if item is not None
        ]

        if not aggregated_data:
            logger.error("No valid data to process")
            return pd.DataFrame()

        all_data = reduce(
            lambda left, right: pd.merge(
                left, right, left_index=True, right_index=True, how="outer"
            ),
            aggregated_data,
        )

        if not isinstance(all_data.index, pd.DatetimeIndex):
            all_data.index = pd.to_datetime(all_data.index)
            logger.info("Converted index to datetime format")

        try:
            all_data = all_data.resample(freq).last().bfill().ffill().sort_index()
        except ValueError as ve:
            logger.error(f"Invalid frequency '{freq}' provided: {ve}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error during resampling: {e}")
            return pd.DataFrame()

        end_time = time.time()
        logger.info(
            f"Data preparation completed in {end_time - start_time:.2f} seconds"
        )

        return all_data

    except Exception as e:
        logger.error(f"Unexpected error in data preparation: {e}", exc_info=True)
        return pd.DataFrame()


def create_symbol_mapper(configs: List[Dict]) -> Dict[str, str]:
    symbol_mapper = {}
    for config in configs:
        if "data_pipelines" in config and "stocks" in config["data_pipelines"]:
            data_info = config["data_pipelines"]["stocks"]
            for d in data_info:
                if "symbol" in d and "full_name" in d:
                    symbol_mapper[d["symbol"]] = d["full_name"]
    return symbol_mapper
