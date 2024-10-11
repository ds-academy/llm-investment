import os
import aiofiles
import asyncio
from contextlib import asynccontextmanager


class AsyncFileLock:
    def __init__(self, lock_file: str):
        self.lock_file = lock_file
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self):
        max_attempts = 10
        for attempt in range(max_attempts):
            async with self._lock:
                if not os.path.exists(self.lock_file):
                    async with aiofiles.open(self.lock_file, "w") as f:
                        await f.write("lock")
                    try:
                        yield
                    finally:
                        os.remove(self.lock_file)
                    return
            await asyncio.sleep(0.5)  # 대기 시간을 늘립니다
        raise TimeoutError(
            f"Could not acquire lock for {self.lock_file} after {max_attempts} attempts"
        )
