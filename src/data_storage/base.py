from abc import ABC, abstractmethod
from typing import Callable, Awaitable, Any

from src.data_storage.model import DataItem, ErrorLogEntity


class BaseDataStorage(ABC):
    def __init__(self):
        self.error_log: list[ErrorLogEntity] = []
        self.max_retries: int = 3

    def _append_error(self, error: Exception, data: DataItem, try_count: int) -> None:
        self.error_log.append(ErrorLogEntity(data = data, error = error, try_count=try_count))

    async def _run_with_retry(self, data, coro: Callable[..., Awaitable[Any]], *args, **kwargs):
        for i in range(1, self.max_retries+1):
            try:
                return await coro(*args, **kwargs)
            except Exception as e:
                self._append_error(error=e, data=data, try_count=i)

        return None


    @abstractmethod
    async def save(self, data: DataItem):
        raise NotImplementedError
    @abstractmethod
    async def close(self):
        raise NotImplementedError
