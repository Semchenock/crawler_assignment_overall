import asyncio
from dataclasses import asdict

import aiofiles
import json

from src.data_storage.base import BaseDataStorage
from src.data_storage.model import DataItem

class JSONStorage(BaseDataStorage):
    def __init__(self, file_path:str="data.jsonl"):
        super().__init__()
        self.file_path = file_path
        self._lock = asyncio.Lock()
        self.file = None

    async def _init_file(self):
        if self.file is not None:
            return

        self.file = await aiofiles.open(self.file_path, "a")

    async def _save_raw(self, data: DataItem):
        await self._init_file()
        async with self._lock:
            data_dict = asdict(data)
            line = json.dumps(data_dict, ensure_ascii=False, default=str)
            await self.file.write(line + "\n")

    async def save(self, data: DataItem):
        await self._run_with_retry(data, self._save_raw, data)

    async def close(self):
        if self.file is not None:
            await self.file.close()
