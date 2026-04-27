import asyncio
from dataclasses import asdict

import aiofiles
import csv
from io import StringIO

from src.data_storage.base import BaseDataStorage
from src.data_storage.model import DataItem

class CSVStorage(BaseDataStorage):
    def __init__(self, file_path:str="data.csv", encoding="utf-8"):
        super().__init__()
        self.file_path = file_path
        self.encoding = encoding
        self._lock = asyncio.Lock()
        self.file = None
        self.headers = None

    async def _init_file(self):
        if self.file is not None:
            return

        self.file = await aiofiles.open(
            self.file_path, "a", encoding=self.encoding
        )

    async def _init_headers(self, data: DataItem):
        if self.headers is not None:
            return

        data_dict = asdict(data)

        self.headers = list(data_dict.keys())

        header_buffer = StringIO()
        writer = csv.DictWriter(header_buffer, fieldnames=self.headers)
        writer.writeheader()

        line = header_buffer.getvalue()
        await self.file.write(line)


    def _dict_to_csv_line(self, data: DataItem) -> str:
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=self.headers)
        data_dict = asdict(data)

        writer.writerow(data_dict)
        return buffer.getvalue()

    async def _save_raw(self, data: DataItem):
        async with self._lock:
            await self._init_file()
            await self._init_headers(data)

            data_dict = asdict(data)

            if set(data_dict.keys()) != set(self.headers):
                raise ValueError("Inconsistent data schema")

            line = self._dict_to_csv_line(data)
            await self.file.write(line)

    async def save(self, data: DataItem):
        await self._run_with_retry(data, self._save_raw, data=data)

    async def close(self):
        if self.file:
            await self.file.close()
