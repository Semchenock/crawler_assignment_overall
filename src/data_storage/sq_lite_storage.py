import aiosqlite
import json
from datetime import datetime
from dataclasses import fields, asdict

from src.data_storage.model import DataItem
from src.data_storage.base import BaseDataStorage


class SQLiteStorage(BaseDataStorage):
    def __init__(self, db_path:str="data.db", batch_size=100):
        super().__init__()
        self.db_path = db_path
        self.batch_size = batch_size
        self.db = None
        self.buffer: list[DataItem] = []

    @staticmethod
    def _python_to_sqlite_type(py_type):
        if py_type in (str, datetime):
            return "TEXT"
        elif py_type is int:
            return "INTEGER"
        elif py_type is float:
            return "REAL"
        else:
            return "TEXT"


    def _generate_schema(self, model):
        cols = []
        for f in fields(model):
            col_type = self._python_to_sqlite_type(f.type)
            cols.append(f"{f.name} {col_type}")
        return ", ".join(cols)

    async def _init_db(self):
        if self.db is not None:
            return

        self.db = await aiosqlite.connect(self.db_path)

        schema = self._generate_schema(DataItem)

        await self.db.execute(f"""
            CREATE TABLE IF NOT EXISTS data (
                {schema}
            )
        """)

        await self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_url ON data(url)
        """)

        await self.db.commit()

    @staticmethod
    def _serialize_value(value):
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, (list, dict)):
            return json.dumps(value)
        return value


    async def _flush(self):
        if not self.buffer:
            return

        data_dict = asdict(self.buffer[0])

        keys = data_dict.keys()
        columns = ", ".join(keys)
        placeholders = ", ".join(["?"] * len(keys))

        values = [
            tuple(self._serialize_value(asdict(d)[k]) for k in keys)
            for d in self.buffer
        ]

        await self.db.executemany(
            f"INSERT INTO data ({columns}) VALUES ({placeholders})",
            values
        )

        await self.db.commit()
        self.buffer.clear()

    async def _save_raw(self, data: DataItem):
        await self._init_db()
        self.buffer.append(data)

        if len(self.buffer) >= self.batch_size:
            await self._flush()

    async def save(self, data: DataItem):
        await self._run_with_retry(data, self._save_raw, data)

    async def close(self):
        await self._flush()
        if self.db:
            await self.db.close()
