import sqlite3
from collection import Collection


class Database:
    def __init__(self, dbfile: str) -> None:
        self.conn = sqlite3.connect(dbfile)

    def collection(self, name: str) -> Collection:
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {name} (id TEXT, data JSON)")
        return Collection(self.conn, name)

    def close(self):
        self.conn.close()
