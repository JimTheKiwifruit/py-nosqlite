import json
from pprint import pprint
from sqlite3 import Connection
from shortuuid import uuid


class Collection:
    def __init__(self, conn: Connection, table_name: str):
        self.conn = conn
        self.table_name = table_name

    def insert_one(self, data) -> str:
        cur = self.conn.cursor()
        id = uuid()
        cur.execute(
            f"INSERT INTO {self.table_name} (id, data) VALUES (?, json(?))", (id, json.dumps(data)))
        self.conn.commit()
        cur.close()
        return id

    def find_by_id(self, id):
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT (data) FROM {self.table_name} WHERE id = ?", (id,))
        d = cur.fetchone()
        cur.close()
        return json.loads(d[0])

    def find(self, kv: dict):
        if len(kv) == 0:
            return None

        cur = self.conn.cursor()
        # q = f"SELECT json_extract(data, '$.name') as name, json_extract(data, '$.age') as age FROM {self.table_name} where age=302"
        # q = f"SELECT value FROM {self.table_name}, json_each({self.table_name}.data) where "

        queries = ", ".join(
            [f"json_extract(data, '$.{i}') AS {i}" for i in kv.keys()])
        search = " AND ".join(
            [f"{i[0]} = {self.__quote(i[1])}" for i in kv.items()])
        # print("Search:", search)
        # q = f"SELECT * FROM {self.table_name}, json_each({self.table_name}.data) WHERE {search}"
        q = f"SELECT id, data, {queries} FROM {self.table_name} WHERE {search}"
        # print(q)
        cur.execute(q)
        d = cur.fetchall()
        # print(pprint(d))
        cur.close()
        return {s[0]: json.loads(s[1]) for s in d}

    def update_by_id(self, id, data, upsert=False):
        cur = self.conn.cursor()
        cur.execute(
            f"SELECT (data) FROM {self.table_name} WHERE id = ?", (id,))
        if cur.rowcount == 0:
            if upsert:
                cur.execute(
                    f"INSERT INTO {self.table_name} (id, data) VALUES (?, json(?))", (id, json.dumps(data)))
                cur.close()
                return cur.rowcount
            else:
                cur.close()
                return None

        original = cur.fetchone()[0]
        cur.execute(f"UPDATE {self.table_name} SET data = json_patch(?, json(?)) WHERE id = ?", (
            original, json.dumps(data), id))
        self.conn.commit()
        cur.close()
        return cur.rowcount

    def __quote(self, val):
        if isinstance(val, str):
            return f"'{val}'"
        return val
