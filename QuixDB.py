"""
QuixDB - simple file-backed sharded append-only DB for Python.

Features:
- Folder-based database. Each table = subfolder with schema + shards.
- Append-only shard files (NDJSON) reduce corruption risk.
- Atomic writes with fsync, atomic metadata updates.
- Shards for concurrent-friendly writes.
- Cache with mtime invalidation for faster reads.
- Unique constraints (e.g., unique=["Email"]) to prevent duplicate data.
"""

import os
import json
import uuid
import time
import hashlib
from typing import Dict, Any, Optional, List, Callable

# --- Helpers ---
def _fsync(fd_or_path):
    if isinstance(fd_or_path, int):
        os.fsync(fd_or_path)
    else:
        with open(fd_or_path, "rb") as f:
            os.fsync(f.fileno())

def _atomic_write_json(path: str, data):
    tmp = path + ".tmp.%d" % os.getpid()
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _hash_to_shard(key: str, shards: int) -> int:
    h = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(h, "big") % shards

def _now_ts():
    return time.time()

# --- Table class ---
class Table:
    def __init__(self, table_path: str, name: str, schema: Dict[str, str], shards: int = 8, unique: Optional[List[str]] = None):
        self.table_path = table_path
        self.name = name
        self.schema = schema
        self.shards = max(1, int(shards))
        self.unique = unique or []
        self.shards_path = os.path.join(self.table_path, "shards")
        _ensure_dir(self.shards_path)

        for i in range(self.shards):
            p = os.path.join(self.shards_path, f"shard_{i}.ndjson")
            if not os.path.exists(p):
                open(p, "a", encoding="utf-8").close()

        self.meta_path = os.path.join(self.table_path, "schema.json")
        _atomic_write_json(self.meta_path, {
            "name": self.name,
            "schema": self.schema,
            "shards": self.shards,
            "unique": self.unique,
        })

        self._cache = None
        self._cache_mtimes = None
        self._cache_loaded_at = 0

    # --- Internals ---
    def _shard_files(self) -> List[str]:
        return [os.path.join(self.shards_path, f"shard_{i}.ndjson") for i in range(self.shards)]

    def _shard_file_for_record(self, rec_id: str) -> str:
        idx = _hash_to_shard(rec_id, self.shards)
        return os.path.join(self.shards_path, f"shard_{idx}.ndjson")

    def _shard_mtimes(self) -> List[float]:
        return [os.path.getmtime(p) for p in self._shard_files()]

    def _invalidate_cache_if_needed(self):
        current = self._shard_mtimes()
        if self._cache_mtimes != current:
            self._cache = None
            self._cache_mtimes = current

    def _load_all(self):
        records = {}
        for path in self._shard_files():
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    op = obj.get("__op__")
                    rid = obj.get("id")
                    if not rid:
                        continue
                    if op == "insert":
                        records[rid] = obj["data"]
                        records[rid]["__meta__"] = {"created": obj.get("__ts__", 0)}
                    elif op == "update" and rid in records:
                        records[rid].update(obj["data"])
                        records[rid]["__meta__"]["updated"] = obj.get("__ts__", 0)
                    elif op == "delete" and rid in records:
                        del records[rid]
        return records

    def _ensure_cache(self):
        self._invalidate_cache_if_needed()
        if self._cache is None:
            self._cache = self._load_all()
            self._cache_mtimes = self._shard_mtimes()
            self._cache_loaded_at = _now_ts()

    # --- Public API ---
    def insert(self, data: Dict[str, Any]) -> Optional[str]:
        row = {k: str(data.get(k, "")) for k in self.schema.keys()}

        # --- UNIQUE CHECK ---
        if self.unique:
            self._ensure_cache()
            for col in self.unique:
                val = row.get(col, "")
                for existing in self._cache.values():
                    if existing.get(col, "") == val:
                        return None  # reject duplicate
        # -------------------

        rid = uuid.uuid4().hex
        entry = {"__op__": "insert", "id": rid, "data": row, "__ts__": _now_ts()}
        shard_path = self._shard_file_for_record(rid)
        with open(shard_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

        if self._cache is not None:
            self._cache[rid] = row
            self._cache[rid]["__meta__"] = {"created": entry["__ts__"]}
            self._cache_mtimes = self._shard_mtimes()
        return rid

    def select(self, where: Optional[Dict[str, Any]] = None, fields: Optional[List[str]] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        if isinstance(where, dict):
            def where_fn(r): return all(str(r.get(k, "")) == str(v) for k, v in where.items())
        elif callable(where):
            where_fn = where
        else:
            where_fn = lambda r: True

        self._ensure_cache()
        out = []
        for rid, row in self._cache.items():
            if where_fn(row):
                result = {f: row.get(f) for f in fields} if fields else {k: v for k, v in row.items() if k != "__meta__"}
                out.append(result)
                if limit and len(out) >= limit:
                    break
        return out

    def update(self, where: Optional[Dict[str, Any]] = None, data: Dict[str, Any] = None) -> int:
        if not data:
            return 0
        self._ensure_cache()
        to_update_ids = []
        for rid, row in self._cache.items():
            ok = True
            if isinstance(where, dict):
                for k, v in where.items():
                    if str(row.get(k, "")) != str(v):
                        ok = False
                        break
            elif callable(where):
                ok = where(row)
            if ok:
                to_update_ids.append(rid)

        # enforce unique on update
        if self.unique:
            for col in self.unique:
                new_val = data.get(col)
                if new_val is None:
                    continue
                for rid, row in self._cache.items():
                    if str(row.get(col, "")) == str(new_val) and rid not in to_update_ids:
                        return 0  # reject update that violates unique

        count = 0
        for rid in to_update_ids:
            entry = {"__op__": "update", "id": rid, "data": {k: str(v) for k, v in data.items()}, "__ts__": _now_ts()}
            shard_path = self._shard_file_for_record(rid)
            with open(shard_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            count += 1

        self._cache = None
        self._cache_mtimes = self._shard_mtimes()
        return count

    def delete(self, where: Optional[Dict[str, Any]] = None) -> int:
        self._ensure_cache()
        to_delete = []
        for rid, row in self._cache.items():
            ok = True
            if isinstance(where, dict):
                for k, v in where.items():
                    if str(row.get(k, "")) != str(v):
                        ok = False
                        break
            elif callable(where):
                ok = where(row)
            if ok:
                to_delete.append(rid)

        count = 0
        for rid in to_delete:
            entry = {"__op__": "delete", "id": rid, "__ts__": _now_ts()}
            shard_path = self._shard_file_for_record(rid)
            with open(shard_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            count += 1

        self._cache = None
        self._cache_mtimes = self._shard_mtimes()
        return count

# --- Database root ---
class QuixDB:
    def __init__(self, folder: str, default_shards: int = 8):
        self.folder = os.path.abspath(folder)
        _ensure_dir(self.folder)
        self.default_shards = default_shards
        self._tables: Dict[str, Table] = {}
        self._load_existing_tables()

    def _load_existing_tables(self):
        for name in os.listdir(self.folder):
            tp = os.path.join(self.folder, name)
            if os.path.isdir(tp):
                meta = os.path.join(tp, "schema.json")
                if os.path.exists(meta):
                    try:
                        with open(meta, "r", encoding="utf-8") as f:
                            info = json.load(f)
                        schema = info.get("schema", {})
                        shards = info.get("shards", self.default_shards)
                        unique = info.get("unique", [])
                        self._tables[name] = Table(tp, name, schema, shards=shards, unique=unique)
                    except Exception:
                        continue

    def create_table(self, name: str, columns: Dict[str, type], shards: Optional[int] = None, unique: Optional[List[str]] = None):
        if name in self._tables:
            return
        tpath = os.path.join(self.folder, name)
        _ensure_dir(tpath)
        schema = {k: "str" for k in columns.keys()}
        s = shards if shards is not None else self.default_shards
        tbl = Table(tpath, name, schema, shards=s, unique=unique)
        self._tables[name] = tbl

    def table(self, name: str) -> Table:
        if name not in self._tables:
            raise KeyError(f"Table {name} not found")
        return self._tables[name]

    # convenience
    def insert(self, table: str, data: Dict[str, Any]) -> Optional[str]:
        return self.table(table).insert(data)

    def select(self, table: str, where: Optional[Dict[str, Any]] = None, fields: Optional[List[str]] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        return self.table(table).select(where=where, fields=fields, limit=limit)

    def update(self, table: str, where: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None) -> int:
        return self.table(table).update(where=where, data=data)

    def delete(self, table: str, where: Optional[Dict[str, Any]] = None) -> int:
        return self.table(table).delete(where=where)

# --- Connection helper ---
def connect(folder: str, default_shards: int = 8) -> QuixDB:
    return QuixDB(folder, default_shards=default_shards)




































"""import os
import json
import time
import threading

class QuixDB:
    def __init__(self, db_folder):
        self.DB_FOLDER = db_folder

        self.UsersAndTables = {}  # Dictionary to hold users and their tables.

        # Create the database folder if it doesn't exist
        os.makedirs(self.DB_FOLDER, exist_ok=True)

    # Create User and Table if not exists
    def create_table(self, table_name, table):
        os.makedirs(os.path.join(self.DB_FOLDER, table_name), exist_ok=True)
        self.UsersAndTables[table_name] = table
        print(f"Table '{table_name}' created with structure: {table}")

        with open(f"{self.DB_FOLDER}{table_name}_structure.json", 'w') as f:
            json.dump(table, f)


    
    def insert(self, table_name, data):
        pass

    def select(self, table_name, where):
        pass

    def update(self, table_name, where, data):
        pass

    def delete(self, table_name, where):
        pass


def connect(db_folder):
    return QuixDB(db_folder)"""