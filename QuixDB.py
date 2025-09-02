import os
import json
import uuid
import time
import hashlib
import threading
import fnmatch
from typing import Dict, Any, Optional, List, Callable, Set, Union
from dataclasses import dataclass
from functools import lru_cache

# --- Configuration ---
DEFAULT_SHARDS = 8
CACHE_SIZE = 1000
FSYNC_ENABLED = True  # Can be disabled for better performance when reliability isn't critical

# --- Helpers ---
def _fsync(fd_or_path):
    if not FSYNC_ENABLED:
        return
        
    if isinstance(fd_or_path, int):
        os.fsync(fd_or_path)
    else:
        with open(fd_or_path, "rb") as f:
            os.fsync(f.fileno())

def _atomic_write_json(path: str, data):
    tmp = path + f".tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        if FSYNC_ENABLED:
            os.fsync(f.fileno())
    os.replace(tmp, path)

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _hash_to_shard(key: str, shards: int) -> int:
    # Use a faster hash function for sharding
    h = hashlib.md5(key.encode("utf-8")).digest()
    return int.from_bytes(h, "big") % shards

def _now_ts():
    return time.time()

# --- Table class ---
class Table:
    def __init__(self, table_path: str, name: str, schema: Dict[str, str], shards: int = DEFAULT_SHARDS, 
                 unique: Optional[List[str]] = None, cache_size: int = CACHE_SIZE):
        self.table_path = table_path
        self.name = name
        self.schema = schema
        self.shards = max(1, int(shards))
        self.unique = unique or []
        self.shards_path = os.path.join(self.table_path, "shards")
        _ensure_dir(self.shards_path)

        # Create shard files if they don't exist
        for i in range(self.shards):
            p = os.path.join(self.shards_path, f"shard_{i}.ndjson")
            if not os.path.exists(p):
                with open(p, "a", encoding="utf-8") as f:
                    if FSYNC_ENABLED:
                        f.flush()
                        os.fsync(f.fileno())

        self.meta_path = os.path.join(self.table_path, "schema.json")
        _atomic_write_json(self.meta_path, {
            "name": self.name,
            "schema": self.schema,
            "shards": self.shards,
            "unique": self.unique,
        })

        # Cache initialization
        self._cache = {}
        self._unique_index = {col: {} for col in self.unique}  # Index for unique constraints
        self._cache_mtimes = None
        self._cache_loaded_at = 0
        self._lock = threading.RLock()
        
        # Preload cache
        self._ensure_cache()

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
            with self._lock:
                if self._cache_mtimes != current:  # Double-check locking
                    self._cache = {}
                    self._unique_index = {col: {} for col in self.unique}
                    self._cache_mtimes = current

    def _load_all(self):
        records = {}
        unique_index = {col: {} for col in self.unique}
        
        for path in self._shard_files():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                            
                        op = obj.get("__op__")
                        rid = obj.get("id")
                        if not rid:
                            continue
                            
                        if op == "insert":
                            records[rid] = obj["data"]
                            records[rid]["__meta__"] = {"created": obj.get("__ts__", 0)}
                            
                            # Update unique index
                            for col in self.unique:
                                val = records[rid].get(col, "")
                                if val in unique_index[col]:
                                    # Conflict detected - keep the latest one
                                    if obj.get("__ts__", 0) > records[unique_index[col][val]].get("__meta__", {}).get("created", 0):
                                        del records[unique_index[col][val]]
                                        unique_index[col][val] = rid
                                    else:
                                        del records[rid]
                                        break
                                else:
                                    unique_index[col][val] = rid
                                    
                        elif op == "update" and rid in records:
                            # Check unique constraints before update
                            can_update = True
                            for col in self.unique:
                                new_val = obj["data"].get(col)
                                if new_val is not None and new_val != records[rid].get(col):
                                    if new_val in unique_index[col] and unique_index[col][new_val] != rid:
                                        can_update = False
                                        break
                            
                            if can_update:
                                # Remove old unique values from index
                                for col in self.unique:
                                    old_val = records[rid].get(col)
                                    if old_val in unique_index[col] and unique_index[col][old_val] == rid:
                                        del unique_index[col][old_val]
                                
                                # Update record
                                records[rid].update(obj["data"])
                                records[rid]["__meta__"]["updated"] = obj.get("__ts__", 0)
                                
                                # Add new unique values to index
                                for col in self.unique:
                                    new_val = records[rid].get(col, "")
                                    unique_index[col][new_val] = rid
                                    
                        elif op == "delete" and rid in records:
                            # Remove from unique index
                            for col in self.unique:
                                val = records[rid].get(col, "")
                                if val in unique_index[col] and unique_index[col][val] == rid:
                                    del unique_index[col][val]
                            del records[rid]
            except FileNotFoundError:
                continue
                
        return records, unique_index

    def _ensure_cache(self):
        self._invalidate_cache_if_needed()
        if not self._cache:
            with self._lock:
                if not self._cache:  # Double-check locking
                    self._cache, self._unique_index = self._load_all()
                    self._cache_mtimes = self._shard_mtimes()
                    self._cache_loaded_at = _now_ts()

    def _check_unique_constraints(self, data: Dict[str, Any], exclude_id: str = None) -> bool:
        """Check if data violates unique constraints"""
        for col in self.unique:
            val = data.get(col)
            if val is not None and val in self._unique_index[col]:
                if exclude_id is None or self._unique_index[col][val] != exclude_id:
                    return False
        return True

    # --- Public API ---
    def insert(self, data: Dict[str, Any]) -> Optional[str]:
        row = {k: str(data.get(k, "")) for k in self.schema.keys()}
        
        with self._lock:
            self._ensure_cache()
            
            # Check unique constraints
            if not self._check_unique_constraints(row):
                return None
                
            rid = uuid.uuid4().hex
            entry = {"__op__": "insert", "id": rid, "data": row, "__ts__": _now_ts()}
            shard_path = self._shard_file_for_record(rid)
            
            try:
                with open(shard_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                    f.flush()
                    if FSYNC_ENABLED:
                        os.fsync(f.fileno())
            except IOError:
                return None
                
            # Update cache
            self._cache[rid] = row
            self._cache[rid]["__meta__"] = {"created": entry["__ts__"]}
            
            # Update unique index
            for col in self.unique:
                val = row.get(col, "")
                self._unique_index[col][val] = rid
                
            # Update mtime for the shard
            current_mtimes = list(self._cache_mtimes)
            shard_idx = _hash_to_shard(rid, self.shards)
            current_mtimes[shard_idx] = os.path.getmtime(shard_path)
            self._cache_mtimes = tuple(current_mtimes)
            
            return rid

    def select(self, where: Optional[Dict[str, Any]] = None, fields: Optional[List[str]] = None, 
               limit: Optional[int] = None) -> List[Dict[str, Any]]:
        self._ensure_cache()
        
        if isinstance(where, dict):
            def where_fn(r): 
                return all(str(r.get(k, "")) == str(v) for k, v in where.items())
        elif callable(where):
            where_fn = where
        else:
            where_fn = lambda r: True

        out = []
        for rid, row in self._cache.items():
            if where_fn(row):
                result = {f: row.get(f) for f in fields} if fields else row.copy()
                if "__meta__" in result:
                    del result["__meta__"]
                out.append(result)
                if limit and len(out) >= limit:
                    break
        return out

    def update(self, where: Optional[Dict[str, Any]] = None, data: Dict[str, Any] = None) -> int:
        if not data:
            return 0
            
        with self._lock:
            self._ensure_cache()
            
            # Find records to update
            to_update = []
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
                    to_update.append(rid)

            # Check unique constraints for all updates first
            for rid in to_update:
                updated_data = self._cache[rid].copy()
                updated_data.update({k: str(v) for k, v in data.items()})
                if not self._check_unique_constraints(updated_data, exclude_id=rid):
                    return 0

            # Perform updates
            count = 0
            for rid in to_update:
                entry = {"__op__": "update", "id": rid, "data": {k: str(v) for k, v in data.items()}, "__ts__": _now_ts()}
                shard_path = self._shard_file_for_record(rid)
                
                try:
                    with open(shard_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        f.flush()
                        if FSYNC_ENABLED:
                            os.fsync(f.fileno())
                except IOError:
                    continue
                    
                # Update cache
                old_row = self._cache[rid]
                for col in self.unique:
                    old_val = old_row.get(col, "")
                    if old_val in self._unique_index[col] and self._unique_index[col][old_val] == rid:
                        del self._unique_index[col][old_val]
                
                old_row.update({k: str(v) for k, v in data.items()})
                old_row["__meta__"]["updated"] = entry["__ts__"]
                
                for col in self.unique:
                    new_val = old_row.get(col, "")
                    self._unique_index[col][new_val] = rid
                
                count += 1
                
                # Update mtime for the shard
                current_mtimes = list(self._cache_mtimes)
                shard_idx = _hash_to_shard(rid, self.shards)
                current_mtimes[shard_idx] = os.path.getmtime(shard_path)
                self._cache_mtimes = tuple(current_mtimes)
                
            return count

    def delete(self, where: Optional[Dict[str, Any]] = None) -> int:
        with self._lock:
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
                
                try:
                    with open(shard_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        f.flush()
                        if FSYNC_ENABLED:
                            os.fsync(f.fileno())
                except IOError:
                    continue
                    
                # Update cache and index
                row = self._cache[rid]
                for col in self.unique:
                    val = row.get(col, "")
                    if val in self._unique_index[col] and self._unique_index[col][val] == rid:
                        del self._unique_index[col][val]
                del self._cache[rid]
                
                count += 1
                
                # Update mtime for the shard
                current_mtimes = list(self._cache_mtimes)
                shard_idx = _hash_to_shard(rid, self.shards)
                current_mtimes[shard_idx] = os.path.getmtime(shard_path)
                self._cache_mtimes = tuple(current_mtimes)
                
            return count

    def bulk_insert(self, data_list: List[Dict[str, Any]]) -> List[str]:
        """Insert multiple records at once for better performance"""
        with self._lock:
            self._ensure_cache()
            
            results = []
            shard_entries = {}
            
            # Prepare all entries
            for data in data_list:
                row = {k: str(data.get(k, "")) for k in self.schema.keys()}
                
                # Check unique constraints
                if not self._check_unique_constraints(row):
                    results.append(None)
                    continue
                    
                rid = uuid.uuid4().hex
                entry = {"__op__": "insert", "id": rid, "data": row, "__ts__": _now_ts()}
                shard_path = self._shard_file_for_record(rid)
                
                if shard_path not in shard_entries:
                    shard_entries[shard_path] = []
                shard_entries[shard_path].append(entry)
                
                # Update cache
                self._cache[rid] = row
                self._cache[rid]["__meta__"] = {"created": entry["__ts__"]}
                
                # Update unique index
                for col in self.unique:
                    val = row.get(col, "")
                    self._unique_index[col][val] = rid
                
                results.append(rid)
            
            # Write to shard files
            updated_shards = set()
            for shard_path, entries in shard_entries.items():
                try:
                    with open(shard_path, "a", encoding="utf-8") as f:
                        for entry in entries:
                            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                        f.flush()
                        if FSYNC_ENABLED:
                            os.fsync(f.fileno())
                    updated_shards.add(shard_path)
                except IOError:
                    # Remove failed entries from cache
                    for entry in entries:
                        if entry["id"] in self._cache:
                            row = self._cache[entry["id"]]
                            for col in self.unique:
                                val = row.get(col, "")
                                if val in self._unique_index[col] and self._unique_index[col][val] == entry["id"]:
                                    del self._unique_index[col][val]
                            del self._cache[entry["id"]]
                    
                    # Mark these as failed
                    for i, rid in enumerate(results):
                        if rid == entry["id"]:
                            results[i] = None
            
            # Update mtimes for updated shards
            if updated_shards:
                current_mtimes = list(self._cache_mtimes)
                for i in range(self.shards):
                    shard_path = os.path.join(self.shards_path, f"shard_{i}.ndjson")
                    if shard_path in updated_shards:
                        current_mtimes[i] = os.path.getmtime(shard_path)
                self._cache_mtimes = tuple(current_mtimes)
            
            return results

# --- Database root ---
class QuixDB:
    _instances = {}
    _lock = threading.RLock()
    
    def __new__(cls, folder: str, default_shards: int = DEFAULT_SHARDS):
        folder = os.path.abspath(folder)
        with cls._lock:
            if folder not in cls._instances:
                instance = super().__new__(cls)
                instance.__init__(folder, default_shards)
                cls._instances[folder] = instance
            return cls._instances[folder]
    
    def __init__(self, folder: str, default_shards: int = DEFAULT_SHARDS):
        if hasattr(self, '_initialized'):
            return
            
        self.folder = os.path.abspath(folder)
        _ensure_dir(self.folder)
        self.default_shards = default_shards
        self._tables: Dict[str, Table] = {}
        self._lock = threading.RLock()
        self._load_existing_tables()
        self._initialized = True

    def _load_existing_tables(self):
        with self._lock:
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
        with self._lock:
            if name in self._tables:
                return
            tpath = os.path.join(self.folder, name)
            _ensure_dir(tpath)
            schema = {k: "str" for k in columns.keys()}
            s = shards if shards is not None else self.default_shards
            tbl = Table(tpath, name, schema, shards=s, unique=unique)
            self._tables[name] = tbl

    def table(self, name: str) -> Table:
        with self._lock:
            if name not in self._tables:
                raise KeyError(f"Table {name} not found")
            return self._tables[name]

    # convenience methods
    def insert(self, table: str, data: Dict[str, Any]) -> Optional[str]:
        return self.table(table).insert(data)

    def select(self, table: str, where: Optional[Dict[str, Any]] = None, fields: Optional[List[str]] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        return self.table(table).select(where=where, fields=fields, limit=limit)

    def update(self, table: str, where: Optional[Dict[str, Any]] = None, data: Optional[Dict[str, Any]] = None) -> int:
        return self.table(table).update(where=where, data=data)

    def delete(self, table: str, where: Optional[Dict[str, Any]] = None) -> int:
        return self.table(table).delete(where=where)
        
    def bulk_insert(self, table: str, data_list: List[Dict[str, Any]]) -> List[str]:
        return self.table(table).bulk_insert(data_list)

# --- Connection helper ---
def connect(folder: str, default_shards: int = DEFAULT_SHARDS) -> QuixDB:
    return QuixDB(folder, default_shards=default_shards)
