import os
import io
import struct
import threading
import time
import platform

# Cross-platform file locking (shared/exclusive)
try:
    import fcntl  # POSIX
except ImportError:  # Windows fallback
    fcntl = None
    import msvcrt


class FileLock:
    """Advisory file lock with shared/exclusive modes.
    On Windows this emulates shared/exclusive by locking the whole file region.
    """

    def __init__(self, path: str):
        self.path = path
        self.fd = None
        self._lock = threading.RLock()

    def acquire(self, exclusive: bool):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.fd = open(self.path, 'a+b')
        if fcntl:
            flags = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
            fcntl.flock(self.fd.fileno(), flags)
        else:
            # Windows: lock the whole file
            # msvcrt.LK_NBLCK is non-blocking; we want blocking, so use LK_LOCK
            # Size: lock a large region
            size = 0x7FFFFFFF
            self.fd.seek(0)
            mode = msvcrt.LK_LOCK  # blocking lock
            if exclusive:
                mode = msvcrt.LK_LOCK
            else:
                mode = msvcrt.LK_LOCK  # Windows doesn't have shared locks; emulate with exclusive
            msvcrt.locking(self.fd.fileno(), mode, size)
        return self

    def release(self):
        if self.fd is None:
            return
        try:
            if fcntl:
                fcntl.flock(self.fd.fileno(), fcntl.LOCK_UN)
            else:
                # Unlock on Windows by relocking with zero size (no-op) then closing
                try:
                    self.fd.seek(0)
                    msvcrt.locking(self.fd.fileno(), msvcrt.LK_UNLCK, 0x7FFFFFFF)
                except Exception:
                    pass
        finally:
            try:
                self.fd.close()
            except Exception:
                pass
            self.fd = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()


# === Low-level binary row codec ===
# Record layout (little-endian):
# [1 byte tombstone (0=live,1=deleted)] [4 bytes payload_len]
# payload = sequence of column-encoded values in schema order
# Types: str -> [4B len][bytes]; int -> [8B signed]; float -> [8B double]; bytes -> [4B len][bytes]


_TYPE_CODES = {
    str: 's',
    int: 'i',
    float: 'f',
    bytes: 'b',
}


class Schema:
    def __init__(self, columns: dict, unique: list | None):
        # columns is an ordered mapping name->type; we will preserve insertion order
        self.columns = dict(columns)
        self.col_names = list(columns.keys())
        self.col_types = [_TYPE_CODES[columns[n]] for n in self.col_names]
        self.unique = list(unique) if unique else []
        for u in self.unique:
            if u not in self.columns:
                raise ValueError(f"Unique column '{u}' not in columns")

    def dumps(self) -> bytes:
        # Not JSON: a tiny binary schema header
        # magic b'QXSC' + version 1 + counts + repeated [name_len, name, type_code, unique_flag]
        buf = io.BytesIO()
        buf.write(b'QXSC')
        buf.write(struct.pack('<I', 1))
        buf.write(struct.pack('<I', len(self.col_names)))
        for name, tcode in zip(self.col_names, self.col_types):
            name_b = name.encode('utf-8')
            buf.write(struct.pack('<H', len(name_b)))
            buf.write(name_b)
            buf.write(tcode.encode('ascii'))
            buf.write(struct.pack('<?', name in self.unique))
        return buf.getvalue()

    @staticmethod
    def loads(b: bytes) -> 'Schema':
        bio = io.BytesIO(b)
        magic = bio.read(4)
        if magic != b'QXSC':
            raise ValueError('Invalid schema file')
        (version,) = struct.unpack('<I', bio.read(4))
        if version != 1:
            raise ValueError('Unsupported schema version')
        (ncols,) = struct.unpack('<I', bio.read(4))
        columns = {}
        unique = []
        for _ in range(ncols):
            (nlen,) = struct.unpack('<H', bio.read(2))
            name = bio.read(nlen).decode('utf-8')
            tcode = bio.read(1).decode('ascii')
            (is_unique,) = struct.unpack('<?', bio.read(1))
            pytype = {v: k for k, v in _TYPE_CODES.items()}[tcode]
            columns[name] = pytype
            if is_unique:
                unique.append(name)
        return Schema(columns, unique)


def _pack_value(tcode: str, value):
    if tcode == 's':
        b = value.encode('utf-8')
        return struct.pack('<I', len(b)) + b
    elif tcode == 'i':
        return struct.pack('<q', int(value))
    elif tcode == 'f':
        return struct.pack('<d', float(value))
    elif tcode == 'b':
        return struct.pack('<I', len(value)) + value
    else:
        raise ValueError('Unsupported type code')


def _unpack_value(tcode: str, bio: io.BytesIO):
    if tcode == 's':
        (n,) = struct.unpack('<I', bio.read(4))
        return bio.read(n).decode('utf-8')
    elif tcode == 'i':
        return struct.unpack('<q', bio.read(8))[0]
    elif tcode == 'f':
        return struct.unpack('<d', bio.read(8))[0]
    elif tcode == 'b':
        (n,) = struct.unpack('<I', bio.read(4))
        return bio.read(n)
    else:
        raise ValueError('Unsupported type code')


class Table:
    def __init__(self, root: str, name: str, schema: Schema):
        self.root = root
        self.name = name
        self.schema = schema
        self.dir = os.path.join(root, name)
        self.data_path = os.path.join(self.dir, 'data.dat')
        self.schema_path = os.path.join(self.dir, 'schema.bin')
        self.lock_path = os.path.join(self.dir, '.lock')
        os.makedirs(self.dir, exist_ok=True)
        self._write_lock = threading.RLock()
        self._mem_index = {}  # key tuple -> (offset, length, alive)
        self._load_or_init()

    # --- schema and bootstrap ---
    def _load_or_init(self):
        if not os.path.exists(self.schema_path):
            with open(self.schema_path, 'wb') as f:
                f.write(self.schema.dumps())
                f.flush(); os.fsync(f.fileno())
        else:
            with open(self.schema_path, 'rb') as f:
                b = f.read()
            self.schema = Schema.loads(b)
        # Ensure data file exists
        if not os.path.exists(self.data_path):
            with open(self.data_path, 'ab') as f:
                f.flush(); os.fsync(f.fileno())
        # Build in-memory index (single pass)
        self._rebuild_index()

    def _rebuild_index(self):
        self._mem_index.clear()
        if not self.schema.unique:
            return
        with open(self.data_path, 'rb') as f:
            pos = 0
            while True:
                header = f.read(5)
                if not header or len(header) < 5:
                    break
                tomb, = struct.unpack('<B', header[:1])
                (plen,) = struct.unpack('<I', header[1:5])
                payload = f.read(plen)
                if len(payload) < plen:
                    break  # truncated; ignore
                # decode unique key
                bio = io.BytesIO(payload)
                values = []
                for tcode in self.schema.col_types:
                    values.append(_unpack_value(tcode, bio))
                row = dict(zip(self.schema.col_names, values))
                key = tuple(row[u] for u in self.schema.unique)
                self._mem_index[key] = (pos, 5 + plen, tomb == 0)
                pos += 5 + plen

    # --- low-level row io ---
    def _encode_row(self, data: dict) -> bytes:
        # must include all columns; missing -> empty default
        payload = io.BytesIO()
        for name, tcode in zip(self.schema.col_names, self.schema.col_types):
            if name not in data:
                raise ValueError(f"Missing column '{name}'")
            payload.write(_pack_value(tcode, data[name]))
        payload_b = payload.getvalue()
        return struct.pack('<B', 0) + struct.pack('<I', len(payload_b)) + payload_b

    def _read_row_at(self, f, offset: int) -> tuple[dict, bool, int]:
        f.seek(offset)
        header = f.read(5)
        if len(header) < 5:
            return {}, False, 0
        tomb, = struct.unpack('<B', header[:1])
        (plen,) = struct.unpack('<I', header[1:5])
        payload = f.read(plen)
        bio = io.BytesIO(payload)
        values = []
        for tcode in self.schema.col_types:
            values.append(_unpack_value(tcode, bio))
        return dict(zip(self.schema.col_names, values)), (tomb == 0), (5 + plen)

    def _iter_rows(self, include_deleted=False):
        with open(self.data_path, 'rb') as f:
            pos = 0
            while True:
                header = f.read(5)
                if not header or len(header) < 5:
                    break
                (tomb,) = struct.unpack('<B', header[:1])
                (plen,) = struct.unpack('<I', header[1:5])
                payload = f.read(plen)
                if len(payload) < plen:
                    break
                if include_deleted or tomb == 0:
                    bio = io.BytesIO(payload)
                    values = []
                    for tcode in self.schema.col_types:
                        values.append(_unpack_value(tcode, bio))
                    row = dict(zip(self.schema.col_names, values))
                    yield (pos, row, tomb == 0)
                pos += 5 + plen

    # --- high-level ops ---
    def insert(self, data: dict) -> bool:
        # Validate data types and presence
        for col, pytype in self.schema.columns.items():
            if col not in data:
                raise ValueError(f"Missing column '{col}' in insert")
            v = data[col]
            if pytype is bytes:
                if not isinstance(v, (bytes, bytearray)):
                    raise TypeError(f"Column '{col}' expects bytes")
            elif not isinstance(v, pytype):
                raise TypeError(f"Column '{col}' expects {pytype.__name__}")
        key = tuple(data[u] for u in self.schema.unique) if self.schema.unique else None
        with self._write_lock:
            with FileLock(self.lock_path).acquire(exclusive=True):
                # enforce unique
                if key is not None and key in self._mem_index and self._mem_index[key][2]:
                    return False
                row_b = self._encode_row(data)
                with open(self.data_path, 'ab') as f:
                    offset = f.tell()
                    f.write(row_b)
                    f.flush(); os.fsync(f.fileno())
                if key is not None:
                    self._mem_index[key] = (offset, len(row_b), True)
                return True

    def select(self, where: dict | None = None):
        where = where or {}
        # fast path: if where covers full unique key
        if self.schema.unique and all(k in where for k in self.schema.unique):
            key = tuple(where[u] for u in self.schema.unique)
            info = self._mem_index.get(key)
            if info and info[2]:
                with open(self.data_path, 'rb') as f:
                    row, alive, _ = self._read_row_at(f, info[0])
                    if alive and all(row.get(k) == v for k, v in where.items()):
                        return row
                    return None
            return None
        # else full scan (alive rows only)
        for _, row, alive in self._iter_rows(include_deleted=False):
            if alive and all(row.get(k) == v for k, v in where.items()):
                return row
        return None

    def _mark_deleted_at(self, offset: int):
        # Set tombstone byte to 1 in-place
        with open(self.data_path, 'r+b') as f:
            f.seek(offset)
            f.write(struct.pack('<B', 1))
            f.flush(); os.fsync(f.fileno())

    def update(self, where: dict, data: dict) -> bool:
        # find matching row(s) (first match only, like a simple DB)
        target_offset = None
        target_row = None
        # Try unique fast path
        if self.schema.unique and all(k in where for k in self.schema.unique):
            key = tuple(where[u] for u in self.schema.unique)
            info = self._mem_index.get(key)
            if info and info[2]:
                with open(self.data_path, 'rb') as f:
                    row, alive, _ = self._read_row_at(f, info[0])
                    if alive and all(row.get(k) == v for k, v in where.items()):
                        target_offset = info[0]
                        target_row = row
        if target_row is None:
            for off, row, alive in self._iter_rows(include_deleted=False):
                if alive and all(row.get(k) == v for k, v in where.items()):
                    target_offset = off
                    target_row = row
                    break
        if target_row is None:
            return False
        # Merge row
        new_row = dict(target_row)
        new_row.update(data)
        # Enforce unique constraint for new values
        new_key = tuple(new_row[u] for u in self.schema.unique) if self.schema.unique else None
        with self._write_lock:
            with FileLock(self.lock_path).acquire(exclusive=True):
                if self.schema.unique:
                    # If new key collides with another alive record
                    existing = self._mem_index.get(new_key)
                    if existing and existing[2]:
                        # If it's the same row, it's ok, else reject
                        if existing[0] != target_offset:
                            return False
                # Append new row
                row_b = self._encode_row(new_row)
                with open(self.data_path, 'ab') as f:
                    new_offset = f.tell()
                    f.write(row_b)
                    f.flush(); os.fsync(f.fileno())
                # Tombstone old row
                self._mark_deleted_at(target_offset)
                # Update index
                if self.schema.unique:
                    # old key
                    old_key = tuple(target_row[u] for u in self.schema.unique)
                    self._mem_index[old_key] = (target_offset, 0, False)
                    self._mem_index[new_key] = (new_offset, len(row_b), True)
                return True

    def delete(self, where: dict) -> bool:
        # Find one matching record
        target_offset = None
        target_row = None
        if self.schema.unique and all(k in where for k in self.schema.unique):
            key = tuple(where[u] for u in self.schema.unique)
            info = self._mem_index.get(key)
            if info and info[2]:
                with open(self.data_path, 'rb') as f:
                    row, alive, _ = self._read_row_at(f, info[0])
                    if alive and all(row.get(k) == v for k, v in where.items()):
                        target_offset = info[0]
                        target_row = row
        if target_row is None:
            for off, row, alive in self._iter_rows(include_deleted=False):
                if alive and all(row.get(k) == v for k, v in where.items()):
                    target_offset = off
                    target_row = row
                    break
        if target_row is None:
            return False
        with self._write_lock:
            with FileLock(self.lock_path).acquire(exclusive=True):
                self._mark_deleted_at(target_offset)
                if self.schema.unique:
                    key = tuple(target_row[u] for u in self.schema.unique)
                    self._mem_index[key] = (target_offset, 0, False)
                return True


class Database:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        os.makedirs(self.root, exist_ok=True)
        self._tables: dict[str, Table] = {}
        self._db_lock = threading.RLock()

    # Public API
    def create_table(self, name: str, columns: dict, unique: list | None = None):
        with self._db_lock:
            tdir = os.path.join(self.root, name)
            os.makedirs(tdir, exist_ok=True)
            schema_path = os.path.join(tdir, 'schema.bin')
            if os.path.exists(schema_path):
                # Table exists; load to cache if not present
                if name not in self._tables:
                    with open(schema_path, 'rb') as f:
                        schema = Schema.loads(f.read())
                    self._tables[name] = Table(self.root, name, schema)
                return
            # Create new table
            schema = Schema(columns, unique)
            tbl = Table(self.root, name, schema)
            self._tables[name] = tbl

    def _get_table(self, name: str) -> Table:
        with self._db_lock:
            if name in self._tables:
                return self._tables[name]
            # lazy load
            tdir = os.path.join(self.root, name)
            schema_path = os.path.join(tdir, 'schema.bin')
            if not os.path.exists(schema_path):
                raise ValueError(f"Table '{name}' does not exist")
            with open(schema_path, 'rb') as f:
                schema = Schema.loads(f.read())
            tbl = Table(self.root, name, schema)
            self._tables[name] = tbl
            return tbl

    def insert(self, table: str, data: dict) -> bool:
        return self._get_table(table).insert(data)

    def select(self, table: str, where: dict | None = None):
        return self._get_table(table).select(where)

    def update(self, table: str, where: dict, data: dict) -> bool:
        return self._get_table(table).update(where, data)

    def delete(self, table: str, where: dict) -> bool:
        return self._get_table(table).delete(where)


# Public module-level API

def connect(path: str) -> Database:
    """Connect to (or create) a QuixDB database located at a folder path."""
    return Database(path)






"""import os
import hashlib
import threading



class QuixDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        


# Connect function for the API
def connect(db_path: str) -> QuixDB:
    return QuixDB(db_path)"""
