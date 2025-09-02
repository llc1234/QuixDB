# QuixDB.py
import os, json, time, hashlib

class QuixDB:
    def __init__(self, folder):
        self.folder = folder
        os.makedirs(folder, exist_ok=True)

    @staticmethod
    def connect(folder):
        return QuixDB(folder)

    def _table_path(self, table):
        return os.path.join(self.folder, table)

    def create_table(self, table, columns, unique=None):
        """Create table if not exists; store columns and unique list in meta.json."""
        if unique is None:
            unique = []
        tpath = self._table_path(table)
        if not os.path.exists(tpath):
            os.makedirs(tpath)
            meta = {
                "columns": {col: typ.__name__ for col, typ in columns.items()},
                "unique": unique,
                "next_id": 1
            }
            # Save metadata
            with open(os.path.join(tpath, "meta.json"), "w") as f:
                json.dump(meta, f)
            # Create row storage
            os.makedirs(os.path.join(tpath, "rows"), exist_ok=True)
            # Create index dirs for each unique column
            for col in unique:
                os.makedirs(os.path.join(tpath, f"index_{col}"), exist_ok=True)

    def _load_meta(self, table):
        meta_path = os.path.join(self._table_path(table), "meta.json")
        with open(meta_path) as f:
            meta = json.load(f)
            meta['next_id'] = int(meta['next_id'])
        return meta

    def _save_meta(self, table, meta):
        path = os.path.join(self._table_path(table), "meta.json")
        with open(path + ".tmp", "w") as f:
            json.dump(meta, f)
        os.replace(path + ".tmp", path)

    def _acquire_lock(self, table):
        """Acquire table lock by creating a lock file atomically."""
        lock_path = os.path.join(self._table_path(table), "lock")
        while True:
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, str(os.getpid()).encode())
                os.close(fd)
                return
            except FileExistsError:
                time.sleep(0.01)

    def _release_lock(self, table):
        """Release table lock by removing lock file."""
        try:
            os.unlink(os.path.join(self._table_path(table), "lock"))
        except FileNotFoundError:
            pass

    def insert(self, table, data):
        """Insert a row into table with data dict."""
        tpath = self._table_path(table)
        if not os.path.exists(tpath):
            raise Exception(f"Table {table} does not exist")
        # Load meta
        meta = self._load_meta(table)
        # Validate columns
        for col in data:
            if col not in meta["columns"]:
                raise Exception(f"Unknown column {col}")
        # Enforce unique constraints
        for col in meta["unique"]:
            if col in data:
                val = data[col]
                # compute hash filename for value
                h = hashlib.sha256(val.encode()).hexdigest()
                idx_file = os.path.join(tpath, f"index_{col}", h)
                if os.path.exists(idx_file):
                    return False  # unique constraint violated
        # Acquire lock to perform write
        self._acquire_lock(table)
        try:
            # Re-check uniqueness inside lock (race safety)
            for col in meta["unique"]:
                if col in data:
                    val = data[col]
                    h = hashlib.sha256(val.encode()).hexdigest()
                    idx_file = os.path.join(tpath, f"index_{col}", h)
                    if os.path.exists(idx_file):
                        return False
            row_id = meta["next_id"]
            # Write row file
            row_file = os.path.join(tpath, "rows", f"{row_id}.json")
            with open(row_file + ".tmp", "w") as f:
                json.dump(data, f)
            os.replace(row_file + ".tmp", row_file)
            # Update unique indexes
            for col in meta["unique"]:
                if col in data:
                    val = data[col]
                    h = hashlib.sha256(val.encode()).hexdigest()
                    idx_file = os.path.join(tpath, f"index_{col}", h)
                    with open(idx_file, "w") as idxf:
                        idxf.write(str(row_id))
            # Update next_id
            meta["next_id"] += 1
            self._save_meta(table, meta)
        finally:
            self._release_lock(table)
        return True

    def select(self, table, where=None):
        """Return list of rows matching all exact conditions in where dict."""
        if where is None: where = {}
        tpath = self._table_path(table)
        if not os.path.exists(tpath):
            return []
        meta = self._load_meta(table)
        # Try to use unique index if possible
        row_ids = None
        for col in meta["unique"]:
            if col in where:
                val = where[col]
                h = hashlib.sha256(val.encode()).hexdigest()
                idx_file = os.path.join(tpath, f"index_{col}", h)
                if os.path.exists(idx_file):
                    with open(idx_file) as f:
                        rid = f.read().strip()
                    row_ids = [rid]
                else:
                    return []  # no such row
                break
        results = []
        if row_ids is None:
            # No unique key in where: scan all rows
            rows_dir = os.path.join(tpath, "rows")
            for fname in os.listdir(rows_dir):
                if not fname.endswith(".json"): continue
                with open(os.path.join(rows_dir, fname)) as f:
                    row = json.load(f)
                match = all(str(row.get(col)) == str(val) for col, val in where.items())
                if match:
                    results.append(row)
        else:
            # Load the single matched row and check others
            for rid in row_ids:
                row_file = os.path.join(tpath, "rows", f"{rid}.json")
                if not os.path.exists(row_file): 
                    continue
                with open(row_file) as f:
                    row = json.load(f)
                # check all where clauses
                if all(str(row.get(col)) == str(val) for col, val in where.items()):
                    results.append(row)
        return results

    def update(self, table, where, data):
        """Update rows matching 'where' with new data (dict)."""
        tpath = self._table_path(table)
        if not os.path.exists(tpath):
            return False
        meta = self._load_meta(table)
        rows_to_update = []
        # Find matching row IDs (like select logic)
        if any(col in where for col in meta["unique"]):
            # use unique index if possible
            for col in meta["unique"]:
                if col in where:
                    val = where[col]
                    h = hashlib.sha256(val.encode()).hexdigest()
                    idx_file = os.path.join(tpath, f"index_{col}", h)
                    if os.path.exists(idx_file):
                        with open(idx_file) as f:
                            rid = f.read().strip()
                        # Confirm other conditions
                        row_file = os.path.join(tpath, "rows", f"{rid}.json")
                        if os.path.exists(row_file):
                            row = json.load(open(row_file))
                            if all(str(row.get(c)) == str(v) for c,v in where.items()):
                                rows_to_update.append((rid, row))
                    break
        else:
            # scan all
            rows_dir = os.path.join(tpath, "rows")
            for fname in os.listdir(rows_dir):
                if not fname.endswith(".json"): continue
                rid = fname[:-5]
                row = json.load(open(os.path.join(rows_dir, fname)))
                if all(str(row.get(c)) == str(v) for c,v in where.items()):
                    rows_to_update.append((rid, row))
        if not rows_to_update:
            return False
        # Apply updates
        self._acquire_lock(table)
        try:
            for rid, oldrow in rows_to_update:
                newrow = oldrow.copy()
                for k,v in data.items():
                    newrow[k] = v
                # Check unique conflicts if changing unique fields
                for col in meta["unique"]:
                    if col in data and col in oldrow:
                        newval = data[col]
                        if newval != oldrow[col]:
                            h_new = hashlib.sha256(newval.encode()).hexdigest()
                            h_old = hashlib.sha256(oldrow[col].encode()).hexdigest()
                            new_idx = os.path.join(tpath, f"index_{col}", h_new)
                            old_idx = os.path.join(tpath, f"index_{col}", h_old)
                            if os.path.exists(new_idx):
                                return False
                            os.remove(old_idx)
                            with open(new_idx, "w") as f:
                                f.write(str(rid))
                # Write row file
                row_file = os.path.join(tpath, "rows", f"{rid}.json")
                with open(row_file + ".tmp", "w") as f:
                    json.dump(newrow, f)
                os.replace(row_file + ".tmp", row_file)
        finally:
            self._release_lock(table)
        return True

    def delete(self, table, where):
        """Delete rows matching 'where' conditions."""
        tpath = self._table_path(table)
        if not os.path.exists(tpath):
            return False
        meta = self._load_meta(table)
        rows_deleted = False
        # Find matching rows (reuse select logic for row IDs)
        matches = self.select(table, where)
        if not matches:
            return False
        self._acquire_lock(table)
        try:
            for row in matches:
                # Determine its ID by finding in unique index
                # (Assumes unique key always present in table design for lookup)
                rid = None
                for col in meta["unique"]:
                    if col in row:
                        h = hashlib.sha256(str(row[col]).encode()).hexdigest()
                        idx_file = os.path.join(tpath, f"index_{col}", h)
                        if os.path.exists(idx_file):
                            rid = open(idx_file).read().strip()
                            os.remove(idx_file)
                            break
                if rid is None:
                    # Fallback: search filename by scanning all rows
                    rows_dir = os.path.join(tpath, "rows")
                    for fname in os.listdir(rows_dir):
                        if not fname.endswith(".json"): continue
                        r = json.load(open(os.path.join(rows_dir, fname)))
                        if all(str(r.get(c)) == str(v) for c,v in where.items()):
                            rid = fname[:-5]
                            break
                if rid:
                    # remove any other unique indexes for this row
                    for col in meta["unique"]:
                        if col in row and col not in where:
                            # if unique key not in where, remove by old value
                            if col in row:
                                h = hashlib.sha256(str(row[col]).encode()).hexdigest()
                                idx_path = os.path.join(tpath, f"index_{col}", h)
                                if os.path.exists(idx_path):
                                    os.remove(idx_path)
                    # delete the row file
                    row_file = os.path.join(tpath, "rows", f"{rid}.json")
                    if os.path.exists(row_file):
                        os.remove(row_file)
                    rows_deleted = True
        finally:
            self._release_lock(table)
        return rows_deleted
