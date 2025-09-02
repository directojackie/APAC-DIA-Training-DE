# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
import argparse, pathlib, os, hashlib, json, datetime as dt
import sys
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq
from schemas.schemas import customers_schema, products_schema, stores_schema, suppliers_schema, orders_header_schema, orders_lines_schema, events_schema, sensors_schema, exchange_rates_schema, shipments_schema, returns_day1_schema
try:
    from deltalake import write_deltalake
except Exception as e:
    write_deltalake = None

# Define arguments when running the pipeline
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    ap.add_argument('--dry-run', action='store_true')
    return ap.parse_args()

# Define the lakehouse directory
def ensure_dirs(lake_root):
    for sub in ['bronze/parquet','bronze/delta']:
        (lake_root/sub).mkdir(parents=True, exist_ok=True)
    (lake_root/'_rejects').mkdir(parents=True, exist_ok=True)

def init_manifest(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP,
            row_count BIGINT,
            reject_count BIGINT,
            status TEXT
        )
    """)

def already_processed(conn, p):
    print('already processed') 
    return conn.execute("SELECT 1 FROM manifest_processed_files WHERE src_path = ?", [str(p)]).fetchone() is not None

def mark_processed(conn, p, n, r, status): 
    print('mark processed')
    conn.execute("INSERT OR REPLACE INTO manifest_processed_files VALUES (?, ?, ?, ?, ?)", 
                 [str(p), dt.datetime.now(dt.UTC), n, r, status])
    # print("Table Has been Loaded in the Deltalake.")

# Function to write to parquet, add partitioning
def write_parquet_partitioned(table, base_path, partitioning=None):
    pads.write_dataset(table, base_dir=str(base_path), format='parquet', partitioning=partitioning, existing_data_behavior='overwrite_or_ignore')

# Function to load data in deltalake

def write_delta(
    table: pa.Table,
    base_path: str | pathlib.Path,
    mode: str = "append",
    partition_by: list[str] | str | None = None,
    merge_schema: bool = True
):
    if write_deltalake is None:
        raise RuntimeError("deltalake not installed")

    if not isinstance(table, pa.Table):
        raise TypeError("Expected a pyarrow.Table for `table`")

    schema_mode = "merge" if merge_schema else None

    write_deltalake(
        table_or_uri=str(base_path),
        data=table,
        partition_by=partition_by,
        mode=mode,
        schema_mode=schema_mode
    )


# Adds ingestion_ts in AU Time Zone, src_filename and src_row_hash columns 
def add_audit_columns(tbl, src_path):
    now = pa.scalar(dt.datetime.now(dt.UTC), type=pa.timestamp('ns', tz='Australia/Perth'))
    ts_col = pa.array([now.as_py()] * len(tbl), type=pa.timestamp('ns', tz='Australia/Perth'))
    src_col = pa.array([str(src_path.name)] * len(tbl))
    hash_col = pa.array([hashlib.md5(str(tbl.slice(i,1)).encode()).hexdigest() for i in range(len(tbl))])
    tbl = tbl.append_column('ingestion_ts', ts_col)
    tbl = tbl.append_column('src_filename', src_col)
    tbl = tbl.append_column('src_row_hash', hash_col)
    return tbl

# Handles rejected data
def handle_rejects(tbl, reason, lake_root, src_path):
    reject_path = lake_root/'_rejects'/f"{src_path.stem}_rejects.parquet"
    tbl = tbl.append_column('reject_reason', pa.array([reason]*len(tbl)))
    pq.write_table(tbl, reject_path)
    print(f"Failed loading {tbl} due to {reason}.")

# Load data to customers table from customers.csv
def load_customers(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'customers.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(customers_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root/'bronze'/'parquet'/'customers'
            dl_base = lake_root/'bronze'/'delta'/'customers'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
        print('customers has been loaded to deltalake.')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        print('error loading customers into deltalake.')
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load data to stores table from stores.csv
def load_stores(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'stores.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(stores_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root/'bronze'/'parquet'/'stores'
            dl_base = lake_root/'bronze'/'delta'/'stores'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load data to products table from products.csv
def load_products(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'products.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(products_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root/'bronze'/'parquet'/'products'
            dl_base = lake_root/'bronze'/'delta'/'products'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        mark_processed(conn, src, 0, len(tbl), 'failed')

# Load data to suppliers table from suppliers.csv
def load_suppliers(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'suppliers.csv'
    if not src.exists(): return
    if already_processed(conn, src): return
    try:
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(suppliers_schema, safe=False)
        tbl = add_audit_columns(tbl, src)
        if not dry_run:
            pq_base = lake_root/'bronze'/'parquet'/'suppliers'
            dl_base = lake_root/'bronze'/'delta'/'suppliers'
            write_parquet_partitioned(tbl, pq_base)
            write_delta(tbl, dl_base, mode='append')
        mark_processed(conn, src, len(tbl), 0, 'success')
    except Exception as e:
        handle_rejects(tbl, str(e), lake_root, src)
        mark_processed(conn, src, 0, len(tbl), 'failed')

def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)
    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(args.manifest)
    conn.execute("INSTALL delta; LOAD delta;")
    init_manifest(conn)

    # load_customers(raw_root, lake_root, conn, dry_run=args.dry_run)
    # load_stores(raw_root, lake_root, conn, dry_run=args.dry_run)
    # load_products(raw_root, lake_root, conn, dry_run=args.dry_run)
    # load_suppliers(raw_root, lake_root, conn, dry_run=args.dry_run)


    print("✅ Bronze load completed for implemented loaders (extend for all tables).")

if __name__ == '__main__':
    main()
