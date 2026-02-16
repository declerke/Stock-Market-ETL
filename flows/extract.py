import pandas as pd
# --- MONKEYPATCH START ---
# SimFin uses an old version of pd.read_csv. We fix it before importing simfin.
import pandas as pd
import builtins

# We wrap the original read_csv to intercept the 'date_parser' argument
original_read_csv = pd.read_csv
def patched_read_csv(*args, **kwargs):
    if 'date_parser' in kwargs:
        kwargs['date_format'] = kwargs.pop('date_parser')
    return original_read_csv(*args, **kwargs)
pd.read_csv = patched_read_csv
# --- MONKEYPATCH END ---

import simfin as sf
from pathlib import Path
from prefect import flow, task
from prefect_gcp import GcsBucket
from utils.config import CONFIG
import shutil
import re

def get_clean_key():
    raw_key = CONFIG.get('sim-fin-api-key', '')
    return re.sub(r'[^a-zA-Z0-9-]', '', raw_key)

@task(name="set_simfin_api_key")
def set_api_key():
    clean_key = get_clean_key()
    sf.set_api_key(clean_key)
    # Using local project dir to avoid Windows Temp permission issues
    data_dir = Path.cwd() / "simfin_data"
    data_dir.mkdir(exist_ok=True)
    sf.set_data_dir(str(data_dir))
    print(f"✓ SimFin API configured with key: {clean_key[:4]}...")

@task(name="extract_fundamentals")
def extract_fundamentals():
    print("Extracting company fundamentals...")
    # These functions call pd.read_csv internally; our patch will now handle it!
    df_income = sf.load_income(variant='annual', market='us')
    df_balance = sf.load_balance(variant='annual', market='us')
    df_cashflow = sf.load_cashflow(variant='annual', market='us')
    
    # Merge on Ticker and Report Date
    df = pd.merge(df_income, df_balance, on=['Ticker', 'Report Date'], how='outer')
    df = pd.merge(df, df_cashflow, on=['Ticker', 'Report Date'], how='outer')
    
    return df.reset_index()

@task(name="extract_prices")
def extract_prices():
    print("Extracting stock prices...")
    df_prices = sf.load_shareprices(variant='daily', market='us')
    return df_prices.reset_index()

@task(name="save_to_parquet")
def save_to_parquet(df: pd.DataFrame, filename: str) -> Path:
    out_dir = Path.cwd() / "data_temp"
    out_dir.mkdir(exist_ok=True)
    filepath = out_dir / filename
    # Parquet requires string column names
    df.columns = df.columns.astype(str)
    df.to_parquet(filepath, engine='pyarrow', index=False)
    print(f"✓ Saved {filename} locally")
    return filepath

@task(name="upload_to_gcs")
def upload_to_gcs(local_path: Path, gcs_path: str):
    try:
        gcs_bucket = GcsBucket.load("gcs-bucket")
        gcs_bucket.upload_from_path(from_path=local_path, to_path=gcs_path)
        print(f"✓ Uploaded to GCS: {gcs_path}")
    except Exception as e:
        print(f"⚠️ GCS Upload skipped/failed: {e}")

@flow(name="extract_stock_data", log_prints=True)
def extract_flow():
    set_api_key()
    
    # Process Fundamentals
    df_f = extract_fundamentals()
    f_path = save_to_parquet(df_f, "fundamentals.parquet")
    upload_to_gcs(f_path, "raw/fundamentals/fundamentals.parquet")
    
    # Process Prices
    df_p = extract_prices()
    p_path = save_to_parquet(df_p, "prices.parquet")
    upload_to_gcs(p_path, "raw/prices/prices.parquet")

if __name__ == "__main__":
    extract_flow()