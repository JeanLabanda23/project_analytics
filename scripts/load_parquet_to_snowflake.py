import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import re


# route to parquet file
BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED = BASE_DIR/"data"/"processed"
PQ_FILE = "SALES_DATA_long.parquet"
pq_path = PROCESSED / PQ_FILE

print (f"🔍 Leyendo Parquet desde: {pq_path}")
df = pd.read_parquet(pq_path)
print(f"✅ Parquet leído: {len(df)} filas, {len(df.columns)} columnas")

#sanitize columns for snowflake
def sanitize(col: str) -> str:
    # replace non-alfanumeric '_'
    s = re.sub(r'[^0-9A-Za-z_]', '_', col)
    # colapse many '_' in one
    s = re.sub(r'_+', '_', s)
    # delete '_' at start and final, and convert to upper
    return s.strip('_').upper()

df.columns = [sanitize(c) for c in df.columns]
print("🔄 Columnas tras sanitizar:", list(df.columns))

#connect to snowflake using environment variables
ctx = snowflake.connector.connect(
    user      = os.getenv("SF_USER"),
    password  = os.getenv("SF_PASSWORD"),
    account   = os.getenv("SF_ACCOUNT"),
    warehouse = "ANALYTICS_WH",
    database  = "PROJECT_DB",
    schema    = "RAW",
    role      = "ACCOUNTADMIN"
)
cs=ctx.cursor()

# Make sure Snowflake uses the right warehouse
cs.execute("USE WAREHOUSE ANALYTICS_WH")

#parallel insert with write_pandas
success, nchunks, nrows, _ = write_pandas(
    conn       = ctx,
    df         = df,
    table_name = "SALES_MONTHLY",
    schema     = "RAW"
)

print(f"✅ Cargados {nrows} filas en {nchunks} archivos: éxito={success}")

# Close
cs.close()
ctx.close()