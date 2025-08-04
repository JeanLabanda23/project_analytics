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
df.columns = [
    re.sub(r'[^0-9A-Za-z_]', '_', col).upper()
    for col in df.columns
]
print("🔄 Columnas tras sanitizar:", list(df.columns))

#connect to snowflake using environment variables
ctx = snowflake.connector.connect(
    user      = os.getenv("SF_USER"),
    password  = os.getenv("SF_PASSWORD"),
    account   = os.getenv("SF_ACCOUNT"),
    warehouse = "XSMALL",
    database  = "PROJECT_DB",
    schema    = "RAW",
    role      = "ACCOUNTADMIN"
)

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