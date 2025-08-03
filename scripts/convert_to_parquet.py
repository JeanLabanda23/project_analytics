import pandas as pd
from pathlib import Path

#directories

BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED = BASE_DIR/"data"/"processed"
PROCESSED.mkdir(exist_ok=True)

#file names

CSV_NAME= "SALES_DATA_long.csv"
PQ_NAME= "SALES_DATA_long.parquet"

def convert():
    #CSV as Path
    csv_path = PROCESSED/ CSV_NAME
    print(f"🔍 Leyendo CSV desde: {csv_path}")

    #read csv in dataframe
    df=pd.read_csv(csv_path)
    print(f"✅ CSV leído: {len(df)} filas, {len(df.columns)} columnas")

    #route parquet as Path
    pq_path = PROCESSED/PQ_NAME

    #convert to parquet
    df.to_parquet(str(pq_path), index=False)
    print(f"✅ Parquet escrito en: {pq_path}")

if __name__ == "__main__":
    convert()