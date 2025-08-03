import pandas as pd
from pathlib import Path

RAW_DIR = Path (__file__).resolve().parents[1]/"data"/"raw"
OUT_DIR = Path (__file__).resolve().parents[1]/"data"/"processed"
OUT_DIR.mkdir(exist_ok=True)

FILE_NAME = "SALES_DATA.CSV"
ID_VARS = [
    'CORP', 'Laboratory', 'LAB_MTA', 'Product',
    'TIPO PRODUCTO', 'prod ty', 'Pack Code',
    'Product Launch', 'Pack Launch',
    'ATC 1', 'ATC 3', 'ATC 4', 'ATC4 COD',
    'Pack', 'Concatenate Molecule (Spanish)',
    'Metric'
]

def clean_and_melt(file_name: str):
    df= pd.read_csv(RAW_DIR / file_name)

    value_vars = [c for c in df.columns if c not in ID_VARS]

    long = df.melt(
                id_vars=ID_VARS,
                value_vars=value_vars,
                var_name='period',
                value_name='metric_value'
            )
    long['period']=pd.to_datetime(long['period'],
                                  format='%b-%y',
                                  errors='coerce')
    
    long['metric_value'] = long['metric_value'].astype(str)

    long['metric_value'] = long['metric_value'].str.replace(',', '',regex=False)

    long['metric_value'] = pd.to_numeric(long['metric_value'],errors='coerce')

    long = long.dropna(subset=['metric_value'])
    
    base = Path (file_name).stem
    out_path = OUT_DIR / f"{base}_long.csv"
    long.to_csv(out_path, index=False)
    print(f"✅ Guardado: {out_path}")

if __name__ == "__main__":
    clean_and_melt(FILE_NAME)