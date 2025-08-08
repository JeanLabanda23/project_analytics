import os
import snowflake.connector

ctx = snowflake.connector.connect(
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    account=os.getenv("SF_ACCOUNT"),
    role="ACCOUNTADMIN",
    warehouse="ANALYTICS_WH",
    database="PROJECT_DB",
    schema="RAW"
)
cs = ctx.cursor()

def run_file(path):
    with open(path, 'r') as f:
        cs.execute(f.read())
    print(f"✅ Ejecutado: {path}")

if __name__ == "__main__":
    run_file("snowflake/init.sql")
    run_file("snowflake/load_fact.sql")
    cs.close()
    ctx.close()