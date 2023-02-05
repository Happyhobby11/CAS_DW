from config import cfg
from table import bgtables
from query import sqtables
from google.cloud import bigquery

import psycopg2 as psycopg

# import json


conn = psycopg.connect(
    dbname=cfg.POSTGRES_DB_DW,
    user=cfg.POSTGRES_USER_DW,
    password=cfg.POSTGRES_PASSWORD_DW,
    host=cfg.POSTGRES_HOST_DW,
)
conn.autocommit = True
client = bigquery.Client()


item1 = [sqtables[0], bgtables[0]]
item2 = [sqtables[1], bgtables[1]]
item3 = [sqtables[2], bgtables[2]]
item4 = [sqtables[3], bgtables[3]]
item5 = [sqtables[4], bgtables[4]]

inputs = [item1, item2, item3, item4, item5]

for input in inputs:
    with conn.cursor() as cur:
        cur.execute(f"SELECT {input[0]} from {input[1]}")
        print(f"======{input[1]} before encoding ======")
        value = cur.fetchall()
        print(value[0:20])
        # print(f"======{query} after encoding ======")
        # new_value = json.dumps(value, cls=ComplexEncoder)
        # print(new_value)
        target = client.get_table(f"{cfg.PROJECT_ID}.{cfg.DATASET_ID}.{input[1]}")
        client.insert_rows(target, value)
        print(f"insert {input[1]} to BQ done!")
