from config import cfg, ComplexEncoder
from table import fact_dim, bgtables
from google.cloud import bigquery

# import pandas as pd
import psycopg2 as psycopg
import json


conn = psycopg.connect(
    dbname=cfg.POSTGRES_DB_DW,
    user=cfg.POSTGRES_USER_DW,
    password=cfg.POSTGRES_PASSWORD_DW,
    host=cfg.POSTGRES_HOST_DW,
)
conn.autocommit = True
client = bigquery.Client()

# staging_pets_sql = "staging_pets"
# staging_maps_sql = "staging_maps"
# queries = [staging_pets_sql, staging_maps_sql]
# queries = bgtables[0:2]
queries = [fact_dim.DIM_DISTRICT]

for query in queries:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * from {query}")
        print(f"======{query} before encoding ======")
        value = cur.fetchall()
        print(value)
        print(f"======{query} after encoding ======")
        # new_value = json.dumps(value, cls=ComplexEncoder)
        # print(new_value)
        target = client.get_table(f"{cfg.PROJECT_ID}.{cfg.DATASET_ID}.{query}")
        client.insert_rows(target, value)
        print(f"insert {query} to BQ done!")


# def prepare_spark():
#     global spark

#     # Initalize Spark
#     from pyspark.sql import SparkSession

#     # lastest postgresql jdbc version is 42.5.1
#     packages = ["org.postgresql:postgresql:42.2.27"]

#     spark = (
#         SparkSession.builder.appName("Transform from application database")
#         .master(f"spark://{cfg.SPARK_MASTER}:7077")
#         .config("spark.jars.packages", ",".join(packages))
#         .getOrCreate()
#     )

# table names
# edit_pages = client.get_table('tecky-analytics.recentchange.edit_pages')

# result = client.insert_rows_json(edit_pages,[value])
