from pyspark.sql import DataFrame
from config import cfg
from query import qy
from table import tb


def prepare_spark():
    global spark

    # Initalize Spark
    from pyspark.sql import SparkSession

    # lastest postgresql jdbc version is 42.5.1
    packages = ["org.postgresql:postgresql:42.2.27"]

    spark = (
        SparkSession.builder.appName("Transform from application database")
        .master(f"spark://{cfg.SPARK_MASTER}:7077")
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )


def read_queries_from_app(query: str):

    df: DataFrame = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:postgresql://{}/{}".format(
                cfg.POSTGRES_HOST_APP, cfg.POSTGRES_DB_APP
            ),
        )
        .option("dbtable", query)
        .option("user", cfg.POSTGRES_USER_APP)
        .option("password", cfg.POSTGRES_PASSWORD_APP)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return df


def write_dataframes_to_dw(df: DataFrame, table: str):
    df.show()

    df.write.format("jdbc").option(
        "url",
        "jdbc:postgresql://{}/{}".format(cfg.POSTGRES_HOST_DW, cfg.POSTGRES_DB_DW),
    ).option("dbtable", table).option("user", cfg.POSTGRES_USER_DW).option(
        "password", cfg.POSTGRES_PASSWORD_DW
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()
    print(f"insert {table} success!")


def transform():
    prepare_spark()
    df_maps = read_queries_from_app(qy.MAPS_QUERY)
    df_pets = read_queries_from_app(qy.PETS_QUERY)
    df_events = read_queries_from_app(qy.EVENTS_QUERY)
    df_exp_like = read_queries_from_app(qy.EXP_LIKE_QUERY)
    df_exp = read_queries_from_app(qy.EXP_QUERY)
    write_dataframes_to_dw(df_maps, tb.MAPS_TABLE)
    write_dataframes_to_dw(df_pets, tb.PETS_TABLE)
    write_dataframes_to_dw(df_events, tb.EVENTS_TABLE)
    write_dataframes_to_dw(df_exp_like, tb.EXP_LIKE_TABLE)
    write_dataframes_to_dw(df_exp, tb.EXP_TABLE)


if __name__ == "__main__":
    transform()
