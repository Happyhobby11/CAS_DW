from dotenv import load_dotenv
from pyspark.sql import DataFrame

load_dotenv()


def prepare_spark():
    global spark

    # Initalize Spark
    from pyspark.sql import SparkSession
    import os

    SPARK_MASTER: str = os.getenv("SPARK_MASTER")

    # lastest postgresql jdbc version is 42.5.1
    packages = ["org.postgresql:postgresql:42.2.27"]

    spark = (
        SparkSession.builder.appName("Transform from application database")
        .master(f"spark://{SPARK_MASTER}:7077")
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )


def read_staging_pets_from_app():
    import os

    POSTGRES_DB_APP: str = os.getenv("POSTGRES_DB_APP")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST")
    POSTGRES_QUERY: str = """
    """

    df: DataFrame = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://{}/{}".format(POSTGRES_HOST, POSTGRES_DB_APP))
        .option("dbtable", POSTGRES_QUERY)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return df


def read_staging_maps_from_app():
    import os

    POSTGRES_DB_APP: str = os.getenv("POSTGRES_DB_APP")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST")
    POSTGRES_QUERY: str = """
    """

    df: DataFrame = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://{}/{}".format(POSTGRES_HOST, POSTGRES_DB_APP))
        .option("dbtable", POSTGRES_QUERY)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return df


def write_dataframes_to_dw(df: DataFrame, table: str):
    df.show()
    import os

    POSTGRES_DB_DW: str = os.getenv("POSTGRES_DB_DW")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST")
    POSTGRES_TABLE: str = table

    df.write.format("jdbc").option(
        "url", "jdbc:postgresql://{}/{}".format(POSTGRES_HOST, POSTGRES_DB_DW)
    ).option("dbtable", POSTGRES_TABLE).option("user", POSTGRES_USER).option(
        "password", POSTGRES_PASSWORD
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "append"
    ).save()


def transform():
    prepare_spark()
    df_pets = read_staging_pets_from_app
    write_dataframes_to_dw(df_pets, "staging_pets")


if __name__ == "__transform__":
    transform()
