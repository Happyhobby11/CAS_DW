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
    packages = ["org.postgresql:postgresql:42.2.18"]

    spark = (
        SparkSession.builder.appName("Transform from application database")
        .master(f"spark://{SPARK_MASTER}:7077")
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )


def extract_tables_from_app():
    return 1


def write_to_s3_bucket(df: DataFrame):
    return df


def backup():
    prepare_spark()
    df = extract_tables_from_app()
    write_to_s3_bucket(df)


if __name__ == "__backup__":
    backup()
