from dotenv import load_dotenv
from pyspark.sql import DataFrame
from config import cfg

load_dotenv()


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


def read_staging_pets_from_app():

    POSTGRES_QUERY: str = """
    (SELECT
        p.name as name,
        p.gender as gender,
        u.email as user_email,
        u.year_birth as user_year_birth,
        u.month_birth as user_month_birth,
        u.gender as user_gender,
        u.district as district,
        f.english_species as parent_species,
        s.english_species as species,
        EXTRACT (year FROM p.date_birth) as year_birth,
        EXTRACT (month FROM p.date_birth) as month_birth,
        pi.name as img,
        EXTRACT (year FROM pi.created_at) as img_year,
        EXTRACT (month FROM pi.created_at) as img_month
    FROM pets p
    inner join users u on p.user_id = u.id
    inner join species s on s.id = p.species_id
    inner join species f on f.id = s.family_id
    full join pets_img pi on pi.pet_id = p.id)
    pets_info"""

    df: DataFrame = (
        spark.read.format("jdbc")
        .option(
            "url",
            "jdbc:postgresql://{}/{}".format(
                cfg.POSTGRES_HOST_APP, cfg.POSTGRES_DB_APP
            ),
        )
        .option("dbtable", POSTGRES_QUERY)
        .option("user", cfg.POSTGRES_USER_APP)
        .option("password", cfg.POSTGRES_PASSWORD_APP)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return df


# (
#             WITH tmp_table AS
#                 (
#                     SELECT * FROM species WHERE family_id is NULL
#                 )
#             SELECT tmp_table.english_species
#             FROM species
#             JOIN tmp_table ON tmp_table.id = species.family_id
#             WHERE tmp_table.family_id is NULL
#         ) parent_species,

#     (SELECT
#         p.name as name,
#         p.gender as gender,
#         u.email as user_email,
#         u.year_birth as user_year_birth,
#         u.month_birth as user_month_birth,
#         u.gender as user_gender,
#         u.district as district,
#         f.eng_species as parent_species,
#         s.eng_species as species,
#         EXTRACT (year FROM p.date_birth) as year_birth,
#         EXTRACT (month FROM p.date_birth) as month_birth,
#         pi.name as img,
#         EXTRACT (year FROM pi.created_at) as img_year,
#         EXTRACT (month FROM pi.created_at) as img_month
#     FROM pets p
#     full join users u on p.user_id = u.id
#     full join species s on s.id = p.species_id
#     full join species f on f.id = s.family_id
#     full join pets_img pi on pi.pet_id = p.id)
#     pets_info"""
# """
# (SELECT
#     s.id as id_s,
#     s.family_id as family_id_s,
#     s.chinese_species as chinese_species_s,
#     s.english_species as english_species_s,
#     f.id as id_f,
#     f.family_id as family_id_f,
#     f.chinese_species as chinese_species_f,
#     f.english_species as english_species_f
# FROM species s, species f
# WHERE f.id = s.family_id)
#     pets_info"""


def read_staging_maps_from_app():
    pass


def write_dataframes_to_dw(df: DataFrame):
    df.show()
    # import os

    # POSTGRES_DB_DW: str = os.getenv("POSTGRES_DB_DW")
    # POSTGRES_USER_DW: str = os.getenv("POSTGRES_USER_DW")
    # POSTGRES_PASSWORD_DW: str = os.getenv("POSTGRES_PASSWORD_DW")
    # POSTGRES_HOST_DW: str = os.getenv("POSTGRES_HOST_DW")
    # POSTGRES_TABLE: str = table

    # df.write.format("jdbc").option(
    #     "url", "jdbc:postgresql://{}/{}".format(POSTGRES_HOST_DW, POSTGRES_DB_DW)
    # ).option("dbtable", POSTGRES_TABLE).option("user", POSTGRES_USER_DW).option(
    #     "password", POSTGRES_PASSWORD_DW
    # ).option(
    #     "driver", "org.postgresql.Driver"
    # ).mode(
    #     "append"
    # ).save()


def transform():
    prepare_spark()
    df_pets = read_staging_pets_from_app()
    df_maps = read_staging_maps_from_app()
    write_dataframes_to_dw(df_pets)
    write_dataframes_to_dw(df_maps)


if __name__ == "__main__":
    transform()
