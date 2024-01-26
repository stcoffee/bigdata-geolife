from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, input_file_name, to_date, concat, lit, to_timestamp, \
    monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


def read_trajectory_dataset(spark: SparkSession, input_dir: str) -> DataFrame:
    """ Extract trajectories dataset, as list of coordinates with user id, trajectory id, timestamp\n\n
    return schema\n
    | -- user_id: string(nullable=false)\n
    | -- trajectory_id: string(nullable=false)\n
    | -- date: date(nullable=true)\n
    | -- datetime: timestamp(nullable=true)\n
    | -- latitude: double(nullable=true)\n
    | -- longitude: double(nullable=true)\n
    """

    trajectory_schema = StructType(
        [StructField("latitude", DoubleType(), True),
         StructField("longitude", DoubleType(), True),
         StructField("discard1", StringType(), True),
         StructField("altitude", DoubleType(), True),
         StructField("discard2", StringType(), True),
         StructField("string_date", StringType(), True),
         StructField("string_time", StringType(), True)]
    )

    df = spark.read.csv(input_dir, pathGlobFilter="*.plt", recursiveFileLookup="true",
                        schema=trajectory_schema, mode='DROPMALFORMED')

    # looks for format /Trajectory/{digit}.plt at the end of string (supports both / and \ as delimiters)
    # binds digit from {digit}.plt
    bind_trajectory_pattern = r"(?<=(?:\\|\/)Trajectory(?:\\|\/))(\d+)(?=\.plt$)"

    # looks for format /{digit}/Trajectory/{digit}.plt at the end of string, supports both / and \ as delimiters
    # binds digit from /{digit}/Trajectory
    bind_user_id_pattern = r"(?<=(?:\\|\/))(\d+)(?=(?:(?:\\|\/)Trajectory(?:\\|\/)\d+\.plt)$)"

    df = df.withColumn("user_id", regexp_extract(input_file_name(), bind_user_id_pattern, 1))
    df = df.withColumn("trajectory_id", regexp_extract(input_file_name(), bind_trajectory_pattern, 1))
    df = df.withColumn("date", to_date(df.string_date, "yyyy-MM-dd"))
    df = df.withColumn("datetime",
                       to_timestamp(concat(df.string_date, lit(" "), df.string_time), "yyyy-MM-dd HH:mm:ss"))
    df = df.drop("discard1", "discard2", "string_date", "string_time", "altitude")

    return df.select("user_id", "trajectory_id", "date", "datetime", "latitude", "longitude")


def filter_trajectory_bad_data(df: DataFrame) -> DataFrame:
    df = df.filter(df.latitude.isNotNull() & df.latitude.between(-90, 90)
                   & df.longitude.isNotNull() & df.longitude.between(-180, 180)
                   & df.date.isNotNull() & df.datetime.isNotNull()
                   & (df.user_id != lit("")) & (df.user_id != lit("")))
    return df


def read_labels_dataset(spark: SparkSession, input_dir: str) -> DataFrame:
    """ Extract labels dataset, with user id, start, end, and transportation mode (label)
        return schema \n
         |-- l_user_id: string (nullable = false) \n
         |-- label_id: long (nullable = false) \n
         |-- start: timestamp (nullable = true) \n
         |-- end: timestamp (nullable = true) \n
         |-- label: string (nullable = true) \n
    """
    labels_schema = StructType(
        [StructField("Start Time", TimestampType(), True),
         StructField("End Time", TimestampType(), True),
         StructField("Transportation Mode", StringType(), True)]
    )
    df_labels = spark.read.csv(input_dir, pathGlobFilter="labels.txt", recursiveFileLookup="true", schema=labels_schema,
                               mode='DROPMALFORMED', timestampFormat="yyyy/MM/dd HH:mm:ss", sep="\t",
                               header=True, inferSchema=False)

    # looks for /{digit}/labels.txt at the end of string, binds {digit}, supports both \ and / as delimiters
    bind_user_id_pattern = r"(?<=\\|\/)(\d+)(?=\\|\/labels\.txt$)"

    df_labels = df_labels.withColumn("l_user_id", regexp_extract(input_file_name(), bind_user_id_pattern, 1))
    df_labels = df_labels.withColumn("label_id", monotonically_increasing_id())
    df_labels = df_labels.withColumnRenamed("Start Time", "start")
    df_labels = df_labels.withColumnRenamed("End Time", "end")
    df_labels = df_labels.withColumnRenamed("Transportation Mode", "label")

    return df_labels.select(df_labels.l_user_id, df_labels.label_id, df_labels.start,
                            df_labels.end, df_labels.label)


def filter_labels_bad_data(df_labels: DataFrame) -> DataFrame:
    return df_labels.filter((df_labels.l_user_id != lit("")) & df_labels.start.isNotNull() & df_labels.end.isNotNull()
                            & df_labels.label.isNotNull() & (df_labels.label != lit("")))


def join_trajectories_and_labels(df: DataFrame, dfl: DataFrame) -> DataFrame:
    df = df.join(dfl, [df.user_id == dfl.l_user_id, df.datetime.between(dfl.start, dfl.end)], "inner")
    df = df.drop("l_user_id")
    return df


def read_trajectories_with_labels(spark: SparkSession, input_dir: str) -> DataFrame:
    """ returns schema
    | -- user_id: string(nullable=false)\n
    | -- trajectory_id: string(nullable=false)\n
    | -- date: date(nullable=true)\n
    | -- datetime: timestamp(nullable=true)\n
    | -- latitude: double(nullable=true)\n
    | -- longitude: double(nullable=true)\n
    | -- label_id: long(nullable=false)\n
    | -- start: timestamp(nullable=true)\n
    | -- end: timestamp(nullable=true)\n
    | -- label: string(nullable=true)\n
    """
    df = read_trajectory_dataset(spark, input_dir)
    df = filter_trajectory_bad_data(df)

    dfl = read_labels_dataset(spark, input_dir)
    dfl = filter_labels_bad_data(dfl)

    joined_df = join_trajectories_and_labels(df, dfl)
    return joined_df
