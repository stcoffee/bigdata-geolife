from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, avg, sum

from src.jobs.base import BaseJob
from src.shared.extract import geolife_extractor as extractor
from src.shared.transform import geolife_transformer as transformer


class DailyStatisticsJob(BaseJob):
    def __init__(self, input_dir: str, output_dir: str, debug: bool = False):
        super().__init__(input_dir, output_dir, debug)

    def run_job(self):
        conf = SparkConf(loadDefaults=True)
        spark = SparkSession.builder.appName(self._job_name).config(conf=conf).getOrCreate()

        print("Reading dataset")
        df = extractor.read_trajectories_with_labels(spark, self._input_dir)
        self._show_debug(df)
        self._write_to_csv_debug(df, "dataset")

        print("Calculating partial sums")
        df = transformer.calculate_partial_distances(df)
        self._show_debug(df)
        self._write_to_csv_debug(df, "partial")

        print("Calculating daily sums")
        df = self.__calculate_daily_values(df)
        self._show_debug(df)
        self._write_to_csv_debug(df, "daily_values")

        print("Calculating daily statistics")
        df = self.__calculate_daily_statistics(df)
        self._show_debug(df)
        self._write_to_csv(df, "daily_statistics")

        spark.stop()

    @staticmethod
    def __calculate_daily_values(df: DataFrame):
        df = df.groupBy(["user_id", "label", "date"]) \
            .agg(sum("dist_part_km").alias("daily_distance_km"),
                 sum("time_part_h").alias("daily_duration_h"))

        return df

    @staticmethod
    def __calculate_daily_statistics(df: DataFrame):
        df = df.groupBy(["user_id", "label"]) \
            .agg(max("daily_distance_km").alias("max_daily_distance_km"),
                 min("daily_distance_km").alias("min_daily_distance_km"),
                 avg("daily_distance_km").alias("avg_daily_distance_km"),
                 max("daily_duration_h").alias("max_daily_duration_h"),
                 min("daily_duration_h").alias("min_daily_duration_h"),
                 avg("daily_duration_h").alias("avg_daily_duration_h")) \
            .sort(["user_id", "label"])

        return df
