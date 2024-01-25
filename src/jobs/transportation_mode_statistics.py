from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import min, max, avg, sum, count

import src.shared.transform.geolife_transformer as transformer
from src.jobs.base import BaseJob
from src.shared.extract import geolife_extractor as extractor


class TransportationStatisticsJob(BaseJob):
    def __init__(self, input_dir: str, output_dir: str, debug: bool = False):
        super().__init__(input_dir, output_dir, debug)

    def run_job(self):
        conf = SparkConf(loadDefaults=True)
        spark = SparkSession.builder.appName(self._job_name).config(conf=conf).getOrCreate()

        print("Reading dataset")
        df = extractor.read_trajectories_with_labels(spark, self._input_dir)
        self._write_to_csv_debug(df, "dataset")
        self._show_debug(df)

        print("Calculating distance per segment")
        df = transformer.calculate_distance_and_duration_per_segment(df)
        self._write_to_csv_debug(df, "partitioned")
        self._show_debug(df)

        print("Calculation statistics per trip")
        df_values = self.__calculate_individual_trip_values(df)
        self._write_to_csv_debug(df_values, "trip_values")
        self._show_debug(df)

        print("Calculation statistics per transportation mode")
        df_statistics = self.__calculate_transportation_mode_statistics(df_values)
        self._write_to_csv(df_statistics, "transportation_statistics")
        self._show_debug(df)

        spark.stop()

    @staticmethod
    def __calculate_individual_trip_values(df: DataFrame):
        df = df.groupBy(["user_id", "date", "label", "label_id"]) \
            .agg(sum("dist_part_km").alias("total_distance_km"),
                 sum("time_part_h").alias("total_duration_h")) \
            .sort(["user_id", "date", "label", "label_id"])

        return df

    @staticmethod
    def __calculate_transportation_mode_statistics(df: DataFrame):
        df = df.groupBy(["user_id", "label"])\
            .agg(count("label_id").alias("number_of_trips"),
                 min("total_distance_km").alias("min_trip_km"),
                 max("total_distance_km").alias("max_trip_km"),
                 avg("total_distance_km").alias("avg_trip_km"),
                 max("total_duration_h").alias("max_trip_duration_h"),
                 min("total_duration_h").alias("min_trip_duration_h"),
                 avg("total_duration_h").alias("avg_trip_duration_h"))\
            .sort("user_id", "label")

        return df
