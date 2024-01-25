import time

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import min, max, avg, sum, count

import src.shared.transform.geolife_transformer as transformer
from src.jobs.base import BaseJob
from src.shared.extract import geolife_extractor as extractor


class TripStatisticsJob(BaseJob):
    def __init__(self, input_dir: str, output_dir: str, write_intermediate: bool = False):
        super().__init__(input_dir, output_dir, write_intermediate)

    def run_job(self):
        start_time = time.time()

        conf = SparkConf(loadDefaults=True)
        spark = SparkSession.builder.appName(self._job_name).config(conf=conf).getOrCreate()

        df = extractor.read_trajectories_with_labels(spark, self._input_dir)
        self._write_intermediate_to_csv(df, "dataset")

        df = transformer.calculate_distance_and_duration_per_segment(df)
        self._write_intermediate_to_csv(df, "partitioned")

        df_values = self.__calculate_individual_trip_values(df)
        self._write_to_csv(df_values, "trip_values")

        df_statistics = self.__calculate_individual_trip_statistics(df_values)
        self._write_to_csv(df_statistics, "trip_statistics")

        spark.stop()
        print(f"Execution time {time.time() - start_time}s")

    @staticmethod
    def __calculate_individual_trip_values(df: DataFrame):
        df = df.groupBy(["user_id", "date", "label", "label_id"]) \
            .agg(sum("dist_part_km").alias("total_distance_km"),
                 sum("time_part_h").alias("total_duration_h")) \
            .sort(["user_id", "date", "label", "label_id"])

        return df

    @staticmethod
    def __calculate_individual_trip_statistics(df: DataFrame):
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
