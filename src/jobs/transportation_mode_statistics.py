from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
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

        print("Calculating partial sums")
        df = transformer.calculate_partial_distances(df)
        self._write_to_csv_debug(df, "partial_distances")
        self._show_debug(df)

        print("Calculating sums per trip")
        df = self.__calculate_distances_per_trip(df)
        self._write_to_csv_debug(df, "trip_distances")
        self._show_debug(df)

        print("Calculating statistics per transportation mode")
        df = self.__calculate_transportation_mode_statistics(df)
        self._write_to_csv(df, "transportation_statistics")
        self._show_debug(df)

        spark.stop()

    @staticmethod
    def __calculate_distances_per_trip(df: DataFrame):
        df = df.groupBy(["user_id", "label", "label_id"]) \
            .agg(sum("dist_part_km").alias("total_distance_km"),
                 sum("time_part_h").alias("total_duration_h"))
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
