import time

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import min, max, avg, sum, count

import src.shared.transform.geolife_transformer as transformer
from src.jobs.base import BaseJob
from src.shared.extract import geolife_extractor as extractor


class DailyStatisticsJob(BaseJob):
    def __init__(self, input_dir: str, output_dir: str, write_intermediate: bool = False):
        super().__init__(input_dir, output_dir, write_intermediate)

    def run_job(self):
        conf = SparkConf(loadDefaults=True)
        spark = SparkSession.builder.appName(self._job_name).config(conf=conf).getOrCreate()

        start_time = time.time()

        df = extractor.read_trajectories_with_labels(spark, self._input_dir)
        self._write_intermediate_to_csv(df, "dataset")

        df = transformer.calculate_distance_and_duration_per_segment(df)
        self._write_intermediate_to_csv(df, "partitioned")

        final_df = self.__calculate_daily_statistics(df)
        self._write_to_csv(final_df, "daily_statistics")

        print(f"Execution time {time.time() - start_time}s")
        spark.stop()

    @staticmethod
    def __calculate_daily_statistics(df: DataFrame):
        df = df.groupBy(["user_id", "date", "label"]) \
            .agg(sum("dist_part_km").alias("daily_distance_km"),
                 sum("time_part_h").alias("daily_duration_h"))

        df = df.groupBy(["user_id", "label"]) \
            .agg(max("daily_distance_km").alias("max_daily_distance_km"),
                 min("daily_distance_km").alias("min_daily_distance_km"),
                 avg("daily_distance_km").alias("avg_daily_distance_km"),
                 max("daily_duration_h").alias("max_daily_duration_h"),
                 min("daily_duration_h").alias("min_daily_duration_h"),
                 avg("daily_duration_h").alias("avg_daily_duration_h")) \
            .sort(["user_id", "label"])

        return df
