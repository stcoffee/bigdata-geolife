from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame

from src.jobs.base import BaseJob
from src.shared.extract import geolife_extractor as extractor


class TransportationModesJob(BaseJob):
    def __init__(self, input_dir: str, output_dir: str, debug: bool = False):
        super().__init__(input_dir, output_dir, debug)

    def run_job(self):
        conf = SparkConf(loadDefaults=True)
        spark = SparkSession.builder.appName(self._job_name).config(conf=conf).getOrCreate()

        print("Reading dataset")
        df = extractor.read_labels_dataset(spark, self._input_dir)
        df = extractor.filter_labels_bad_data(df)
        self._show_debug(df)
        self._write_to_csv_debug(df, "dataset")

        modes = self.__find_distinct_modes(df)
        self._show_debug(modes)
        self._write_to_csv(modes, "transportation_modes", header=False)

        spark.stop()

    @staticmethod
    def __find_distinct_modes(df: DataFrame) -> DataFrame:
        df = df.select(df.label)\
            .distinct()\
            .sort(df.label)
        return df
