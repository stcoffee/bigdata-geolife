import datetime as dt

from pyspark.sql import DataFrame


class BaseJob:
    def __init__(self, input_dir: str, output_dir: str, write_intermediate: bool = False):
        self._input_dir = input_dir
        self._job_name = "{}_{}".format(self.__class__.__name__, dt.datetime.now().strftime("%y_%m_%dT%H_%M_%S"))
        self._output_dir = u"{}/{}".format(output_dir, self._job_name)
        self._write_intermediate_config = write_intermediate

    def _write_intermediate_to_csv(self, df: DataFrame, folder: str = None):
        if self._write_intermediate_config:
            df.show(20)
            if folder is not None:
                self._write_to_csv(df, folder)

    def _write_to_csv(self, df: DataFrame, folder: str = None):
        df.write.format("csv").save("{}/{}".format(self._output_dir, folder), header="true")
