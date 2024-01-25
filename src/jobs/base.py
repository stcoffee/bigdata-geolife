import datetime as dt

from pyspark.sql import DataFrame


class BaseJob:
    def __init__(self, input_dir: str, output_dir: str, debug: bool = False):
        self._input_dir = input_dir
        self._job_name = "{}_{}".format(self.__class__.__name__, dt.datetime.now().strftime("%y_%m_%dT%H_%M_%S"))
        self._output_dir = u"{}/{}".format(output_dir, self._job_name)
        self._debug = debug
        self.__show_n = 20

    def _write_to_csv_debug(self, df: DataFrame, folder: str, header: bool = True):
        if self._debug and folder is not None:
            self._write_to_csv(df, folder, header)

    def _show_debug(self, df: DataFrame):
        if self._debug:
            df.show()

    def _write_to_csv(self, df: DataFrame, folder: str = None, header: bool = True):
        df.write.format("csv").save("{}/{}".format(self._output_dir, folder), header=header)
