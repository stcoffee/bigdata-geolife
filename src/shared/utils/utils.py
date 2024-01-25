from pyspark.sql import Column
from pyspark.sql.functions import lit, acos, sin, radians, cos, when, unix_timestamp


def dist_in_km(long_x: Column | str, lat_x: Column | str, long_y: Column | str, lat_y: Column | str) -> Column:
    return (acos(
        sin(radians(lat_x)) * sin(radians(lat_y)) +
        cos(radians(lat_x)) * cos(radians(lat_y)) *
        cos(radians(long_x) - radians(long_y))
    ) * lit(6371.0)).alias("dist_in_km")


def datediff_in_hours(start: str | Column, end: str | Column) -> Column:
    return ((unix_timestamp(end) - unix_timestamp(start)) / lit(3600)).alias("datediff_in_seconds")


def calculate_speed(s: str | Column, t: str | Column) -> Column:
    return (when(t != lit(0), (s/t)).otherwise(None)).alias("speed")
