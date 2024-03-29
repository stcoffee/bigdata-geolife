from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import lead, isnan

import src.shared.utils.utils as utils


def calculate_partial_distances(df: DataFrame) -> DataFrame:
    # We want to partition the data by one individual label,
    # label id to differentiate between two same labels one after the other
    partition = [df.user_id, df.label, df.label_id]
    window_spec = Window.partitionBy(partition).orderBy(df.datetime)
    df = df.select(df.user_id, df.label, df.label_id, df.latitude, df.longitude, df.date, df.datetime,
                   lead(df.latitude).over(window_spec).alias("next_latitude"),
                   lead(df.longitude).over(window_spec).alias("next_longitude"),
                   lead(df.datetime).over(window_spec).alias("next_datetime"))

    next_point_not_null = (df.next_latitude.isNotNull() & df.next_longitude.isNotNull())
    next_datetime_not_null = (df.next_datetime.isNotNull() & df.next_datetime.isNotNull())
    point_not_same_as_next_point = ((df.latitude != df.next_latitude) | (df.longitude != df.next_longitude))
    df = df.filter(next_point_not_null & next_datetime_not_null & point_not_same_as_next_point)

    df = df.select(df.user_id, df.label, df.label_id, df.latitude, df.longitude, df.date, df.datetime,
                   df.next_latitude, df.next_longitude, df.next_datetime,
                   utils.dist_in_km(df.longitude, df.latitude, df.next_longitude, df.next_latitude).alias("dist_part_km"),
                   utils.datediff_in_hours(df.datetime, df.next_datetime).alias("time_part_h"))

    valid_distance = (df.dist_part_km.isNotNull() & (~isnan(df.dist_part_km)))
    valid_time = (df.time_part_h.isNotNull() & (~isnan(df.time_part_h)))
    df = df.filter(valid_distance & valid_time)

    return df
