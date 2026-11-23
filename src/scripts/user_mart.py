"""Актуальный и домашний города, поездки и местное время пользователей."""

from pyspark.sql import Window, functions as F

from geo_common import latest_position, run


def build_mart(events, messages):
    order = Window.partitionBy("user_id").orderBy(
        F.asc("event_ts"), F.asc_nulls_first("message_id"), F.desc("lat"), F.desc("lon")
    )
    visits = (
        messages.withColumn("previous_zone", F.lag("zone_id").over(order))
        .withColumn("new_visit", F.when(F.col("zone_id").eqNullSafe(F.col("previous_zone")), 0).otherwise(1))
        .withColumn("visit_id", F.sum("new_visit").over(order.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("local_day", F.to_date(F.from_utc_timestamp("event_ts", F.col("timezone"))))
        .groupBy("user_id", "visit_id", "city")
        .agg(F.min("local_day").alias("first_day"), F.max("local_day").alias("last_day"))
    )
    # Между сообщениями могут быть паузы. Смена города завершает визит.
    home = (
        visits.where(F.datediff("last_day", "first_day") > 27)
        .groupBy("user_id")
        .agg(F.max(F.struct("visit_id", "city")).getField("city").alias("home_city"))
    )
    travel = visits.groupBy("user_id").agg(
        F.count("*").alias("travel_count"),
        F.transform(
            F.sort_array(F.collect_list(F.struct("visit_id", "city"))),
            lambda visit: visit["city"],
        ).alias("travel_array"),
    )
    active = latest_position(messages).select(
        "user_id", F.col("city").alias("act_city"),
        F.from_utc_timestamp("event_ts", F.col("timezone")).alias("local_time"),
    )
    return active.join(home, "user_id", "left").join(travel, "user_id").select(
        "user_id", "act_city", "home_city", "travel_count", "travel_array", "local_time"
    )


if __name__ == "__main__":
    run("users", build_mart)
