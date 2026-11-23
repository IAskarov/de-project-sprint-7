"""Уникальные пары подписчиков, которым можно рекомендовать друг друга."""

from pyspark.sql import functions as F

from geo_common import distance_km, latest_position, run


def build_mart(events, messages):
    channels = (
        events.where((F.col("event_type") == "subscription") & F.col("channel_id").isNotNull())
        .select("user_id", "channel_id").distinct()
    )
    positions = latest_position(messages).select(
        "user_id", "zone_id", "lat", "lon", "timezone", "event_ts"
    )
    members = channels.join(positions, "user_id").withColumn(
        "lat_band", F.floor(F.col("lat") / F.lit(0.01))
    )
    # В пределах 1 км широты различаются менее чем на 0,01°. Достаточно соседних полос.
    left = members.withColumn(
        "match_band", F.explode(F.array(F.col("lat_band") - 1, F.col("lat_band"), F.col("lat_band") + 1))
    ).alias("left")
    right = members.alias("right")
    pairs = (
        left.join(
            right,
            (F.col("left.channel_id") == F.col("right.channel_id"))
            & (F.col("left.match_band") == F.col("right.lat_band"))
            & (F.col("left.user_id") < F.col("right.user_id")),
        )
        .where(distance_km(F.col("left.lat"), F.col("left.lon"),
                           F.col("right.lat"), F.col("right.lon")) <= 1)
        .select(
            F.col("left.user_id").alias("user_left"),
            F.col("right.user_id").alias("user_right"),
            F.col("left.zone_id").alias("zone_id"),
            F.from_utc_timestamp(F.col("left.event_ts"), F.col("left.timezone")).alias("local_time"),
        )
        .dropDuplicates(["user_left", "user_right"])
    )
    correspondence = events.where(
        (F.col("event_type") == "message") & F.col("recipient_id").isNotNull()
    ).select(
        F.least("user_id", "recipient_id").alias("user_left"),
        F.greatest("user_id", "recipient_id").alias("user_right"),
    ).distinct()
    return pairs.join(correspondence, ["user_left", "user_right"], "left_anti").select(
        "user_left", "user_right", F.current_timestamp().alias("processed_dttm"),
        "zone_id", "local_time",
    )


if __name__ == "__main__":
    run("friends", build_mart)
