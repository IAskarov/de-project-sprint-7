"""Недельные и месячные показатели по городам."""

from pyspark.sql import functions as F

from geo_common import latest_position, run

METRICS = ("message", "reaction", "subscription", "user")


def counts(prefix):
    return [F.count(F.when(F.col("event_type") == kind, 1)).alias(f"{prefix}_{kind}")
            for kind in METRICS]


def build_mart(events, messages):
    message_events = messages.select(
        "zone_id", "event_ts", F.lit("message").alias("event_type")
    )
    registrations = (
        events.where((F.col("event_type") == "message") & F.col("event_ts").isNotNull())
        .groupBy("user_id")
        .agg(F.min("event_ts").alias("event_ts"))
        .withColumn("event_type", F.lit("user"))
    )
    non_messages = events.where(
        (F.col("event_type") != "message") & F.col("event_ts").isNotNull()
    ).select("user_id", "event_ts", "event_type")
    inferred = (
        non_messages.unionByName(registrations)
        .join(latest_position(messages).select("user_id", "zone_id"), "user_id")
        .select("zone_id", "event_ts", "event_type")
    )
    calendar = (
        message_events.unionByName(inferred)
        .withColumn("month", F.trunc(F.to_date("event_ts"), "month"))
        .withColumn("week", F.trunc(F.to_date("event_ts"), "week"))
    )
    weekly = calendar.groupBy("week", "zone_id").agg(*counts("week"))
    monthly = calendar.groupBy("month", "zone_id").agg(*counts("month"))
    # Для пограничной недели показываем полный недельный итог в каждом затронутом месяце.
    return (
        calendar.select("month", "week", "zone_id").distinct()
        .join(weekly, ["week", "zone_id"])
        .join(monthly, ["month", "zone_id"])
        .select("month", "week", "zone_id",
                *(f"week_{kind}" for kind in METRICS),
                *(f"month_{kind}" for kind in METRICS))
    )


if __name__ == "__main__":
    run("zones", build_mart)
