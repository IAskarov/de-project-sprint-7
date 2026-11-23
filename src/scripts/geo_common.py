"""Чтение источников, георазметка сообщений и параметры расчёта."""

import argparse
import logging
from datetime import date

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StructType

EVENTS_PATH = "/user/master/data/geo/events"
ROOT = "/user/master/data/geo/sprint7"
CITY_TIMEZONES = {
    "Sydney": (1, 8, 9, 10, 23),
    "Melbourne": (2, 7, 11, 18, 19),
    "Brisbane": (3, 6, 13, 14, 15, 16, 21, 22),
    "Perth": (4, 24),
    "Adelaide": (5,),
    "Hobart": (12, 20),
    "Darwin": (17,),
}


def distance_km(lat1, lon1, lat2, lon2):
    """Формула гаверсинуса для координат в градусах."""
    lat1, lon1, lat2, lon2 = (F.radians(value) for value in (lat1, lon1, lat2, lon2))
    haversine = (
        F.pow(F.sin((lat2 - lat1) / 2), 2)
        + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin((lon2 - lon1) / 2), 2)
    )
    clamped = F.when(haversine > 1, 1).when(haversine < 0, 0).otherwise(haversine)
    return 2 * 6371 * F.asin(F.sqrt(clamped))


def latest_position(messages):
    latest = Window.partitionBy("user_id").orderBy(
        F.desc("event_ts"), F.desc_nulls_last("message_id"),
        F.asc("lat"), F.asc("lon"),
    )
    return messages.withColumn("_rank", F.row_number().over(latest)).where("_rank = 1").drop("_rank")


class GeoLake:
    def __init__(self, spark, options):
        self.spark = spark
        self.options = options

    def events(self):
        raw = self.spark.read.parquet(self.options.events_path)
        partition = F.col("date")
        raw = raw.where(partition <= self.options.as_of.isoformat())
        if self.options.history_start:
            raw = raw.where(partition >= self.options.history_start.isoformat())
        event_schema = raw.schema["event"].dataType
        if not isinstance(event_schema, StructType):
            raise ValueError("В источнике ожидается вложенная структура event")
        fields = set(event_schema.fieldNames())

        def nested(name):
            return F.col("event." + name) if name in fields else F.lit(None)

        kind = F.col("event_type")
        sender = nested("message_from")
        actor = (
            F.when(kind == "message", sender)
            .when(kind == "reaction", F.coalesce(nested("reaction_from"), nested("user")))
            .otherwise(F.coalesce(nested("subscription_user"), nested("user")))
        )
        event_ts = F.when(
            kind == "message", F.coalesce(nested("message_ts"), nested("datetime"))
        ).otherwise(F.coalesce(nested("datetime"), nested("message_ts")))
        return raw.where(kind.isin("message", "reaction", "subscription")).select(
            kind.alias("event_type"),
            actor.cast("long").alias("user_id"),
            nested("message_to").cast("long").alias("recipient_id"),
            nested("message_id").cast("long").alias("message_id"),
            nested("subscription_channel").cast("string").alias("channel_id"),
            event_ts.cast("timestamp").alias("event_ts"),
            F.col("lat").cast("double").alias("lat"),
            F.col("lon").cast("double").alias("lon"),
        ).where(F.col("user_id").isNotNull())

    def cities(self):
        csv = self.spark.read.option("header", True).option("sep", ";").csv(self.options.cities_path)
        cities = csv.select(
            F.col("id").cast("int").alias("zone_id"),
            F.col("city").alias("city"),
            F.regexp_replace("lat", ",", ".").cast("double").alias("city_lat"),
            F.regexp_replace("lng", ",", ".").cast("double").alias("city_lon"),
        )
        timezones = self.spark.createDataFrame(
            [(city_id, "Australia/" + region)
             for region, city_ids in CITY_TIMEZONES.items() for city_id in city_ids],
            "zone_id int, timezone string",
        )
        catalog = cities.join(F.broadcast(timezones), "zone_id", "left")
        invalid = (
            F.col("zone_id").isNull() | F.col("city").isNull()
            | F.col("timezone").isNull() | (~F.col("city_lat").between(-90, 90))
            | (~F.col("city_lon").between(-180, 180))
            | F.col("city_lat").isNull() | F.col("city_lon").isNull()
        )
        if not catalog.take(1) or catalog.where(invalid).take(1):
            raise ValueError("Проверьте координаты и соответствия zone_id/часового пояса в geo.csv")
        return catalog

    def located_messages(self, events):
        messages = events.where(
            (F.col("event_type") == "message")
            & F.col("event_ts").isNotNull()
            & F.col("lat").between(-90, 90)
            & F.col("lon").between(-180, 180)
        ).select("user_id", "message_id", "event_ts", "lat", "lon")
        # Один ближайший город на уникальную пару координат. При равенстве берём меньший ID.
        points = messages.select("lat", "lon").distinct()
        candidates = points.crossJoin(F.broadcast(self.cities()))
        nearest = candidates.groupBy("lat", "lon").agg(
            F.min(F.struct(
                distance_km(F.col("lat"), F.col("lon"), F.col("city_lat"), F.col("city_lon"))
                .alias("distance"),
                "zone_id", "city", "timezone",
            )).alias("nearest")
        ).select("lat", "lon", "nearest.zone_id", "nearest.city", "nearest.timezone")
        return messages.join(nearest, ["lat", "lon"])

    def write(self, frame, mart):
        destination = f"{self.options.output_root.rstrip('/')}/{mart}/date={self.options.as_of}"
        logging.info("Запись %s в %s", mart, destination)
        frame.write.mode("overwrite").parquet(destination)


def run(mart, build_mart):
    parser = argparse.ArgumentParser(description=f"Расчёт витрины {mart}")
    parser.add_argument("--as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--history-start", type=date.fromisoformat)
    parser.add_argument("--events-path", default=EVENTS_PATH)
    parser.add_argument("--cities-path", default=f"{ROOT}/reference/geo.csv")
    parser.add_argument("--output-root", default=f"{ROOT}/analytics")
    options = parser.parse_args()
    if options.history_start and options.history_start > options.as_of:
        parser.error("--history-start должна быть не позже --as-of")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = SparkSession.builder.appName(f"sprint7_{mart}").config(
        "spark.sql.session.timeZone", "UTC"
    ).getOrCreate()
    try:
        lake = GeoLake(spark, options)
        events = lake.events().persist()
        messages = lake.located_messages(events).persist()
        lake.write(build_mart(events, messages), mart)
    finally:
        spark.stop()
