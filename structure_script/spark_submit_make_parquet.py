#!/usr/bin/env python

# spark-submit job for creating parquet tables from raw json

from pyspark.sql.functions import explode
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local[*]')
spark = SparkSession(sc)

raw_data = spark.read.json("s3a://meetupevents/fix2017/*/*/*")
filtered_events = raw_data.filter('id IS NOT NULL')
filtered_events.cache()


# events table
events = filtered_events.selectExpr(
                                        "description",
                                        "duration",
                                        "event_url",
                                        "group.id AS group_id",
                                        "id AS event_id",
                                        "mtime",
                                        "name AS event_name",
                                        "payment_required AS paid_event",
                                        "status",
                                        "time",
                                        "utc_offset",
                                        "venue.name AS venue_name",
                                        "yes_rsvp_count")

events.write.parquet("s3a://meetupevents/parquet-ss/events", mode="overwrite")


# categories table
categories = filtered_events.selectExpr(
                                        "group.category.id AS category_id",
                                        "group.category.name AS category_name",
                                        "group.category.shortname AS category_shortname")

categories.write.parquet("s3a://meetupevents/parquet-ss/categories", mode="overwrite")


# groups table
groups = filtered_events.selectExpr(
                                        "group.city AS group_city",
                                        "group.country AS group_country",
                                        "group.group_lat",
                                        "group.group_lon",
                                        "group.id AS group_id",
                                        "group.name AS group_name",
                                        "group.state AS group_state",
                                        "group.category.id AS category_id")

groups.write.parquet("s3a://meetupevents/parquet-ss/groups", mode="overwrite")

# venues table
venues = filtered_events.selectExpr(
                                        "venue.address_1 AS venue_add1",
                                        "venue.address_2 AS venue_add2",
                                        "venue.city AS venue_city",
                                        "venue.country AS venue_country",
                                        "venue.lat AS venue_lat",
                                        "venue.lon AS venue_lon",
                                        "venue.name AS venue_name",
                                        "venue.phone AS venue_phone",
                                        "venue.state AS venue_state")

venues.write.parquet("s3a://meetupevents/parquet-ss/venues", mode="overwrite")

sc.stop()
