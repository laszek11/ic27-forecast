from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="silver.ic27_thursdays")
def ic27_thursdays():
    df = (
        spark.read.table("trains_arrivals_departures")
        .filter(F.dayofweek("departureDate") == 5)
        .withColumn("scheduledTime_local", F.from_utc_timestamp(F.to_timestamp("scheduledTime"), "Europe/Helsinki"))
        .withColumn("actualTime_local", F.from_utc_timestamp(F.to_timestamp("actualTime"), "Europe/Helsinki"))
    )
    return df