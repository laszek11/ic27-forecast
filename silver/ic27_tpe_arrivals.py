from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="silver.ic27_tpe_arrivals")
def ic27_tpe_arrivals():
    df = (
        spark.read.table("bronze.ic27_arrivals")
        .withColumn("scheduledTime_local", F.from_utc_timestamp(F.to_timestamp("scheduledTime"), "Europe/Helsinki"))
        .withColumn("actualTime_local", F.from_utc_timestamp(F.to_timestamp("actualTime"), "Europe/Helsinki"))
    )
    return df