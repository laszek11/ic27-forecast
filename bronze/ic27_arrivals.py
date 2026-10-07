from pyspark import pipelines as dp
import requests
import pandas as pd
from datetime import date, timedelta


@dp.materialized_view(name="v_ic27_arrivals")
def v_ic27_arrivals():
    trainNumber = 27
    
    # fetching data for the last 180 days (we could filter only for thursdays, but in bronze we can store other days to analyze for other patterns)
    yesterday = date.today() - timedelta(days=1)
    start = date.today() - timedelta(days=180)

    frames = []
    departureDate = start
    while departureDate <= yesterday:
        url = f"https://rata.digitraffic.fi/api/v1/trains/{departureDate}/{trainNumber}"
        data = requests.get(url).json()

        if data:
            frames.append(pd.json_normalize(data, record_path="timeTableRows", meta=["departureDate", "trainNumber"]))

        departureDate = departureDate + timedelta(days=1)

    df = pd.concat(frames).query(
        "(stationShortCode == 'TPE' and type == 'ARRIVAL')"
    )

    columns = [
        "departureDate", "trainNumber", "stationShortCode", "type",
        "scheduledTime", "actualTime", "differenceInMinutes", "cancelled",
    ]
    return spark.createDataFrame(df[columns])


@dp.materialized_view(name="ic27_arrivals")
def trains_arrivals_departures():
    return spark.read.table("v_ic27_arrivals") 