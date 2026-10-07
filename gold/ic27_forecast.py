from pyspark import pipelines as dp

@dp.materialized_view(name='gold.ic27_forecast')
def ic27_forecast():
    return spark.sql("""     
        WITH base AS (
          SELECT
            *,
            row_number() OVER (ORDER BY departureDate DESC) AS rn   /* last thursday's scheduled time (in case of a change in timetable) */
          FROM silver.ic27_tpe_arrivals
          WHERE cancelled = false
            AND actualTime_local IS NOT NULL
        ),
        stats AS (
          SELECT
            max(CASE WHEN rn = 1 THEN scheduledTime_local END) AS last_scheduled,
            count(*)                                           AS days,
            round(avg(differenceInMinutes), 1)                 AS avg_delay,
            percentile_approx(differenceInMinutes, 0.5)        AS median_delay, /* median delay will show the most accurate middle figure for the delay, taking huge delays out of the calculation */
            max(differenceInMinutes)                           AS max_delay,
            round(avg(CASE
                    WHEN date_format(actualTime_local, 'HH:mm') <= '16:00'
                    THEN 1 ELSE 0 END) * 100, 0)               AS pct_on_time_by_1600 /* target time on the event is 16:15, 0:15h transfer, 16:00 MAX arrival */
          FROM base
        )
        SELECT
          date_add(to_date(date_trunc('week', current_date())), 10)                AS forecast_date,
          date_format(last_scheduled, 'HH:mm')                                     AS scheduled_arrival,
          date_format(timestampadd(MINUTE, median_delay, last_scheduled), 'HH:mm') AS forecast_arrival,
          days,
          avg_delay,
          median_delay,
          max_delay,
          pct_on_time_by_1600,
          CASE WHEN pct_on_time_by_1600 >= 80 /* I set 80% as a threshold */
               THEN 'IC27 is fine'
               ELSE 'Take an earlier train'
          END                                                                      AS recommendation
        FROM stats
    """)   