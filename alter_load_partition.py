from datetime import datetime, timedelta
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import HiveContext

start_date = '2020-04-01'
start_date = datetime.strptime(start_date,'%Y-%m-%d')
end_date = start_date+timedelta(days=9)

sc = SparkContext()
hc = HiveContext(sc)

daterange = pd.date_range(start_date, end_date)

for i in daterange:
    print(i)

    query="Alter table saket.tableau_feed_insights add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)

    query="Alter table saket.googledbm_insights add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)

    query="Alter table saket.tableau_geo_report add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)

    query="Alter table saket.google_dbm_insights_geo add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)

    query="Alter table saket.tableau_feed_sitedomain_insights add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)

    query="Alter table saket.report_dbm_url_keword_wl add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)

    query="Alter table saket.report_timelag_tz add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)

    query="Alter table saket.report_timelag_dbm add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)

    query="Alter table saket.report_url_keword_wl add if not exists partition (day_numeric=\'"+i.strftime('%Y%m%d')+"\')"
    hc.sql(query)
