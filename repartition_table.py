from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext()

hc = HiveContext(sc)

df = hc.sql("Select * from saket.report_geo_cross_dsp")
df.repartition("miq_advertiser_id", "jarvis_campaign_id","dayserial_numeric").write.partitionBy("miq_advertiser_id", "jarvis_campaign_id", "dayserial_numeric").mode("overwrite").parquet("s3://dwh-reports-data/tableau_cross_dsp_insights/report_geo/")

df = hc.sql("Select * from saket.report_sd_insights_cross_dsp")
df.repartition("miq_advertiser_id", "jarvis_campaign_id","dayserial_numeric").write.partitionBy("miq_advertiser_id", "jarvis_campaign_id", "dayserial_numeric").mode("overwrite").parquet("s3://dwh-reports-data/tableau_cross_dsp_insights/report_sitedomain_insights/")

df = hc.sql("Select * from saket.report_tableau_feed_cross_dsp")
df.repartition("miq_advertiser_id", "jarvis_campaign_id","dayserial_numeric").write.partitionBy("miq_advertiser_id", "jarvis_campaign_id", "dayserial_numeric").mode("overwrite").parquet("s3://dwh-reports-data/tableau_cross_dsp_insights/tableau_feed_insights/")

df = hc.sql("Select * from saket.report_timelag_tz_cross_dsp")
df.repartition("miq_advertiser_id", "jarvis_campaign_id","dayserial_numeric").write.partitionBy("miq_advertiser_id", "jarvis_campaign_id","dayserial_numeric").mode("overwrite").parquet("s3://dwh-reports-data/tableau_cross_dsp_insights/report_timelag_tz/")

df = hc.sql("Select * from saket.report_url_keyword_cross_dsp")
df.repartition("miq_advertiser_id", "jarvis_campaign_id","dayserial_numeric").write.partitionBy("miq_advertiser_id", "jarvis_campaign_id", "dayserial_numeric").mode("overwrite").parquet("s3://dwh-reports-data/tableau_cross_dsp_insights/url_keyword_sd/")

df = hc.sql("Select * from saket.tableau_feed_insights_lu_tmp")
df.repartition("miq_advertiser_id", "jarvis_campaign_id","dayserial_numeric").write.partitionBy("miq_advertiser_id", "jarvis_campaign_id", "dayserial_numeric").mode("overwrite").parquet("s3://dwh-reports-data/tableau_cross_dsp_insights/tableau_feed_cross_dsp_lkup/")
