from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext()

sqlContext = HiveContext(sc)

tableau_insights_cross_dsp_df = sqlContext.sql("Select * from saket.report_url_keyword_cross_dsp")

tableau_insights_cross_dsp_df.repartition(2).write.partitionBy('miq_advertiser_id', 'jarvis_campaign_id','dayserial_numeric').save('s3://dwh-reports-data/tableau_cross_dsp_insights/url_keyword_sd', mode='overwrite',compression="snappy")