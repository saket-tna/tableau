from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext()

sqlContext = HiveContext(sc)

tableau_insights_cross_dsp_df = sqlContext.sql("Select * from saket.tableau_feed_insights_lu_tmp")

tableau_insights_cross_dsp_df.repartition(2).write.partitionBy('miq_advertiser_id','jarvis_campaign_id').save('s3://dwh-reports-data/tableau_cross_dsp_insights/tableau_feed_cross_dsp_lkup', mode='overwrite',compression="snappy")
