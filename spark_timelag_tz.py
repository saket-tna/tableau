from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext()

sqlContext = HiveContext(sc)

tableau_inisghts_cross_dsp_df = sqlContext.sql("Select * from saket.report_timelag_tz_cross_dsp")

tableau_inisghts_cross_dsp_df.repartition(2).write.partitionBy('jarvis_campaign_id').save('s3://dwh-reports-data/tableau_cross_dsp_insights/report_timelag_tz', mode='overwrite',compression="snappy")
