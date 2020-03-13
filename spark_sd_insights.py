from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext()

sqlContext = HiveContext(sc)

tableau_inisghts_cross_dsp_df = sqlContext.sql("Select * from saket.report_sd_insights_cross_dsp")

tableau_inisghts_cross_dsp_df.repartition(2).write.partitionBy('jarvis_campaign_id','dayserial_numeric').save('s3://dwh-reports-data/tableau_cross_dsp_insights/report_sitedomain_insights', mode='overwrite',compression="snappy")
