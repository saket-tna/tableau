from pyspark.sql import HiveContext
from pyspark import SparkContext

sc = SparkContext()
sqlContext = HiveContext(sc)

tableau_inisghts_cross_dsp_df = sqlContext.sql("Select * from saket.tableau_feed_insights_cross_dsp")

tableau_inisghts_cross_dsp_df.repartition(2).write.partitionBy('agency_id','dayserial_numeric').save('s3://dwh-reports-data/saket/cross_dsp_reports/tableau_feed_insights_agency', mode='overwrite',compression="snappy")
