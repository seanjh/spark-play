from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet('output/cw-cpu.parquet')
df.createOrReplaceTempView('cw_cpu')
df.show()

agg = spark.sql(
    '''
    SELECT
        date(c.timestamp) AS day,
        avg(c.val) AS avg_val
    FROM cw_cpu AS c
    GROUP BY day
    ORDER BY day
    '''
)
agg.write.format('parquet').mode('overwrite').save('output/cw-cpu-daily.parquet')
agg.show()
