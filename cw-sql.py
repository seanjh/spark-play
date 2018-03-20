from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.json('input/cw-fdw-cpu-2.json')
df.createOrReplaceTempView('cw')

cpu = spark.sql(
    '''
    SELECT t.pos, timestamp(t.val / 1000) AS timestamp, c.val
    FROM cw
    LATERAL VIEW posexplode(MetricData[0].Timestamps) t AS pos, val
    LATERAL VIEW posexplode(MetricData[0].Values) c AS pos, val
    WHERE t.pos = c.pos
    ORDER BY timestamp
    '''
)
cpu.write.format('parquet').mode('overwrite').save('output/cw-cpu.parquet')
cpu.show()
