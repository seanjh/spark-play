from pyspark.sql import SparkSession
from pyspark.sql.functions import col, posexplode

spark = SparkSession.builder.getOrCreate()

df = spark.read.json('input/cw-fdw-cpu-2.json')
df.createOrReplaceTempView('cw')

ts = df.select(
    posexplode(df['MetricData'][0]['Timestamps']).alias('pos', 'rawts'),
).select(
    'pos',
    (col('rawts') / 1000).cast('timestamp').alias('ts'),
)
vals = df.select(
    posexplode(df['MetricData'][0]['Values']).alias('pos', 'val'),
)

cpu = ts.join(vals, 'pos').drop('pos').orderBy('ts')
cpu.write.format('parquet').mode('overwrite').save('output/cw-cpu.parquet')
cpu.show()
