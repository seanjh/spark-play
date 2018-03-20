from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.json('input/arrays.json')
df.createOrReplaceTempView('arrays')

r = spark.sql(
    '''
    SELECT foos.*, bars.*
    FROM arrays
    LATERAL VIEW posexplode(foo) foos AS pos, val
    LATERAL VIEW posexplode(bar) bars AS pos, val
    WHERE foos.pos = bars.pos
    '''
)
r.show()
