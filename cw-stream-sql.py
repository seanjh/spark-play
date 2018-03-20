from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    ArrayType, FloatType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField('Timestamps', ArrayType(TimestampType())),
    StructField('Values', FloatType()),
])
ds = spark.readStream.schema(schema).json('streams/cw-fdw-cpu')
ds.show()
ds.createOrReplaceTempView('cw')

cpu = spark.sql(
    '''
    SELECT t.pos, timestamp(t.val) AS timestamp, c.val
    FROM cw
    LATERAL VIEW posexplode(Timestamps) t AS pos, val
    LATERAL VIEW posexplode(Values) c AS pos, val
    WHERE t.pos = c.pos
    ORDER BY timestamp
    '''
)

query = cpu.writeStream.outputMode('append').format('console').start()

query.awaitTermination()
