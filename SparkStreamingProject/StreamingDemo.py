from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("streaming_app_demo").getOrCreate()
ssc = StreamingContext(spark.sparkContext, batchDuration=5)
# 5 - results will be printed after every 5 seconds (Micro-batch)

lines = ssc.socketTextStream("localhost", 9999)

words = lines.flatMap(lambda line : line.split(" "))
word_count = words.map(lambda word : (word, 1)).reduceByKey(lambda a,b : a + b)

word_count.pprint()

ssc.start()
ssc.awaitTermination()

# [4,5,7,8,2,3,4] - reduce
# 4 + 5 = 9
# 9 + 7 = 16
# 16 + 8 = 24

# [('ravi',1),('pooja',1),('ravi',1),('akash',1)] - reduceByKey
# 'ravi' = 1
# 'pooja' = 1
# 'ravi' = 1 + 1 = 2
# 'akash' = 1
