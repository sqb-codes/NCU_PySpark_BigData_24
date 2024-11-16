# PySpark Streaming job to read and process comments

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

source_dir = "tmp"
checkpoint_dir = "checkpoints"

def process_stream():
    spark = SparkSession.builder.appName("youtube-app").getOrCreate()
    comments_df = spark.readStream.text("tmp/comments*.txt")
    # comments_df = spark.readStream.format("txt").load(source_dir)

    words_df = comments_df.select(explode(split(comments_df.value, " ")).alias("word"))
    words_counts = words_df.groupBy("word").count()

    print("Words Data Frame...")
    print(words_df)

    # write the word counts to a memory table so that flask can access
    query = words_counts.writeStream.format("memory").queryName("comments_table").outputMode("complete").option("checkpointLocation", checkpoint_dir).start()

    query.awaitTermination()

process_stream()