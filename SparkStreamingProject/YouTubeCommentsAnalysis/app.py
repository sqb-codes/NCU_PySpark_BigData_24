# Flask application to serve comments to the browser
from flask import Flask, jsonify
from pyspark.sql import SparkSession

app = Flask(__name__)

spark = SparkSession.builder.appName("FlaskApp").getOrCreate()

@app.route("/comments", methods=["GET"])
def get_comments():
    comments_df = spark.sql("SELECT * FROM comments_table")
    comments = comments_df.collect()

    print(comments)
    comments_list = [{"word" : row.word, "count" : row["count"]} for row in comments]
    return jsonify(comments_list)

if __name__ == "__main__":
    app.run(port=5000)
