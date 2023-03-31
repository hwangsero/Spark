import pyspark.sql.functions as f
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("wordCount sparkSql ver") \
        .getOrCreate()

    # load data
    df = ss.read.text("data/words.txt")

    # transform
    df = df.withColumn('word', f.explode(f.split(f.col('value'), ' '))).withColumn("count", f.lit(1)).groupby("word").sum()

    df.show()
    
