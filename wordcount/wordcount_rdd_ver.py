from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == '__main__':
    print(1)
    ss: SparkSession = SparkSession.builder\
        .appName("wordCount RDD ver")\
        .getOrCreate()
    sc: SparkContext = ss.sparkContext

    # tansform
    text_file: RDD[str] = sc.textFile('data/words.txt')

    """
    This blog is exclusive for all the people who are interested in learning one of the trending in current IT industry.

    This -> (This, 1)
    
    """
    print(2)
    counts = text_file.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda count1, count2: count1 + count2)

    # action
    output = counts.collect()

    for (word, count) in output:
        print(f"{word}:{count}")

