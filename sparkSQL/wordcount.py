from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 1. 스파크세션 생성
spark = SparkSession.builder.appName("sample").master("local[*]").getOrCreate()
"""
만약 추가적인 설정이 필요하다면 빌더가 제공하는 config() 메서드를 이용해 config("spark.driver.host", "127.0.0.1")과 같은 형태로 지정할 수 있다.
"""

# 2. 스파크세션으로부터 데이터셋 또는 데이터프레임 생성
source = "wordcount/data/words.txt"
df = spark.read.text(source)

# 3. 생성된 데이터프레임 또는 데이터셋을 이용해 데이터 처리
# .alias를 통해 칼럼명이 "word"로 변경됨
word_df = df.select(explode(split(col("value"), " ")).alias("word"))
result = word_df.groupBy("word").count
print(1)