from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
"""
리플랙션: 스키마 정의를 추가하지 않아도 컬랙션에 포함된 오브젝트의 속성값으로 알아서 스키마 정보를 추출하고 데이터프레임을 만드는 방법
"""
spark = SparkSession.builder.appName("sample").master("local[*]").getOrCreate()
row1 = Row(name="name1", age=7, job="student")
row2 = Row(name="name2", age=7, job="student")
data = [row1, row2]
df = spark.createDataFrame(data)
df.show()

"""
명시적 타입 지정을 통한 데이터 프레임 생성
"""
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("job", StringType(), True),
])
row1 = Row(name="name1", age=7, job="student")
row2 = Row(name="name2", age=7, job="student")
data = [row1, row2]
df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()


