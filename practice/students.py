from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    ss = SparkSession.builder\
        .master("local")\
        .appName("students")\
        .getOrCreate()

    schema = StructType([
        StructField("id", IntegerType())
        , StructField("name", StringType())
        , StructField("age", IntegerType())
        , StructField("gender", StringType())
        , StructField("major", StringType())
        , StructField("gpa", FloatType())
    ])

    # 1. PySpark를 사용하여 데이터를 읽어들이세요.
    students = ss.read.csv("data/students.csv", header=True, schema=schema)

    # 2. 나이별 학생 수를 계산하세요.
    students.groupBy(['age']).agg(count("id"))
    students.groupBy(['age']).count()

    # 3. 성별 별로 전공(major)의 분포를 계산하세요.
    students.groupBy(['gender', 'major']).count()

    # 4. 전공별 평균 학점(GPA)을 계산하세요.
    students.groupBy(['major']).agg(avg("gpa"))

    # 5. 학점(GPA)이 3.5 이상인 학생들의 이름을 출력하세요.
    students.filter(col('gpa') > 3.5).select("name").show()
    students.filter(students.gpa > 3.5).select("name").show()
