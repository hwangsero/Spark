from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

if __name__ == "__main__":
    ss = SparkSession.builder\
        .master("local")\
        .appName("students")\
        .getOrCreate()

    schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("age", IntegerType()),
        StructField("gender", StringType()),
        StructField("major", StringType()),
        StructField("gpa", FloatType()),
        StructField("state", StringType()),
        StructField("scholarship", StringType()),
    ])

    students = ss.read.csv("data/students_extended.csv", header=True, schema=schema)

    # 1. 주(state)별 학생 수를 계산하세요.
    students.groupBy('state').count().show()

    # 2. 전공(major)별 장학금(scholarship)의 총액을 계산하세요.
    students.groupBy('major').agg(sum('scholarship').alias('sum_scholarship')).show()

    # 3. 평균 학점(GPA)이 3.5 이상인 학생들에게만 장학금이 지급되었는지 확인
    students.filter((students.gpa >= 3.5) & (students.scholarship == 0)).show()

    # 4. 각 주에서 가장 높은 학점(GPA)을 받은 학생의 이름 찾기
    window_spec = Window.partitionBy('state').orderBy(desc('gpa'))
    students.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).select("state", "name", "gpa").show()

    # 5. 각 전공별 학점(GPA)의 중앙값을 계산하세요.
    students.groupBy('major').agg(mean('gpa')).show()
