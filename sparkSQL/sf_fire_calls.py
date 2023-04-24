from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# define schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log dataframe ex") \
        .getOrCreate()

    df = ss.read.schema(fire_schema).csv("data/sf-fire-calls.csv", header=True)

    # Q-1) 2018년(CallDate)에 왔던 신고 전화들의 모든 유형(CallType)들 나열하고,
    df = df.withColumn('CallDateYear', year(to_timestamp(col('CallDate'), 'MM/dd/yyyy')))\
        .filter(col('CallDateYear') == '2018')
    df.select('CallType').where(col('CallType').isNotNull()).groupby('CallType').count().orderBy('count',
                                                                                                 ascending=False).show()
    # Q-2) 2018년의 각 달(month)별 신고 수 확인 후, 가장 신고 수가 많은 달 확인하기.
    df = df.withColumn('CallDateMonth', month(to_timestamp(col('CallDate'), 'MM/dd/yyyy')))
    df.select('CallDateMonth').groupby('CallDateMonth').count().orderBy('count', ascending=False).show()

    # Q-3) 2018년에 가장 많은 신고가 들어온 샌프란시스코 지역은?
    df.where(col('City') == "San Francisco").groupby("Address").count().orderBy("count", ascending=False).show()


    # Q-4) 2018년에 샌프란시스코의 이웃 지역 중 응답 시간이 가장 느린 다섯 곳은?
    df.select('Neighborhood', 'Delay').where(col('City') == "San Francisco").orderBy('Delay', ascending=False).take(5)
    # Q-5) 2018년 데이터를 parquet 형태로 저장한 후 다시 불러오기.

    # write
    df.write.format("parquet").mode("overwrite").save("data/2018-sf-fire-calls.parquet")
    # read
    parquet_df = ss.read.format("parquet").load("data/2018-sf-fire-calls.parquet")
