from pyspark.sql import SparkSession
from pyspark.sql.types import *


def load_data(ss: SparkSession, from_file, schema):
    if from_file:
        return ss.read.schema(schema).csv("data/log.csv")

    log_data_inmemory = [
        ["130.31.184.234", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["28.252.170.12", "2023-02-26 04:15:21", "GET", "/events", "401", 73],
        ["180.97.92.48", "2023-02-26 04:15:22", "POST", "/parsers", "503", 17],
        ["73.218.61.17", "2023-02-26 04:16:22", "DELETE", "/lists", "201", 91],
        ["24.15.193.50", "2023-02-26 04:17:23", "PUT", "/auth", "400", 24],
    ]

    return ss.createDataFrame(log_data_inmemory, schema)


if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("log sql ex") \
        .getOrCreate()

    from_file = True

    # define schema
    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),  # 단위 : milliseconds
    ])

    table_name = "log_data"
    load_data(ss, from_file, fields).createOrReplaceTempView(table_name)

    # 데이터 확인
    #ss.sql(f"SELECT * FROM {table_name}").show()

    # 컬럼 변환
    # a: 현재 latency 단위는 밀리세컨드 변경
    ss.sql(f"SELECT *, latency / 1000 as latency_seconds FROM {table_name}").show()

    # b: stringType인 timestamp 칼럼을 timestampType으로 변경
    ss.sql(f"""SELECT ip, TIMESTAMP(timestamp) AS timestamp, method, endpoint, status_code, latency "
           FROM {table_name}""")

    # filter
    # a: status_code = 400, endpoint = '/users' 인 경우만 출력
