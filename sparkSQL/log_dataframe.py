from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions \
    import col, to_timestamp, max, min, mean, date_trunc, collect_set, \
    hour, minute, count


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
        .appName("log dataframe ex") \
        .getOrCreate()

    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", IntegerType(), False),
    ])
    log_data_inmemory = [
        ["130.31.184.234", "2023-02-26 04:15:21", "PATCH", "/users", "400", 61],
        ["28.252.170.12", "2023-02-26 04:15:21", "GET", "/events", "401", 73],
        ["180.97.92.48", "2023-02-26 04:15:22", "POST", "/parsers", "503", 17],
        ["73.218.61.17", "2023-02-26 04:16:22", "DELETE", "/lists", "201", 91],
        ["24.15.193.50", "2023-02-26 04:17:23", "PUT", "/auth", "400", 24],
    ]

    df = ss.createDataFrame(log_data_inmemory, schema=fields)

    # 컬럼 변환
    # a: 현재 latency 단위는 밀리세컨드 -> 초단위로 변경
    def millisecond_to_second(latency):
        return latency / 1000

    df = df.withColumn(
        "latency_seconds",
       # millisecond_to_second(col("latency"))
        millisecond_to_second(df.latency)
    )


    # b: stringType인 timestamp 칼럼을 timestampType으로 변경

    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )

    # filter
    # a: status_code = 400, endpoint = '/users' 인 경우만 출력
    df.filter((col("status_code") == "400") & (col("endpoint") == "/users"))

    # group by
    # a: method, endpoint 별 latency의 최소, 최대, 평균값
    group_cols = ["method", "endpoint"]
    df.groupBy(group_cols).agg(min("latency").alias("min_latency"),
                               max("latency").alias("max_latency"),
                               mean("latency").alias("mean_latency"))

    # b: 분 단위의 중복을 제거한 ip 리스트, 개수 뽑기
    group_cols = ["hour", "minute"]

    df.withColumn(
        "hour", hour(date_trunc("hour", col("timestamp")))
    ).withColumn(
        "minute", minute(date_trunc("minute", col("timestamp")))
    ).groupBy(group_cols).agg(collect_set("ip").alias("ip_list"),
                              count("ip").alias("ip_count")).sort(group_cols).explain()