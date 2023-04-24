from typing import List
from datetime import datetime

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("rdd examples ver") \
        .getOrCreate()
    sc: SparkContext = ss.sparkContext
    log_rdd: RDD[str] = sc.textFile('data/log.txt')

    # rdd의 모든 row 출력
    #log_rdd.foreach(print)

    """
    1. map
    rdd의 각 row를 list[str]으로 변환
    """

    def parse_line(row: str):
        return row.strip().split(' | ')

    parsed_log_rdd: RDD[List[str]] = log_rdd.map(parse_line)

    """
    2. filter
    a. status code가 404만 필터
    
    """

    def get_only_404(row: List[str]) -> bool:
        return row[3] == '404'

    rdd_404 = parsed_log_rdd.filter(get_only_404)

    """
    b. status code가 정상인 것만 필터
    """

    def get_only_2xx(row: List[str]):
        return row[3].startswith("2")
    rdd_normal = parsed_log_rdd.filter(get_only_2xx)

    """
    c. method가 POST이고 /playbooks인 api만 필터링 
    """

    def get_post_request_and_playbooks_api(row: List[str]):
        request = row[2].replace('"',"")
        return request.startswith("POST") and "/playbooks" in request

    rdd_post_playbooks: RDD[List[str]] = parsed_log_rdd.filter(get_post_request_and_playbooks_api)

    """
    3. reduce
    a. API method별 갯수 출력
    """
    def extract_api_method(row: List[str]):
        request = row[2].replace('"',"")
        api_method = request.split(" ")[0]
        return api_method, 1

    rdd_count_by_api_method = parsed_log_rdd.map(extract_api_method)\
        .reduceByKey(lambda a, b: a + b)\
        .sortByKey()

    """
    b. API 시간의 분단위 몇개의 요청이 있는지 출력
    """

    def extract_hour_and_minute(row: List[str]):
        timestamp = row[1].replace("[", "").replace("]", "")
        date_format = "%d/%b/%Y:%H:%M:%S"
        date_time_obj = datetime.strptime(timestamp, date_format)
        return f"{date_time_obj.hour}:{date_time_obj.minute}", 1

    rdd_count_by_minute = parsed_log_rdd.map(extract_hour_and_minute)\
        .reduceByKey(lambda a, b: a + b)\
        .sortByKey()
    rdd_count_by_minute.foreach(print)

    """
    4. group by
    a. status code, api method 별 ip 리스트 출력
    """

    def extract_cols(row: List[str]) -> tuple[str, str, str]:
        ip = row[0]
        status_code = row[3]
        api_log = row[2].replace("\"", "")
        api_method = api_log.split(" ")[0]

        return status_code, api_method, ip

    # group by
    parsed_log_rdd.map(extract_cols) \
        .map(lambda x: ((x[0], x[1]), x[2])) \
        .groupByKey().mapValues(list).foreach(print)

    # reduce by
    parsed_log_rdd.map(extract_cols) \
        .map(lambda x: ((x[0], x[1]), x[2])) \
        .reduceByKey(lambda a, b: f"{a},{b}") \
        .map(lambda row: (row[0], row[1].split(","))).foreach(print)


