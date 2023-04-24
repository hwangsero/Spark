from pyspark import SparkContext, RDD
from typing import List, Tuple
from pyspark.sql import SparkSession

def parse_line(row: str):
    return row.strip().split(',')

def scoring(row: Tuple[int, Tuple[int, int]]):
    course = row[0]
    pes = row[1][1] / row[1][0]
    if pes >= 0.9:
        return (course,10)
    elif pes >= 0.5:
        return (course,4)
    elif pes >= 0.25:
        return (course,2)
    return (course, 0)

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local") \
        .appName("my_practical") \
        .getOrCreate()
    sc: SparkContext = ss.sparkContext

    rdd_views: RDD[str] = sc.textFile("data/views*")
    rdd_views = rdd_views.distinct()
    parsed_rdd_views: RDD[List[str]] = rdd_views.map(parse_line)
    rdd_view_chapter_key = parsed_rdd_views.map(lambda x: (x[1], x[0]))

    rdd_chapters: RDD[str] = sc.textFile("data/chapters.csv")
    parsed_rdd_chapters: RDD[List[str]] = rdd_chapters.map(parse_line)

    rdd_titles: RDD[str] = sc.textFile("data/titles.csv")
    parsed_rdd_titles: RDD[List[str]] = rdd_titles.map(parse_line)

    # 챕터, 코스
    rdd_chapters_chapter_key = parsed_rdd_chapters.map(lambda x: (x[1], x[0]))

    # 코스, 카운트
    total_count_by_course: RDD[Tuple[int, int]] = parsed_rdd_chapters.map(lambda x: (x[0],1)).reduceByKey(lambda a, b: a + b)

    # 챕터, (학생, 코스)
    joined_rdd = rdd_view_chapter_key.join(rdd_chapters_chapter_key)
    # (학생, 코스) 챕터 수
    joined_rdd = joined_rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

    # (코스, 챕터 수)
    joined_rdd = joined_rdd.map(lambda x: (x[0][1], x[1]))
    # 코스, (챕터 수, 전체 챕터 수)
    joined_rdd = joined_rdd.join(joined_rdd)
    score_rdd = joined_rdd.map(scoring)
    avg_rdd = score_rdd.groupByKey().mapValues(lambda x: sum(x) / len(x))

    result_rdd = avg_rdd.join(parsed_rdd_titles)
    result_rdd = result_rdd.map(lambda x: (x[1][1], x[1][0]))
    result = result_rdd.collect()
    result.sort(key=lambda x: x[1], reverse=True)
    print(result)