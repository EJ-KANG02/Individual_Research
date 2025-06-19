from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, DoubleType

# 좌표 군집화 함수
def round_coord(lat, lon):
    if lat is None or lon is None:
        return "null,null"
    return f"{round(lat, 2)},{round(lon, 2)}"

round_coord_udf = udf(round_coord, StringType())

# Kafka 메시지 스키마 정의
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("agent_id", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("timestamp", StringType())

# Spark 세션
spark = SparkSession.builder \
    .appName("GPS Stream with Grouping") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Kafka 스트림 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "gps-topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka value → JSON 파싱
json_df = df.selectExpr("CAST(value AS STRING)")
parsed_df = json_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 좌표 그룹화
grouped_df = parsed_df.withColumn("coord_group", round_coord_udf(col("latitude"), col("longitude")))

# 집계
agg_df = grouped_df.groupBy("agent_id", "coord_group").count()

# 콘솔 출력 (update 모드 사용)
query = agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

print("🔥 Streaming started:", query.status)

query.awaitTermination()
