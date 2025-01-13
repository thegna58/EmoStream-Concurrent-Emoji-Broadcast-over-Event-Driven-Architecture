from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, from_json, to_timestamp, explode, current_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, ArrayType
from kafka import KafkaProducer
import json
import time

def create_spark_session():
    return SparkSession.builder \
        .appName("EmojiReactionStreamProcessor") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.kafka.consumer.cache.enabled", "false") \
        .getOrCreate()

def send_to_kafka_partition(partition):
    """Initialize Kafka producer and send data for each partition."""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    try:
        for row in partition:
            result = {
                "window_start": row["window"].start.isoformat(),
                "window_end": row["window"].end.isoformat(),
                "emoji_type": row["emoji_type"],
                "count": row["count"]
            }
            producer.send('processed_emoji_data', value=result)
            print(f"Sent to Kafka: {result}")
    except Exception as e:
        print(f"Error sending to Kafka: {str(e)}")
    finally:
        producer.close()

def main():
    try:
        # Step 1: Initialize Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("ERROR")
        print("\u2705 Spark session initialized successfully")

        # Step 2: Define schema for incoming messages
        message_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("emoji_type", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        wrapper_schema = ArrayType(message_schema)
        print("\u2705 Schema defined for incoming messages")

        # Step 3: Create streaming DataFrame from Kafka
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", 'emoji_topic') \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        print("\u2705 Connected to Kafka stream")

        # Step 4: Process the raw messages
        raw_messages = kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        print("\u2705 Debugging raw messages")
        
        debug_query = raw_messages \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        # Step 5: Parse JSON data and handle array structure
        parsed_df = raw_messages \
            .select(from_json(col("value"), wrapper_schema).alias("data")) \
            .select(explode("data").alias("emoji_data")) \
            .select("emoji_data.*")

        # Step 6: Handle timestamp with watermark for late data
        df_with_time = parsed_df \
            .withColumn("event_timestamp", 
                       to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
            .withWatermark("event_timestamp", "5 seconds")

        # Step 7: Create sliding windows for real-time aggregation
        emoji_counts = df_with_time \
            .groupBy(
                window(col("event_timestamp"), "10 seconds", "5 seconds"),
                "emoji_type"
            ) \
            .count()
        print("\u2705 Created windowed aggregations")

        # Step 8: Filter and sort results
        filtered_emojis = emoji_counts \
            .filter(col("count") > 1)

        # Step 9: Write to Kafka and Console
        # Create Kafka sink query
        kafka_sink_query = filtered_emojis \
            .writeStream \
            .foreachBatch(lambda df, epoch_id: df.foreachPartition(send_to_kafka_partition)) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoint_kafka") \
            .start()

        # Create Console sink query
        console_query = filtered_emojis \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .trigger(processingTime='5 seconds') \
            .option("truncate", "false") \
            .option("numRows", 10) \
            .option("checkpointLocation", "/tmp/checkpoint_console") \
            .start()
        
        print("\u2705 Started streaming queries")

        # Step 10: Print processing details
        print("\n\U0001f504 Stream Processing Details:")
        print(f"Debug Query ID: {debug_query.id}")
        print(f"Console Query ID: {console_query.id}")
        print(f"Kafka Sink Query ID: {kafka_sink_query.id}")

        # Step 11: Monitor stream status with improved error handling
        while True:
            try:
                if not (console_query.isActive and kafka_sink_query.isActive):
                    print("\u274c One or more queries stopped unexpectedly")
                    break
                    
                print(f"\U0001f504 Active queries: {len(spark.streams.active)}")
                print(f"Console query status: {console_query.status}")
                print(f"Kafka sink status: {kafka_sink_query.status}")
                
                # Additional check for query health
                if console_query.lastProgress:
                    print(f"Processed rows: {console_query.lastProgress['numInputRows']}")
                
                time.sleep(5)
                    
            except Exception as e:
                print(f"\u274c Stream monitoring error: {str(e)}")
                break

    except Exception as e:
        print(f"\n\u274c Error occurred: [{e.__class__.__name__}] {str(e)}")
        print("\n\U0001f527 Troubleshooting steps:")
        print("1. Verify Kafka is running:")
        print("   nc -z localhost 9092")
        print("2. Check Kafka topics:")
        print("   kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic emoji_topic")
        print("   kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic processed_emoji_data")
        print("3. Monitor processed data:")
        print("   kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic processed_emoji_data --from-beginning")

    finally:
        if 'debug_query' in locals():
            debug_query.stop()
        if 'console_query' in locals():
            console_query.stop()
        if 'kafka_sink_query' in locals():
            kafka_sink_query.stop()
        if 'spark' in locals():
            spark.stop()
        print("\u2705 Shutdown complete")

if __name__ == "__main__":
    main()
