from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

# Kafka configuration
kafka_config = {
    "kafka.bootstrap.servers": "77.81.230.104:9092",
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";'
}

my_name = "margosha"
input_topic = f"{my_name}_building_sensors"
temp_alert_topic = f"{my_name}_temperature_alerts"
humidity_alert_topic = f"{my_name}_humidity_alerts"

# Create Spark session
spark = SparkSession.builder \
    .appName("IoTAlertsProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def load_alert_conditions():
    """Load alert conditions from CSV file"""
    pdf = pd.read_csv("alerts_conditions.csv")
    
    # Convert to Spark DataFrame
    schema = StructType([
        StructField("condition_id", IntegerType(), True),  # Змінено ім'я для уникнення конфлікту
        StructField("humidity_min", IntegerType(), True),
        StructField("humidity_max", IntegerType(), True),
        StructField("temperature_min", IntegerType(), True),
        StructField("temperature_max", IntegerType(), True),
        StructField("code", IntegerType(), True),
        StructField("message", StringType(), True)
    ])
    
    return spark.createDataFrame(pdf.values.tolist(), schema)

def process_alerts():
    """Main processing function"""
    
    # Load alert conditions
    alert_conditions = load_alert_conditions()
    print("Alert conditions loaded:")
    alert_conditions.show()
    
    # Read from Kafka
    sensor_stream = spark.readStream \
        .format("kafka") \
        .options(**kafka_config) \
        .option("subscribe", input_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    sensor_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    parsed_data = sensor_stream.select(
        from_json(col("value").cast("string"), sensor_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Convert timestamp
    parsed_data = parsed_data.withColumn(
        "event_time", 
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    )
    
    # Apply windowing and aggregation
    windowed_data = parsed_data \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds"),
            col("id")
        ) \
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            count("*").alias("readings_count")
        ) \
        .select(
            col("id").alias("sensor_id"),  # Перейменовуємо одразу
            col("avg_temperature"),
            col("avg_humidity"),
            col("readings_count"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end")
        )
    
    # Cross join with alert conditions and filter
    alerts = windowed_data.crossJoin(alert_conditions)
    
    # Build complex filtering condition
    alert_condition = (
        # Temperature conditions
        ((col("temperature_min") != -999) & (col("avg_temperature") < col("temperature_min"))) |
        ((col("temperature_max") != -999) & (col("avg_temperature") > col("temperature_max"))) |
        # Humidity conditions  
        ((col("humidity_min") != -999) & (col("avg_humidity") < col("humidity_min"))) |
        ((col("humidity_max") != -999) & (col("avg_humidity") > col("humidity_max")))
    )
    
    filtered_alerts = alerts.filter(alert_condition)
    
    # Prepare alert messages
    alert_messages = filtered_alerts.select(
        col("sensor_id"),
        col("avg_temperature"),
        col("avg_humidity"),
        col("condition_id"),
        col("code").alias("alert_code"),
        col("message").alias("alert_message"),
        col("window_start"),
        col("window_end"),
        current_timestamp().alias("alert_timestamp")
    )
    
    # Convert to JSON for Kafka output
    kafka_output = alert_messages.select(
        col("sensor_id").cast("string").alias("key"),
        to_json(struct("*")).alias("value")
    )
    
    # Write to Kafka (temperature alerts)
    temp_query = kafka_output \
        .writeStream \
        .format("kafka") \
        .options(**kafka_config) \
        .option("topic", temp_alert_topic) \
        .option("checkpointLocation", "/tmp/checkpoint_temp") \
        .outputMode("append") \
        .start()
    
    # Write to console for debugging
    console_query = alert_messages \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    return temp_query, console_query

def main():
    """Main execution function"""
    try:
        print("Starting Spark Streaming application...")
        
        temp_query, console_query = process_alerts()
        
        print("Queries started. Waiting for data...")
        
        # Wait for termination
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()