import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Kafka configuration
kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}

my_name = "margosha"
topic_name = f"{my_name}_building_sensors"

# Create producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')
)

def generate_sensor_data():
    """Generate random sensor data"""
    return {
        "id": random.randint(1, 10),
        "temperature": round(random.uniform(-5, 35), 2),
        "humidity": round(random.uniform(0, 100), 2),
        "timestamp": datetime.now().isoformat()
    }

def main():
    print(f"Starting sensor data generation to topic: {topic_name}")
    
    try:
        while True:
            data = generate_sensor_data()
            
            # Send to Kafka
            producer.send(topic_name, key=str(data["id"]), value=data)
            
            print(f"Sent: {data}")
            time.sleep(5)  # Send data every 5 seconds
            
    except KeyboardInterrupt:
        print("Stopping data generation...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
    