# --- 1. CREATE TOPICS SCRIPT ---

from kafka.admin import KafkaAdminClient, NewTopic

kafka_config = {
    "bootstrap_servers": ['77.81.230.104:9092'],
    "username": 'admin',
    "password": 'VawEzo1ikLtrA8Ug8THa',
    "security_protocol": 'SASL_PLAINTEXT',
    "sasl_mechanism": 'PLAIN'
}

my_name = "margosha"
topic_names = [
    f"{my_name}_building_sensors",
    f"{my_name}_temperature_alerts",
    f"{my_name}_humidity_alerts"
]

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    client_id="create_topics_script"
)

topics = [NewTopic(name=name, num_partitions=2, replication_factor=1) for name in topic_names]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created:", topic_names)
except Exception as e:
    print("Topics may already exist:", e)