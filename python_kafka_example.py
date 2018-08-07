import json as j

# Kafka Should Be Up and Running on your localhost 9092
# Follow This link to do it... 
# Refrence: 
# https://tecadmin.net/install-apache-kafka-ubuntu/

# Kafka Config
kafka_ip = "localhost"
kafka_host = "localhost:9092"
kafka_timeout = 5000
auto_offset_reset = 'earliest'
enable_auto_commit = True

# Kafka Config
from kafka import BrokerConnection, SimpleClient, KeyedProducer, SimpleConsumer
import time

host = kafka_ip  # Only IP
kafka_host = kafka_host  # ip + port
kafka_broker = BrokerConnection(host, 9092, afi=2, client_id="prd_valid_client", reconnect_backoff_ms=30,
                                reconnect_backoff_max_ms=1000,
                                max_in_flight_requests_per_connection=100, api_version=(0, 10))

# Create A Global Connection -- With the Kafka Client, retry every 2Sec.
i = 0
while True:
    if not kafka_broker.connected():
        i += 1
        try:
            # Async Connection
            print("Trying Kafka Connection..." + str(i))
            kafka_broker.connect()
        except Exception as e:
            kafka_broker.connect_blocking(10000)
    else:
        print("Kafka Connected!" + str(kafka_broker.connected()))
        break
    time.sleep(2)

# Simple Kafka Global Client For Producer And Consumer, with timeout of 10Sec
client = SimpleClient(kafka_host, timeout=10000)
# A Global Producer... which uses the cient to connect it, in async mode.
producer = KeyedProducer(client, async_send=False,
                         req_acks=KeyedProducer.ACK_AFTER_LOCAL_WRITE,
                         ack_timeout=5000,
                         sync_fail_on_error=False)


def push_to_kafka(data, topic_name):
    # Simple Def, to push the data (JSON), data to the given topic
    print("kafka " + str(data))
    # Make sure, auto topic creation is Turn ON -- Kafka Setting
    topic = strip_spaces_with_underscore(topic_name)  # testTopic
    # To send messages synchronously, retry to send a message 3 times.
    for i in range(0, 2):
        try:
            tt = producer.send_messages(topic, str.encode(topic_name, errors="ignore"),
                                        str.encode(j.dumps(data), errors="ignore"))
            for r in tt:
                print(r)
        except Exception as e:
            print("Error in push_to_kafka. Retrying -- " + str(i))
            continue
        break
    # try:
    #     tt = producer.send_messages(topic, str.encode(topic_name, errors="ignore"),
    #                                 str.encode(j.dumps(data), errors="ignore"))
    #     for r in tt:
    #         print(r)
    # except Exception as e:
    #     print("Error in push_to_kafka.")
    return 'SUCCESS'


def pull_from_kafka(topic_name):
    MSG_PULL_COUNT = 1  # Number, of Message to pull at ones.
    # Pull The Kafka Message from the given topic, return the JSON
    try:
        topic = strip_spaces_with_underscore(topic_name)
        # Simple Consumer to consume data, using client
        consumer = SimpleConsumer(client, "test-consumer-group", topic, auto_offset_reset=auto_offset_reset)
        final_dict = {}
        try:
            for partition, msg in consumer.get_messages(count=MSG_PULL_COUNT, timeout=10000, block=False):
                final_dict.update({"key": msg.key.decode('utf-8'), "value": msg.value.decode('utf-8')})
                consumer.commit()
        except Exception as e:
            print("Error in pull_from_kafka [consumer].")
            final_dict.update({"value": str({})})
        if final_dict is None or final_dict == {}:
            final_dict.update({"value": str({})})
        aa = final_dict.get("value")
        return j.loads(aa)
    except Exception as e:
        aa = {}
        print("Error in pull_from_kafka.")
        return aa


def strip_spaces_with_underscore(s):
    # No special Characters Are Allowed in Topic Name
    return re.sub(r'[^A-Za-z0-9]+', '', s)

