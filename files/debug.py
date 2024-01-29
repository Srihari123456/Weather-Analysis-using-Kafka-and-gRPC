from kafka import KafkaConsumer, KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError
from kafka.errors import TopicAlreadyExistsError
from report_pb2 import Report



broker = "localhost:9092"
admin_client = KafkaAdminClient(bootstrap_servers=[broker])
#print("Topics:", admin_client.list_topics())
consumer = KafkaConsumer(bootstrap_servers=[broker],group_id="debug")
#print("Consumer assignment is", consumer.assignment())

consumer.subscribe(["temperatures"])
batch = consumer.poll(1000)

#If we run producer.py messages get stored on Kafka broker. Then if we shut producer.py and run debug.py,
# we do not see any messages printed. This is because Kafka consumer reads from the current batch and does not want to historically
# catch up on data.
# If we do consumer.seek_to_beginning() we can see all messages

while True:
    batch = consumer.poll(1000)
    for topic_partition, messages in batch.items():
        for msg in messages:
            s = Report.FromString(msg.value)
            #message_dict = {}
            print({"partition":msg.partition, "key":str(msg.key, "utf-8"), "date":s.date, "degrees":s.degrees})

