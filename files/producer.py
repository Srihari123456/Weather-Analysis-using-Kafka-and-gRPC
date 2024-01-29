from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from kafka.errors import TopicAlreadyExistsError
import time 
from weather import get_next_weather
from report_pb2 import Report
import calendar

broker = "localhost:9092"
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    print(admin_client.list_topics())
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

try:
    admin_client.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])
except TopicAlreadyExistsError:
    print("already exists")
print("Topics:", admin_client.list_topics())

producer = KafkaProducer(bootstrap_servers=[broker],retries=10,acks='all')
for date, degrees in get_next_weather(delay_sec=0.1):
    month = calendar.month_name[int(date.split("-")[1])]
    msg = Report(date=date, degrees=degrees)
    producer.send("temperatures", value=msg.SerializeToString(), key=bytes(month, "utf-8"))
    print(date, degrees, month) # date, max_temperature

