from kafka import KafkaAdminClient, KafkaConsumer
from report_pb2 import Report
import sys
from kafka import TopicPartition
from collections import defaultdict

broker = "localhost:9092"

import os
import json
data_dict = {}
offset_dict = {}

def check_partition_N_json(N):
    file_path = './files/partition-'+str(N)+'.json'

    if not os.path.exists(file_path):
        initial_data = {"partition": N, "offset": 0}
        with open(file_path, 'w') as file:
            json.dump(initial_data, file)
            print(f"Created and initialized {file_path} with default values.")
    else:
        print(f"{file_path} already exists.")
   
    partition_num = N

    with open(file_path, 'r') as file:
        partition_info = json.load(file)
        offset = partition_info.get('offset', 0)
        print("Partition to continue reading from =",offset)
        data_dict[partition_num] = partition_info

    return offset

def updateOffsetInFile(N, offset):
    file_path = './files/partition-'+str(N)+'.json'

    with open(file_path, 'r') as file:
        data = json.load(file)
    data["offset"] = offset

    with open(file_path+'.tmp', 'w') as file:
        json.dump(data, file, indent=4)
        print("Atomic Write")
        os.rename(file_path+'.tmp',file_path)

def update_statistics(temp_dict, N): 
    file_name = './files/partition-'+str(N)+'.json'
    try:
        with open(file_name, 'r') as file:
            partition_info = json.load(file)
    except FileNotFoundError:
        partition_info = {'partition': partition_number, 'offset': 0}

    month = temp_dict['key']
    year = temp_dict['date'][:4]
    temperature = temp_dict['degrees']
    incoming_date = temp_dict['date']

    month_data = partition_info.setdefault(month, defaultdict(dict))
    if year not in month_data:
        month_data[year] = {}
    year_data = month_data[year]
    if 'end' in year_data and incoming_date <= year_data['end']:
        print("Duplicate value detected")
        return
    year_data['count'] = year_data.get('count', 0) + 1
    year_data['sum'] = year_data.get('sum', 0) + temperature
    year_data['avg'] = year_data.get('sum',0) / year_data.get('count',1)
    
    if 'start' not in year_data or incoming_date < year_data['start']:
        #year.pop('start_date')
        year_data['start'] = incoming_date
    if 'end' not in year_data or incoming_date > year_data['end']:
        #del year['end_date']
        year_data['end'] = incoming_date
    print("Updated statistics")

    with open(file_name+'.tmp', 'w') as file:
        json.dump(partition_info, file, indent=4)
        print("Atomic Write")
        os.rename(file_name+'.tmp',file_name)

def report_consumer(partitions=[]):
    counts = {}
    consumer = KafkaConsumer(bootstrap_servers=[broker])
    consumer.assign([TopicPartition("temperatures", p) for p in partitions])

    for tp in consumer.assignment():
        offsetToSeek = check_partition_N_json(tp.partition)
        print("Topic partition for partition "+str(tp.partition))
        print("Offset of this partition to seek = "+str(offsetToSeek))
        consumer.seek(tp, offsetToSeek)

    #consumer.seek_to_beginning()
    while True:
        batch = consumer.poll(1000)
        for tp, messages in batch.items():
            for msg in messages:
                temp_dict = {}
                report = Report.FromString(msg.value)
                temp_dict["partition"] = tp.partition
                temp_dict["key"] = str(msg.key,"utf-8")
                temp_dict["date"] = report.date
                temp_dict["degrees"] = report.degrees   
                
                print("Partition = ",tp.partition,", printing report = ",report)
                update_statistics(temp_dict, tp.partition)

            print("For partition "+str(tp.partition)+" the completion offset = "+str(consumer.position(tp)))
            data_dict[tp.partition] = consumer.position(tp)
            updateOffsetInFile(tp.partition, consumer.position(tp))

# 2 dictionaries
# data dictionary [partition_key] --> [data in the partition <ever growing>]
# the format of this data is the one used in the debug.py
# offset dictionary [partition_key] --> [offset]

if __name__ == "__main__":
    args = sys.argv[1:]
    partitions = []
    for arg in args:
        try:
            partitions.append(int(arg))
        except ValueError:
            print(f"Skipping non-integer argument: {arg}")

    report_consumer(partitions)
