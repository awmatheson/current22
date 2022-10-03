# This file will load data from a set of files
# to kafka
import json
import os
import re
from time import sleep

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig, distribute
from bytewax.outputs import KafkaOutputConfig

from kafka.admin import KafkaAdminClient, NewTopic


all_files = os.listdir("./test")    
CSV_FILES = list(filter(lambda f: f.endswith('.csv') and 'test' not in f, all_files))

def process_files(workers_files, state):
    for file_name in workers_files:
        match = re.match('^.+?((\-?|\+?)?\d+(\.\d+)?) \s*((\-?|\+?)?\d+(\.\d+)?).+?$',
                   file_name)
        sensor_key = ", ".join([match[1],match[4]])
        print(sensor_key)
        with open(f"test/{file_name}") as lines:
            state = state or 0
            for i, line in enumerate(lines):
                if i < 1:
                    continue
                state += 1
                line = line.strip().rstrip(',')
                sleep(0.01)
                yield (state, (str(sensor_key), line.split(',')))

def input_builder(worker_index, worker_count, resume_state):
    state = resume_state or None
    workers_files = distribute(CSV_FILES, worker_index, worker_count)
    return process_files(workers_files, state)

def serialize(line):
    key, data = line
    new_key_bytes = json.dumps(key).encode('utf-8')
    headers = ["created_at","entry_id","PM1.0_CF1_ug/m3","PM2.5_CF1_ug/m3","PM10.0_CF1_ug/m3","UptimeMinutes","RSSI_dbm","Temperature_F","Humidity","PM2.5_ATM_ug/m3"]
    try:
        data = {headers[i]: data[i] for i in range(len(data))}
    except IndexError:
        print(headers)
        print(data)
    return new_key_bytes, json.dumps(data).encode('utf-8')

flow = Dataflow()
flow.input("file_input", ManualInputConfig(input_builder))
flow.map(serialize)
flow.capture(
    # StdOutputConfig()
    KafkaOutputConfig(
        brokers=["localhost:9092"],
        topic="sensor_data",
    )
)

if __name__ == "__main__":
    
    # Use the kafka admin client to create the topic
    input_topic_name = "sensor_data"
    localhost_bootstrap_server = "localhost:9092"
    admin = KafkaAdminClient(bootstrap_servers=[localhost_bootstrap_server])

    # Create input topic
    try:
        input_topic = NewTopic(input_topic_name, num_partitions=3, replication_factor=1)
        admin.create_topics([input_topic])
        print(f"input topic {input_topic_name} created successfully")
    except:
        print(f"Topic {input_topic_name} already exists")

    # run the dataflow    
    spawn_cluster(flow, **parse.cluster_args())