# This file will load data from a set of files
# to kafka
import json
import os
import re
from time import sleep
import csv

from bytewax import parse
from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster, run_main
from bytewax.inputs import ManualInputConfig, distribute
from bytewax.outputs import KafkaOutputConfig, StdOutputConfig

from kafka.admin import KafkaAdminClient, NewTopic


def input_builder(worker_index, worker_count, resume_state):
    state = None  # ignore recovery
    with open("test/all.csv", "+r") as f:
        reader_obj = csv.reader(f)
        next(reader_obj)
        for line in reader_obj:
            sensor = line.pop()
            match = re.match('^.+?((\-?|\+?)?\d+(\.\d+)?) \s*((\-?|\+?)?\d+(\.\d+)?).+?$',
                   sensor)
            sensor_key = ", ".join([match[1],match[4]])
            yield state, (sensor_key, line)

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
    run_main(flow)
