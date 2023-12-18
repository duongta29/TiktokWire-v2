from kafka import KafkaConsumer
from kafka import TopicPartition
import pickle
import ast

bootstrap_servers = '10.11.101.129:9092'
topic_name = 'osint-posts-update'
partition_number = 0
consumer = KafkaConsumer(
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',
    enable_auto_commit=False
)

topic_partition = TopicPartition(topic_name, partition_number)
consumer.assign([topic_partition])
json_list = []

i = 0 

while True:
    consumer.seek(topic_partition, i)
    for message in consumer:
        data = pickle.loads(message.value)
        if isinstance(data, list):
            with open('data_update.txt', 'a', encoding='utf-8') as f:
                for item in data:
                    data2 = ast.literal_eval(str(item))
                    if 'tiktok' in data2['type']:
                        print("❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤")
                        f.write(f'{item}\n')
        else:
            with open('data_update.txt', 'a', encoding='utf-8') as f:
                data2 = ast.literal_eval(str(data)) 
                if 'tiktok' in data2['type']:
                    print("❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤❤")
                    f.write(f'{data}\n')

        i = i + 1

consumer.close()
