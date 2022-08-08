import json 
from kafka import KafkaConsumer
import boto3

if __name__ == '__main__':
    # Kafka Consumer 
    consumer = KafkaConsumer(
        'messages',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )
    for message in consumer:
        print(json.loads(message.value))

        s3 = boto3.resource('s3')
        s3object = s3.Object('basicaccountstack-pinterestdataeng-proje-datalake-tcvpj2nf0cpq', 'kafka_project/practice.json')
        s3object.put(
            Body=(bytes(json.dumps(json.loads(message.value)).encode('UTF-8')))
        )

    


