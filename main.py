from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient
import datetime
from config import get_settings
import json

def main():
    settings = get_settings()
    # MongoDB Setup
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    
    # Kafka Consumer Setup
    kafka_config = {
            'bootstrap.servers': settings.kafka_server,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': settings.kafka_username,
            'sasl.password': settings.kafka_password,
            'group.id': 'python_processor',
    }
    consumer = Consumer(kafka_config)
    consumer.subscribe(['project-updates', 'table-updates', 'relationship-updates', 'node-updates'])
    print(f"Ready to process Kafka messages from {settings.kafka_server} to {settings.mongo_db}.")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                process_message(msg, db)
    finally:
        consumer.close()

def process_message(msg, db):
    topic = msg.topic()
    data = json.loads(msg.value().decode('utf-8'))
    print(f"Topic: {topic} \n Data:{data}")
    changeId = data.get("changeId")
    project_id = msg.key().decode('utf-8') 
    entity_type = topic.replace("-updates", "")  # project, tables, relationship, node
    collection = db[entity_type]
    versions_collection = db[entity_type + "_versions"]
    changes_collection = db["change"]

    # Update the main collection (upsert=true creates new if non-existent)
    collection.update_one({"_id": data["id"]}, {"$set": data}, upsert=True)

    # Record changes to the versions collection
    version_record = {
        #"change_id": data["change_id"],
        "id": data["id"],
        "changeId": changeId,
        "changes": {k: v for k, v in data.items() if k not in ["_id", "changeId", "id"]}
    }
    versions_collection.insert_one(version_record)
    
    changes_collection.update_one(
        {'_id': changeId},
        {'$setOnInsert': {'timestamp': datetime.datetime.now(datetime.UTC), 'projectId': project_id}},
        upsert=True
    )

if __name__ == "__main__":
    main()