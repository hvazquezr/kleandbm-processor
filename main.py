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
    consumer.subscribe(['project-updates', 'table-updates', 'relationship-updates', 'node-updates', 'change-updates'])
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
    message_time = datetime.datetime.now(datetime.UTC)
    data = json.loads(msg.value().decode('utf-8'))
    entity_type = topic.replace("-updates", "")  # project, tables, relationship, node
    collection = db[entity_type]
    print(f"Topic: {topic} \n Data:{data}")
    if entity_type == "change": # If it's change just update the change collection.
        collection.update_one({"_id": data["id"]}, {"$set": data}, upsert=True)
    else:
        data['lastModified'] = message_time
        changeId = data.get("changeId")
        project_id = msg.key().decode('utf-8') 

        versions_collection = db[entity_type + "_versions"]
        changes_collection = db["change"]

        # Update the main collection (upsert=true creates new if non-existent)
        collection.update_one({"_id": data["id"]}, {"$set": data}, upsert=True)

        # Retrieve the updated object
        updated_object = collection.find_one({"_id": data["id"]})

        # Remove the _id field from the updated object
        updated_object.pop("_id", None)

        # Insert the updated object into the versions collection
        versions_collection.insert_one(updated_object)
        
        changes_collection.update_one(
            {'_id': changeId},
            {'$set': {'timestamp': message_time, 'projectId': project_id}},
            upsert=True
        )

if __name__ == "__main__":
    main()