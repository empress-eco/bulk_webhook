from kafka import KafkaProducer
from kafka.errors import KafkaError
import frappe
import json


def get_producer(settings_name):
    settings = frappe.get_cached_doc("Kafka Settings", settings_name)
    producer = KafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        client_id=settings.client_id,
        value_serializer=lambda e: json.dumps(e).encode("ascii"),
        key_serializer=lambda e: json.dumps(e).encode("ascii"),
        retries=3,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAN",
        sasl_plain_username=settings.get_password("api_key"),
        sasl_plain_password=settings.get_password("api_secret"),
    )
    return producer


def send_kafka(settings_name, topic, key, value):
    producer = get_producer(settings_name)
    res = producer.send(topic=topic, key=key, value=value)
    producer.flush()
    return res


# def on_send_success(record_metadata):
#     print(record_metadata.topic)
#     print(record_metadata.partition)
#     print(record_metadata.offset)


# def on_send_error(excp):
#     log.error("I am an errback", exc_info=excp)
#     # handle exception


# # produce asynchronously with callbacks
# producer.send("my-topic", b"raw_bytes").add_callback(on_send_success).add_errback(
#     on_send_error
# )
