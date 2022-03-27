from kafka import KafkaProducer
import frappe
import json


def get_producer(settings_name):
    settings = frappe.get_cached_doc("Kafka Settings", settings_name)
    producer = KafkaProducer(
        bootstrap_servers=settings.bootstrap_servers,
        client_id=settings.client_id,
        value_serializer=lambda e: json.dumps(e).encode("ascii"),
        key_serializer=lambda e: json.dumps(e).encode("ascii"),
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=settings.get_password("api_key"),
        sasl_plain_password=settings.get_password("api_secret"),
    )
    return producer


def send_kafka(settings_name, topic, key, value):
    producer = get_producer(settings_name)
    future = (
        producer.send(topic=topic, key=key, value=value)
        .add_callback(on_send_success)
        .add_errback(on_send_error)
    )
    res = future.get(timeout=60)
    return res


def on_send_success(record_metadata):
    frappe.log_error(
        str(
            {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
            }
        )
    )


def on_send_error(excp):
    frappe.log_error(str(excp))
    # handle exception


# # produce asynchronously with callbacks
# producer.send("my-topic", b"raw_bytes").add_callback(on_send_success).add_errback(
#     on_send_error
# )
