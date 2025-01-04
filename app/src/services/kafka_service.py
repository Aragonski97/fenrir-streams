import ast
import asyncio
import json
from json import JSONDecodeError

import structlog
from anyio import value
from confluent_kafka import Consumer, Producer
import struct
from confluent_kafka.schema_registry import SchemaRegistryClient
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import TopicPartition
import fastavro
from io import BytesIO


from fenrir_streams.schemas.app_config import app_config


class KafkaSuite:

    def __init__(self, partition_id: int):

        self.logger = structlog.get_logger()
        self.registry_client = SchemaRegistryClient(conf={"url":app_config.registry_url})
        self.partition_id = partition_id

        self.input_schema_raw = self.registry_client.get_latest_version(f"{app_config.input_topic}-value")
        self.input_schema_id = self.input_schema_raw.schema_id
        self.input_schema_json = json.loads(self.input_schema_raw.schema.schema_str)

        self.output_schema_raw = self.registry_client.get_latest_version(f"{app_config.output_topic}-value")
        self.output_schema_json = json.loads(self.output_schema_raw.schema.schema_str)
        self.output_schema_id = self.output_schema_raw.schema_id

        if self.input_schema_raw.schema.schema_type.lower() == "avro":
            self.parsed_schema = fastavro.parse_schema(self.input_schema_json)
        else:
            self.parsed_schema = self.input_schema_json

        self.avro_parsed_output_schema = fastavro.parse_schema(self.output_schema_json)
        self.success_producer = Producer({
            'bootstrap.servers': app_config.kafka_bootstrap_server,
            'acks': 0,
            'linger.ms': 100,
            'batch.size': 20000
        })
        self.fail_producer = Producer({
            'bootstrap.servers': app_config.kafka_bootstrap_server,
            'acks': 0,
            'linger.ms': 100,
            'batch.size': 20000
        })
        self.avro_consumer = Consumer({
                    'bootstrap.servers': app_config.kafka_bootstrap_server,
                    'group.id': app_config.input_topic+"_103",
                    'auto.offset.reset': 'earliest',
                    'enable.auto.commit': 'false',
                    'connections.max.idle.ms': 2 ** 31 - 1,
                })
        self.avro_consumer.assign([TopicPartition(topic=app_config.input_topic, partition=partition_id)])

    def pause(self, worker_id: int):
        self.avro_consumer.pause(partitions=[TopicPartition(topic=app_config.input_topic, partition=worker_id)])
        return

    def resume(self, worker_id: int):
        self.avro_consumer.resume(partitions=[TopicPartition(topic=app_config.input_topic, partition=worker_id)])
        return

    async def get_message(self, extra_logging: dict = None) -> tuple:
        extra_logging = extra_logging or {}
        while True:
            try:
                msg = await asyncio.get_running_loop().run_in_executor(None, self.avro_consumer.poll, 1000)
                if msg is None:
                    return None, None
                if msg.error():
                    self.logger.error(
                        "generate_payload",
                        desc='error_consuming',
                        msg_error=msg.error(),
                        msg=msg.value(),
                        **extra_logging
                    )
                    return None, None
                self.avro_consumer.commit()
                string_key = msg.key()
                try:
                    key_decoded = json.loads(string_key) if string_key else None
                    # magic byte, confluent wire format
                    value = msg.value()[5:]
                    if self.input_schema_raw.schema.schema_type.lower() == "avro":
                        value_decoded = fastavro.schemaless_reader(BytesIO(value), self.input_schema_json)
                    else:
                        try:
                            value_decoded = json.loads(value)
                        except JSONDecodeError as err:
                            self.logger.warning(
                                "json deserialization",
                                err=err,
                                action="defaulting to ast.literal_eval"
                            )
                            try:
                                value_decoded = ast.literal_eval(value)
                            except Exception as err:
                                self.logger.warning(
                                    "json deserialization",
                                    err=err,
                                    action="ast.literal_eval has failed, skipping"
                                )
                                return None, None
                    return key_decoded, value_decoded

                except Exception as err:
                    try:
                        self.logger.warning(
                            "key value deserialization, msg included",
                            msg=str(string_key),
                            value=str(msg.value()),
                            err=err,
                            action="returning nones",
                            **extra_logging
                        )
                    except Exception as err:
                        self.logger.warning(
                            "key value deserialization, message exluded",
                            err=err,
                            action="returning nones",
                            **extra_logging
                        )
                    return None, None
            except Exception as err:
                self.logger.warning(event="generating payload", err=err, **extra_logging)
            return None, None

    def deploy_success_payload(self, key, value) -> None:
        try:
            key_encoded = key.encode('utf-8')
            value_encoded = BytesIO()
            fastavro.schemaless_writer(value_encoded, self.output_schema_json, record=value)
            value_encoded.seek(0)
            value_bytes = value_encoded.getvalue()

            magic_byte = b'\x00'
            schema_id_bytes = struct.pack('>I', self.output_schema_id)  # '>I' for big-endian unsigned int
            value_with_header = magic_byte + schema_id_bytes + value_bytes

            self.success_producer.produce(
                topic=app_config.output_topic,
                key=key_encoded,
                value=value_with_header
            )
            self.logger.info("payload successfully deployed", worker_id=self.partition_id, key=key_encoded)
        except Exception as err:
            self.logger.warning("trying to output alleged success, but encoding failed", err=err, key=key)
            self.deploy_fail_payload(key, value)
        finally:
            self.success_producer.flush()
        return

    def deploy_fail_payload(self, key, value) -> None:
        key_encoded = str(key).encode('utf-8')
        #value_encoded = BytesIO()
        #fastavro.schemaless_writer(value_encoded, self.input_schema_json, record=value)
        self.fail_producer.produce(
            topic=f"{app_config.output_topic}_error",
            key=key_encoded,
            value=str(value)
        )
        self.logger.warning("payload unsuccessfully deployed", worker_id=self.partition_id, key=key_encoded)
        self.fail_producer.flush()
        return

    def redeploy_payload(self, key, value) -> None:
        key_encoded = str(key).encode('utf-8')
        self.success_producer.produce(
            topic=app_config.input_topic,
            key=key_encoded,
            value=json.dumps(value)
        )
        self.success_producer.flush()
        return

    def close(self):
        self.logger.info("closing Kafka consumer and producers")
        self.success_producer.flush()  # Ensure all messages are delivered
        self.fail_producer.flush()
        self.avro_consumer.close()  # Close consumer gracefully
