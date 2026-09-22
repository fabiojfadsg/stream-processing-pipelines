"""Publica o dataset do Lab 02 no tópico JSON Schema criado pelo Flink SQL."""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "data/reference/Lab 02 - Dataset.json"


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Variável obrigatória ausente: {name}")
    return value


def delivery_report(error, message) -> None:
    if error is not None:
        print(f"Falha na entrega: {error}", file=sys.stderr)
        return
    print(
        f"Entregue em {message.topic()} "
        f"[partição {message.partition()}] offset {message.offset()}"
    )


def build_producer(topic: str) -> SerializingProducer:
    schema_registry = SchemaRegistryClient({
        "url": required("SCHEMA_REGISTRY_URL"),
        "basic.auth.user.info": (
            f"{required('SCHEMA_REGISTRY_API_KEY')}:"
            f"{required('SCHEMA_REGISTRY_API_SECRET')}"
        ),
    })

    # A tabela Flink cria o subject <tópico>-value. Usar o ID existente impede
    # que o produtor tente registrar um schema diferente do catálogo Flink.
    registered = schema_registry.get_latest_version(f"{topic}-value")
    value_serializer = JSONSerializer(
        registered.schema,
        schema_registry,
        conf={
            "auto.register.schemas": False,
            "use.schema.id": registered.schema_id,
        },
    )

    return SerializingProducer({
        "bootstrap.servers": required("BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": required("KAFKA_API_KEY"),
        "sasl.password": required("KAFKA_API_SECRET"),
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": value_serializer,
        "acks": "all",
    })


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include-invalid",
        action="store_true",
        help="Acrescenta um produto inválido para demonstrar a quarentena.",
    )
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")
    topic = os.getenv("INPUT_TOPIC", "produtos_raw")
    producer = build_producer(topic)
    products = json.loads(DATASET.read_text(encoding="utf-8"))

    # Eventos crescentes no passado fazem o watermark avançar e fecham as
    # janelas durante uma demonstração curta, sem alterar o dataset original.
    base_time = (
        datetime.now(timezone.utc).replace(second=0, microsecond=0)
        - timedelta(minutes=5)
    )

    events = []
    for index, product in enumerate(products):
        event = product.copy()
        event["event_time"] = int(
            (base_time + timedelta(seconds=index * 5)).timestamp() * 1000
        )
        events.append(event)

    if args.include_invalid:
        events.append({
            "id": "invalid-01",
            "nome": "Produto com preço inválido",
            "preco": "-1.00",
            "quantidade": 1,
            # Além de demonstrar a quarentena, este evento posterior faz o
            # Watermark ultrapassar o fim da última janela dos 50 produtos.
            "event_time": int((base_time + timedelta(minutes=6)).timestamp() * 1000),
        })

    for event in events:
        producer.produce(
            topic=topic,
            key=event["id"],
            value=event,
            on_delivery=delivery_report,
        )
        producer.poll(0)

    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} mensagem(ns) não foram entregues.")
    print(f"Publicação concluída: {len(events)} eventos em {topic}.")


if __name__ == "__main__":
    main()
