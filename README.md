# MBA — Pipeline de Streaming com Apache Flink e Kafka

Projeto para o trabalho de **Stream Processing & Pipelines**, executado integralmente no **Confluent Cloud com Apache Kafka e Apache Flink SQL**. O pipeline ingere produtos em JSON Schema, valida os campos, calcula métricas em janelas de tempo e publica os resultados em Avro.

> O dataset original do Lab 02 permanece inalterado. A carga equivalente está disponível em Flink SQL para que a demonstração não dependa de Python, terminal ou configuração local.

## Objetivo

Construir um pipeline em que a fonte Kafka usa JSON Schema e o destino Kafka usa Avro, incluindo validação, quarentena, agregação e janela temporal no Flink SQL.

## Arquitetura

```text
Dataset JSON → INSERT Flink SQL → Kafka: produtos_raw (JSON Schema)
                                      ↓
                                  Flink SQL
                                      ↓
                   validação + Watermark + janela de 1 minuto
                         ↙                            ↘
       produtos_invalidos (Avro)        produtos_agregados (Avro)
```

## Estrutura

```text
.
├── data/
│   └── reference/       # dataset original do Lab 02
├── docs/
│   └── confluent-cloud-flink.md
├── flink/               # scripts Flink SQL para o Confluent Cloud
└── README.md
```

## Executar no Confluent Cloud

1. Crie um ambiente, cluster Kafka, Schema Registry e Compute Pool do Flink.
2. Abra um SQL Workspace conectado ao cluster.
3. Execute os arquivos de `flink/` na ordem numérica, uma instrução por célula.
4. Mantenha os jobs `02` e `03` em execução, rode a carga `04` e visualize `05`.

O roteiro completo, incluindo as telas do Confluent Cloud e a ordem exata das células SQL, está em [docs/confluent-cloud-flink.md](docs/confluent-cloud-flink.md).

## Dataset

O arquivo original fornecido pelo professor está preservado em [data/reference/Lab 02 - Dataset.json](data/reference/Lab%2002%20-%20Dataset.json). Ele contém 50 produtos com `id`, `nome`, `preco` e `quantidade`.

## Critérios atendidos

- Ingestão contínua: eventos publicados em um tópico Kafka.
- Conversão de formato: JSON Schema para Avro.
- Bônus 1: Schema Registry, `TRY_CAST`, filtro e tópico de dados inválidos.
- Bônus 2 e 3: agregação por janela de 1 minuto.
- Bônus 4: pipeline implantado como jobs gerenciados do Confluent Cloud for Apache Flink.
