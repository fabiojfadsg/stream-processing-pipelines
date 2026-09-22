# MBA — Pipeline de Processamento de Dados em Streaming

Projeto-base para o trabalho de **Stream Processing & Pipelines**. A implementação principal usa **Confluent Cloud, Apache Kafka e Flink SQL** para ingerir produtos em JSON Schema, validar os campos, calcular métricas em janelas de tempo e publicar os resultados em Avro.

> Este repositório entrega a estrutura, os exemplos de configuração e o roteiro de execução. Os diretórios de saída, checkpoints e dados simulados são gerados localmente e não fazem parte da entrega.

## Objetivo

Construir um pipeline em que a fonte Kafka usa JSON Schema e o destino Kafka usa Avro, incluindo validação, quarentena, agregação e janela temporal no Flink SQL. A implementação Spark com saída Parquet permanece no repositório como alternativa local.

## Arquitetura principal — Confluent Cloud

```text
Dataset JSON → produtor Python → Kafka: produtos_raw (JSON Schema)
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
│   ├── reference/       # dataset original do Lab 02
│   ├── input/           # arquivos JSON que simulam a chegada do stream
│   ├── output/parquet/  # sink Parquet (ignorado pelo Git)
│   └── checkpoints/     # estado da query (ignorado pelo Git)
├── docs/
│   └── laboratorio.md   # documentação guiada da entrega
├── flink/               # scripts Flink SQL para o Confluent Cloud
├── infrastructure/      # base Docker da alternativa Spark
├── scripts/             # produtores e geradores de dados
├── src/                 # alternativa local em Spark
└── tests/               # testes futuros
```

## Executar no Confluent Cloud

1. Crie um ambiente, cluster Kafka, Schema Registry e Compute Pool do Flink.
2. No SQL Workspace, execute os arquivos de `flink/` na ordem numérica até `03`.
3. Copie `.env.confluent.example` para `.env` e informe as credenciais.
4. Instale o cliente: `pip install -r requirements-confluent.txt`.
5. Publique o dataset: `python scripts/produce_to_confluent.py`.
6. Execute as consultas de `flink/04_consultas.sql`.

O roteiro completo, incluindo as telas do Confluent Cloud e a ordem exata das células SQL, está em [docs/confluent-cloud-flink.md](docs/confluent-cloud-flink.md). A versão Spark anterior continua disponível como alternativa local em [docs/laboratorio.md](docs/laboratorio.md).

## Dataset

O arquivo original fornecido pelo professor está preservado em [data/reference/Lab 02 - Dataset.json](data/reference/Lab%2002%20-%20Dataset.json). Ele contém 50 produtos com `id`, `nome`, `preco` e `quantidade`.

## Critérios atendidos

- Ingestão contínua: eventos publicados em um tópico Kafka.
- Conversão de formato: JSON Schema para Avro.
- Bônus 1: Schema Registry, `TRY_CAST`, filtro e tópico de dados inválidos.
- Bônus 2 e 3: agregação por janela de 1 minuto.
- Bônus 4: pipeline executado como jobs gerenciados do Confluent Cloud for Apache Flink.
