-- Execute cada CREATE TABLE em uma célula separada do SQL Workspace.
-- A tabela de origem cria o tópico produtos_raw com JSON Schema.
CREATE TABLE IF NOT EXISTS produtos_raw (
    id STRING,
    nome STRING,
    preco STRING,
    quantidade INT,
    event_time BIGINT,
    event_ts AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'value.format' = 'json-registry',
    'scan.startup.mode' = 'earliest-offset'
);

-- A saída agregada é serializada em Avro: formato diferente da origem.
CREATE TABLE IF NOT EXISTS produtos_agregados (
    janela_inicio TIMESTAMP_LTZ(3),
    janela_fim TIMESTAMP_LTZ(3),
    total_produtos BIGINT,
    total_itens BIGINT,
    valor_total_estoque DECIMAL(18, 2)
) WITH (
    'value.format' = 'avro-registry'
);

-- Dead-letter topic para registros que não passam na validação.
CREATE TABLE IF NOT EXISTS produtos_invalidos (
    id STRING,
    nome STRING,
    preco STRING,
    quantidade INT,
    event_time BIGINT,
    motivo STRING
) WITH (
    'value.format' = 'avro-registry'
);
