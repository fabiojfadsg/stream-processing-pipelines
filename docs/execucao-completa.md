# Execução completa — Confluent Cloud, Kafka e Apache Flink

Este arquivo concentra **todos os comandos na ordem de execução**. O professor pode abrir somente este documento e copiar cada bloco SQL para uma célula separada do **Flink SQL Workspace** no Confluent Cloud.

## Antes de começar

No Confluent Cloud, confirme que existem:

1. Um Environment;
2. Um cluster Kafka com Schema Registry;
3. Um Flink Compute Pool na mesma região do cluster;
4. Um SQL Workspace usando o catálogo e o database do projeto.

Use o modo **Streaming**. Execute uma célula por vez e aguarde a indicação de sucesso antes de continuar. As células 7 e 8 são jobs contínuos e devem permanecer em execução durante a carga.

## 1. Criar as tabelas e os tópicos

### Célula 1 — Origem em JSON Schema

```sql
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
```

### Célula 2 — Saída agregada em Avro

```sql
CREATE TABLE IF NOT EXISTS produtos_agregados (
    janela_inicio TIMESTAMP_LTZ(3),
    janela_fim TIMESTAMP_LTZ(3),
    total_produtos BIGINT,
    total_itens BIGINT,
    valor_total_estoque DECIMAL(18, 2)
) WITH (
    'value.format' = 'avro-registry'
);
```

### Célula 3 — Quarentena em Avro

```sql
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
```

## 2. Criar as views de transformação e validação

### Célula 4 — Tipagem segura do preço

```sql
CREATE VIEW produtos_tipados AS
SELECT
    id,
    nome,
    preco,
    TRY_CAST(preco AS DECIMAL(12, 2)) AS preco_decimal,
    quantidade,
    event_time,
    event_ts
FROM produtos_raw;
```

### Célula 5 — Produtos válidos

```sql
CREATE VIEW produtos_validos AS
SELECT
    id,
    nome,
    preco_decimal,
    quantidade,
    event_ts,
    preco_decimal * quantidade AS valor_total
FROM produtos_tipados
WHERE id IS NOT NULL
  AND nome IS NOT NULL
  AND preco_decimal IS NOT NULL
  AND preco_decimal > 0
  AND quantidade IS NOT NULL
  AND quantidade > 0;
```

### Célula 6 — Produtos rejeitados e motivo

```sql
CREATE VIEW produtos_rejeitados AS
SELECT
    id,
    nome,
    preco,
    quantidade,
    event_time,
    CASE
        WHEN id IS NULL THEN 'id ausente'
        WHEN nome IS NULL THEN 'nome ausente'
        WHEN preco_decimal IS NULL THEN 'preco não numérico'
        WHEN preco_decimal <= 0 THEN 'preco deve ser positivo'
        WHEN quantidade IS NULL THEN 'quantidade ausente'
        WHEN quantidade <= 0 THEN 'quantidade deve ser positiva'
        ELSE 'registro inválido'
    END AS motivo
FROM produtos_tipados
WHERE id IS NULL
   OR nome IS NULL
   OR preco_decimal IS NULL
   OR preco_decimal <= 0
   OR quantidade IS NULL
   OR quantidade <= 0;
```

## 3. Iniciar os dois jobs contínuos

Execute as duas células abaixo e **deixe ambas em estado Running**.

### Célula 7 — Job de quarentena

```sql
INSERT INTO produtos_invalidos
SELECT id, nome, preco, quantidade, event_time, motivo
FROM produtos_rejeitados;
```

### Célula 8 — Job de agregação por janela de um minuto

```sql
INSERT INTO produtos_agregados
SELECT
    window_start AS janela_inicio,
    window_end AS janela_fim,
    COUNT(id) AS total_produtos,
    SUM(CAST(quantidade AS BIGINT)) AS total_itens,
    CAST(SUM(valor_total) AS DECIMAL(18, 2)) AS valor_total_estoque
FROM TABLE(
    TUMBLE(
        TABLE produtos_validos,
        DESCRIPTOR(event_ts),
        INTERVAL '1' MINUTE
    )
)
GROUP BY window_start, window_end;
```

## 4. Carregar o dataset do Lab 02

Depois que as células 7 e 8 estiverem em execução, rode a carga abaixo em uma nova célula. Os 50 primeiros registros correspondem ao dataset do Lab 02. O último registro é um controle técnico para avançar o Watermark e fechar a janela final.

### Célula 9 — Carga completa

```sql
INSERT INTO produtos_raw VALUES
    ('1', 'Rustic Soft Table', '108.00', 56, 1767225600000),
    ('2', 'Elegant Steel Car', '452.00', 96, 1767225605000),
    ('3', 'Practical Plastic Salad', '382.00', 47, 1767225610000),
    ('4', 'Incredible Plastic Mouse', '201.00', 7, 1767225615000),
    ('5', 'Sleek Concrete Chips', '441.00', 48, 1767225620000),
    ('6', 'Tasty Frozen Salad', '994.00', 35, 1767225625000),
    ('7', 'Refined Plastic Shoes', '895.00', 96, 1767225630000),
    ('8', 'Fantastic Wooden Cheese', '551.00', 98, 1767225635000),
    ('9', 'Fantastic Frozen Car', '152.00', 14, 1767225640000),
    ('10', 'Tasty Plastic Chips', '849.00', 72, 1767225645000),
    ('11', 'Handmade Soft Hat', '55.00', 33, 1767225650000),
    ('12', 'Fantastic Cotton Hat', '36.00', 22, 1767225655000),
    ('13', 'Recycled Wooden Chair', '784.00', 93, 1767225660000),
    ('14', 'Handmade Rubber Sausages', '935.00', 81, 1767225665000),
    ('15', 'Bespoke Granite Ball', '544.00', 68, 1767225670000),
    ('16', 'Rustic Rubber Sausages', '723.00', 5, 1767225675000),
    ('17', 'Tasty Plastic Keyboard', '12.00', 14, 1767225680000),
    ('18', 'Electronic Cotton Chicken', '330.00', 27, 1767225685000),
    ('19', 'Small Rubber Table', '757.00', 79, 1767225690000),
    ('20', 'Luxurious Plastic Pizza', '595.00', 91, 1767225695000),
    ('21', 'Sleek Metal Table', '101.00', 45, 1767225700000),
    ('22', 'Luxurious Rubber Hat', '751.00', 2, 1767225705000),
    ('23', 'Oriental Frozen Chair', '94.00', 70, 1767225710000),
    ('24', 'Tasty Soft Chips', '192.00', 30, 1767225715000),
    ('25', 'Ergonomic Cotton Gloves', '471.00', 73, 1767225720000),
    ('26', 'Electronic Fresh Chicken', '705.00', 30, 1767225725000),
    ('27', 'Refined Soft Fish', '36.00', 4, 1767225730000),
    ('28', 'Electronic Frozen Bacon', '749.00', 23, 1767225735000),
    ('29', 'Small Wooden Chair', '10.00', 51, 1767225740000),
    ('30', 'Gorgeous Wooden Car', '779.00', 78, 1767225745000),
    ('31', 'Handmade Steel Chicken', '200.00', 42, 1767225750000),
    ('32', 'Bespoke Steel Soap', '505.00', 100, 1767225755000),
    ('33', 'Handmade Granite Chicken', '984.00', 40, 1767225760000),
    ('34', 'Gorgeous Wooden Car', '529.00', 31, 1767225765000),
    ('35', 'Intelligent Soft Table', '197.00', 12, 1767225770000),
    ('36', 'Licensed Rubber Salad', '886.00', 28, 1767225775000),
    ('37', 'Incredible Granite Chicken', '864.00', 6, 1767225780000),
    ('38', 'Tasty Fresh Car', '477.00', 55, 1767225785000),
    ('39', 'Incredible Metal Computer', '468.00', 20, 1767225790000),
    ('40', 'Electronic Wooden Car', '467.00', 22, 1767225795000),
    ('41', 'Electronic Soft Pizza', '142.00', 0, 1767225800000),
    ('42', 'Luxurious Metal Chips', '343.00', 74, 1767225805000),
    ('43', 'Unbranded Steel Chips', '26.00', 18, 1767225810000),
    ('44', 'Incredible Frozen Car', '745.00', 3, 1767225815000),
    ('45', 'Intelligent Wooden Car', '222.00', 26, 1767225820000),
    ('46', 'Handcrafted Wooden Pizza', '430.00', 88, 1767225825000),
    ('47', 'Practical Frozen Hat', '174.00', 46, 1767225830000),
    ('48', 'Tasty Rubber Table', '471.00', 19, 1767225835000),
    ('49', 'Bespoke Frozen Chicken', '526.00', 22, 1767225840000),
    ('50', 'Oriental Frozen Towels', '516.00', 96, 1767225845000),
    ('_watermark', 'Controle de watermark', '-1.00', 1, 1767225960000);
```

## 5. Consultar os resultados

Execute cada consulta em uma célula separada. Como são consultas em modo Streaming, use o botão **Stop** após registrar a evidência desejada.

### Célula 10 — Eventos recebidos na origem

```sql
SELECT * FROM produtos_raw;
```

Resultado esperado: 51 linhas, considerando 50 produtos e o registro técnico de Watermark.

### Célula 11 — Agregações por janela

```sql
SELECT * FROM produtos_agregados;
```

Resultado esperado: cinco janelas fechadas com contagem de produtos, quantidade total de itens e valor total do estoque.

### Célula 12 — Registros rejeitados

```sql
SELECT * FROM produtos_invalidos;
```

Resultado esperado: o produto `41`, cuja quantidade é zero, e o registro técnico `_watermark`, cujo preço é negativo.

### Célula 13 — Diagnóstico do Watermark

```sql
SELECT
    event_ts,
    CURRENT_WATERMARK(event_ts) AS watermark_atual
FROM produtos_raw;
```

## 6. Evidências que devem ser verificadas

No Confluent Cloud, confira:

1. Os dois statements `INSERT` em estado **Running**;
2. Os tópicos `produtos_raw`, `produtos_agregados` e `produtos_invalidos`;
3. O subject `produtos_raw-value` como **JSON Schema**;
4. Os subjects `produtos_agregados-value` e `produtos_invalidos-value` como **Avro**;
5. As cinco janelas na tabela `produtos_agregados`;
6. Os dois registros na tabela `produtos_invalidos`;
7. As métricas do Flink Compute Pool e a linhagem do cluster.

## 7. Cleanup opcional

Esta etapa não é necessária para demonstrar o trabalho. Use-a somente quando quiser encerrar o laboratório.

Primeiro, clique em **Stop** nas células 7 e 8. Depois execute cada comando abaixo em uma célula separada, na ordem apresentada.

### Célula 14

```sql
DROP VIEW IF EXISTS produtos_rejeitados;
```

### Célula 15

```sql
DROP VIEW IF EXISTS produtos_validos;
```

### Célula 16

```sql
DROP VIEW IF EXISTS produtos_tipados;
```

### Célula 17

```sql
DROP TABLE IF EXISTS produtos_invalidos;
```

### Célula 18

```sql
DROP TABLE IF EXISTS produtos_agregados;
```

### Célula 19

```sql
DROP TABLE IF EXISTS produtos_raw;
```

> Atenção: o cleanup remove as tabelas e os recursos associados do catálogo. Preserve as evidências antes de executá-lo e suspenda ou exclua os recursos pagos quando não forem mais necessários.
