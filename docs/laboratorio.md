# Trabalho — Pipeline de Streaming de Produtos

**Disciplina:** Stream Processing & Pipelines  
**Tecnologia:** Apache Spark Structured Streaming / PySpark  
**Origem:** JSON | **Destino:** Parquet

---

## 🎯 Objetivo

Construir um pipeline de dados em streaming que processe produtos do dataset do Lab 02. A solução simula a chegada gradual de arquivos JSON, aplica regras de qualidade, agrega os dados por janela de tempo e grava o resultado no formato colunar Parquet.

Ao final, será possível demonstrar:

1. ingestão contínua com `readStream`;
2. schema explícito para o JSON;
3. validação de preço, quantidade e campos obrigatórios;
4. agregação de estoque em janela de 1 minuto;
5. persistência com `writeStream` em Parquet e checkpoint;
6. uma base de containerização para deploy.

---

## 📋 Pré-requisitos

- Python 3.10 ou superior;
- Java compatível com Apache Spark;
- `pip`;
- opcionalmente, Docker e Docker Compose.

Instale as dependências na raiz do projeto:

```bash
pip install -r requirements.txt
```

---

## 🗂️ Dados de origem

O arquivo `data/reference/Lab 02 - Dataset.json` é uma cópia sem alteração do dataset indicado no enunciado. Cada item contém os campos abaixo:

| Campo | Tipo de origem | Uso no pipeline |
| --- | --- | --- |
| `id` | texto | identificador do produto |
| `nome` | texto | descrição do produto |
| `preco` | texto numérico | convertido para decimal |
| `quantidade` | inteiro | quantidade em estoque |

O conjunto de origem não possui timestamp. Por isso, o adaptador de entrada acrescenta um `event_time` sintético, em intervalos de cinco segundos, sem alterar o arquivo original. Essa decisão deve ser mencionada na apresentação como uma limitação conhecida do dado de origem.

---

## 🚀 Passo a passo guiado

### Passo 1: Simular a chegada de dados

O Spark Structured Streaming lê diretórios; portanto, o arquivo JSON original (um array) é convertido em cinco arquivos JSON Lines, de dez registros cada. O adaptador também acrescenta timestamps sintéticos para demonstrar `Window` e `Watermark`. Execute:

```bash
python scripts/seed_input.py
```

Os arquivos serão criados em `data/input/`, e cada um equivale a um microbatch simulado.

### Passo 2: Executar a ingestão e a transformação

```bash
python src/streaming_pipeline.py
```

O arquivo `src/streaming_pipeline.py` define o schema explicitamente e configura `maxFilesPerTrigger=1`, fazendo o Spark processar um arquivo por microbatch.

### Passo 3: Entender as regras de qualidade

Antes da agregação, o pipeline descarta registros que possuam:

- `id` ou `nome` nulos;
- `preco` não conversível para decimal ou menor/igual a zero;
- `quantidade` nula ou menor/igual a zero.

Também é calculado `valor_total = preco × quantidade`.

### Passo 4: Agregação por janela

Os registros válidos são agrupados com `window(event_time, "1 minute")`. Para cada janela, a saída apresenta:

- total de produtos processados;
- soma das quantidades;
- valor total do estoque.

O gatilho `availableNow=True` processa o backlog disponível e finaliza a query de maneira controlada, sendo apropriado para a demonstração local.

### Passo 5: Verificar o resultado Parquet

Os dados estarão em `data/output/parquet/`. Em um ambiente com PySpark, a leitura de verificação é:

```python
spark.read.parquet("data/output/parquet").show(truncate=False)
```

O checkpoint fica em `data/checkpoints/` e impede o reprocessamento dos mesmos arquivos quando a query é retomada.

---

## 🐳 Base para deploy

A pasta `infrastructure/` contém `Dockerfile` e `docker-compose.yml`. Após gerar os arquivos de entrada, execute:

```bash
docker compose -f infrastructure/docker-compose.yml up --build
```

Na evolução do trabalho, essa imagem pode ser publicada em um registry e executada em um orquestrador como Kubernetes ou Databricks Jobs.

---

## ✅ Critérios de validação da entrega

1. O gerador cria cinco arquivos com dez registros cada.
2. A execução cria arquivos `.parquet` no diretório de saída.
3. Não existem registros com preço ou quantidade inválidos nos cálculos.
4. A saída contém `janela_inicio`, `janela_fim`, `total_produtos`, `total_itens` e `valor_total_estoque`.
5. A pasta de checkpoint é criada após o processamento.

---

## 💡 Próximos incrementos

- Trocar a simulação por Kafka, Kinesis ou Event Hubs.
- Enviar linhas inválidas para uma camada de quarentena (DLQ).
- Usar timestamp fornecido pela fonte quando ele estiver disponível.
- Publicar métricas de latência e throughput.
