# Trabalho — Confluent Cloud e Apache Flink

**Disciplina:** Stream Processing & Pipelines  
**Plataforma:** Confluent Cloud for Apache Flink  
**Origem:** Kafka com JSON Schema | **Destino:** Kafka com Avro

---

## 🎯 Objetivo

Construir um pipeline de streaming gerenciado que publique o dataset de produtos do Lab 02 em Kafka, valide os eventos com Flink SQL, calcule métricas por janela temporal e escreva o resultado em outro tópico e em outro formato.

O pipeline atende aos bônus do enunciado:

1. validação e filtro com Schema Registry e `TRY_CAST`;
2. agregação de quantidade e valor de estoque;
3. janela fixa (`TUMBLE`) de um minuto com Watermark;
4. execução dos jobs no Confluent Cloud for Apache Flink.

---

## 🏗️ Arquitetura

```text
Lab 02 - Dataset.json
         ↓ produtor Python
produtos_raw (Kafka + JSON Schema)
         ↓ Flink SQL
   validação / tipagem
      ↙            ↘
produtos_invalidos  TUMBLE de 1 minuto
(Kafka + Avro)             ↓
                    produtos_agregados
                       (Kafka + Avro)
```

O dataset original não possui data de evento. O produtor acrescenta `event_time` em epoch milliseconds, mantendo o arquivo original intacto. O Flink converte esse campo em `TIMESTAMP_LTZ(3)` e aplica Watermark de cinco segundos.

---

## 📋 Pré-requisitos

- conta no Confluent Cloud;
- um Environment com Schema Registry habilitado;
- um cluster Kafka em região compatível com Flink;
- um Flink Compute Pool na mesma região do cluster;
- Python 3.10 ou superior na máquina que publicará o dataset.

Você precisará de dois pares de credenciais diferentes:

- API key/secret do cluster Kafka;
- API key/secret do Schema Registry.

---

## 🚀 Passo a passo

### Passo 1: Criar os recursos no Confluent Cloud

1. Acesse o Confluent Cloud e crie ou selecione um Environment.
2. Crie um cluster Kafka e habilite o Schema Registry.
3. No menu **Flink**, crie um Compute Pool na mesma região.
4. Abra um **SQL Workspace** ligado ao Environment e ao cluster corretos.

### Passo 2: Criar as tabelas e tópicos

Abra `flink/00_criar_tabelas.sql`. Copie cada `CREATE TABLE` para uma célula separada e execute na ordem em que aparece.

O primeiro comando cria `produtos_raw` e seu schema JSON. Os outros dois criam tópicos Avro para agregações e registros rejeitados. Aguarde todos os comandos terminarem antes de iniciar o produtor.

### Passo 3: Criar as views de transformação

Execute, uma instrução por célula, o conteúdo de `flink/01_criar_views.sql`.

As views não copiam dados. Elas representam a tipagem e as regras de qualidade que serão aplicadas continuamente aos eventos.

### Passo 4: Iniciar os jobs contínuos

Execute `flink/02_processar_invalidos.sql` em uma célula e deixe o job em execução. Em outra célula, execute `flink/03_agregar_janelas.sql`.

O primeiro job envia erros para a quarentena. O segundo fecha as janelas conforme o Watermark avança e escreve as métricas em `produtos_agregados`.

### Passo 5: Configurar o produtor local

Na raiz do projeto:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.confluent.example .env
```

Preencha `.env` com os endpoints e as credenciais exibidos no Confluent Cloud. Não faça commit desse arquivo.

### Passo 6: Publicar o dataset

```bash
python scripts/produce_to_confluent.py
```

Para demonstrar também a quarentena:

```bash
python scripts/produce_to_confluent.py --include-invalid
```

O produtor lê o schema criado pelo Flink no Schema Registry, serializa os 50 produtos como JSON Schema e aguarda a confirmação de entrega do Kafka.

### Passo 7: Consultar e registrar evidências

Execute cada consulta de `flink/04_consultas.sql` em uma célula diferente. Registre capturas de tela de:

1. mensagens em `produtos_raw`;
2. resultados por janela em `produtos_agregados`;
3. mensagem e motivo em `produtos_invalidos`, caso tenha usado `--include-invalid`;
4. jobs `RUNNING` no painel de statements do Flink;
5. schemas JSON e Avro no Schema Registry.

---

## 🧠 Decisões técnicas

- **JSON Schema → Avro:** demonstra a mudança de formato exigida no enunciado usando formatos nativos do Confluent Cloud.
- **Event Time:** os timestamps sintéticos tornam o exemplo reproduzível, pois o dataset original não contém tempo.
- **Watermark:** tolera cinco segundos de atraso e determina quando a janela pode ser emitida.
- **Dead-letter topic:** um erro de qualidade não interrompe o job principal.
- **Schema Registry:** formaliza o contrato dos eventos e permite que o Flink descubra os tipos.

---

## 🧹 Cleanup

1. Interrompa os statements `INSERT INTO` no painel do Flink.
2. Execute `flink/99_cleanup.sql`, uma instrução por célula.
3. Se o ambiente não será mais usado, suspenda ou exclua o Compute Pool e o cluster para evitar consumo de créditos.

> Atenção: o cleanup remove as tabelas e pode remover os recursos associados. Preserve as evidências antes de executá-lo.

---

## ✅ Resultado esperado

Ao final, o tópico `produtos_agregados` deverá conter uma linha Avro por janela fechada, com início, fim, contagem de produtos, total de itens e valor do estoque. Se a opção de teste inválido for usada, `produtos_invalidos` deverá receber uma linha com o motivo `preco deve ser positivo`.
