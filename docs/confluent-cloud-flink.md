# Trabalho — Confluent Cloud e Apache Flink

**Disciplina:** Stream Processing & Pipelines  
**Plataforma:** Confluent Cloud for Apache Flink  
**Origem:** Kafka com JSON Schema | **Destino:** Kafka com Avro

---

## Objetivo

Construir um pipeline de streaming gerenciado que publique o dataset de produtos do Lab 02 em Kafka, valide os eventos com Flink SQL, calcule métricas por janela temporal e escreva o resultado em outro tópico e em outro formato.

O pipeline atende aos bônus do enunciado:

1. Validação e filtro com Schema Registry e `TRY_CAST`;
2. Agregação de quantidade e valor de estoque;
3. Janela fixa (`TUMBLE`) de um minuto com Watermark;
4. Execução dos jobs no Confluent Cloud for Apache Flink.

---

## Arquitetura

```text
Lab 02 - Dataset.json
      ↓ INSERT Flink SQL
produtos_raw (Kafka + JSON Schema)
      ↓ Flink SQL
   validação / tipagem
      ↙            ↘
produtos_invalidos  TUMBLE de 1 minuto
(Kafka + Avro)             ↓
                    produtos_agregados
                       (Kafka + Avro)
```

O dataset original não possui data de evento. O script de carga acrescenta `event_time` em epoch milliseconds, mantendo o arquivo original intacto. O Flink converte esse campo em `TIMESTAMP_LTZ(3)` e aplica Watermark de cinco segundos.

---

## Pré-requisitos

- Conta no Confluent Cloud;
- Um Environment com Schema Registry habilitado;
- Um cluster Kafka em região compatível com Flink;
- Um Flink Compute Pool na mesma região do cluster;
- Acesso ao SQL Workspace usado na apresentação.

---

## Passo a passo

### Passo 1: Criar os recursos no Confluent Cloud

1. Acesse o Confluent Cloud e crie ou selecione um Environment.
2. Crie um cluster Kafka e habilite o Schema Registry.
3. No menu **Flink**, crie um Compute Pool na mesma região.
4. Abra um **SQL Workspace** ligado ao Environment e ao cluster corretos.

### Passo 2: Criar as tabelas e tópicos

Abra `flink/00_criar_tabelas.sql`. Copie cada `CREATE TABLE` para uma célula separada e execute na ordem em que aparece.

O primeiro comando cria `produtos_raw` e seu schema JSON. Os outros dois criam tópicos Avro para agregações e registros rejeitados. Aguarde todos os comandos terminarem antes de iniciar os jobs e executar a carga SQL.

### Passo 3: Criar as views de transformação

Execute, uma instrução por célula, o conteúdo de `flink/01_criar_views.sql`.

As views não copiam dados. Elas representam a tipagem e as regras de qualidade que serão aplicadas continuamente aos eventos.

### Passo 4: Iniciar os jobs contínuos

Execute `flink/02_processar_invalidos.sql` em uma célula e deixe o job em execução. Em outra célula, execute `flink/03_agregar_janelas.sql`.

O primeiro job envia erros para a quarentena. O segundo fecha as janelas conforme o Watermark avança e escreve as métricas em `produtos_agregados`.

### Passo 5: Publicar o dataset pelo Flink SQL

Execute `flink/04_carregar_dataset.sql` em uma nova célula. O próprio Flink publicará os 50 registros do Lab 02 no tópico `produtos_raw` usando JSON Schema. A última linha é um registro técnico inválido que apenas avança o Watermark para fechar todas as janelas.

O registro `id=41` do dataset original possui quantidade zero e será rejeitado pelas regras de qualidade. Isso demonstra a validação usando um dado real do arquivo fornecido.

### Passo 6: Consultar e registrar evidências

Execute cada consulta de `flink/05_consultas.sql` em uma célula diferente. Registre capturas de tela de:

1. Mensagens em `produtos_raw`;
2. Resultados por janela em `produtos_agregados`;
3. Mensagens e motivos em `produtos_invalidos`;
4. Jobs `RUNNING` no painel de statements do Flink;
5. Schemas JSON e Avro no Schema Registry.

---

## Decisões técnicas

- **JSON Schema → Avro:** Demonstra a mudança de formato exigida no enunciado usando formatos nativos do Confluent Cloud.
- **Event Time:** Os timestamps sintéticos tornam o exemplo reproduzível, pois o dataset original não contém tempo.
- **Watermark:** Tolera cinco segundos de atraso e determina quando a janela pode ser emitida.
- **Dead-letter topic:** Um erro de qualidade não interrompe o job principal.
- **Schema Registry:** Formaliza o contrato dos eventos e permite que o Flink descubra os tipos.

---

## Cleanup

1. Interrompa os statements `INSERT INTO` no painel do Flink.
2. Execute `flink/99_cleanup.sql`, uma instrução por célula.
3. Se o ambiente não será mais usado, suspenda ou exclua o Compute Pool e o cluster para evitar consumo de créditos.

> Atenção: o cleanup remove as tabelas e pode remover os recursos associados. Preserve as evidências antes de executá-lo.

---

## Resultado esperado

Ao final, o tópico `produtos_agregados` deverá conter uma linha Avro por janela fechada, com início, fim, contagem de produtos, total de itens e valor do estoque. O tópico `produtos_invalidos` receberá o produto `41`, cuja quantidade é zero, e o registro técnico usado para avançar o Watermark.
