-- Primeiro interrompa manualmente os dois jobs INSERT no Confluent Cloud.
DROP VIEW IF EXISTS produtos_rejeitados;
DROP VIEW IF EXISTS produtos_validos;
DROP VIEW IF EXISTS produtos_tipados;

-- Os comandos abaixo removem também os recursos associados no catálogo.
-- Use apenas quando quiser encerrar definitivamente o laboratório.
DROP TABLE IF EXISTS produtos_invalidos;
DROP TABLE IF EXISTS produtos_agregados;
DROP TABLE IF EXISTS produtos_raw;
