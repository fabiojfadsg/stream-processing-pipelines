-- Job contínuo de quarentena. Deixe esta instrução em execução.
INSERT INTO produtos_invalidos
SELECT id, nome, preco, quantidade, event_time, motivo
FROM produtos_rejeitados;
