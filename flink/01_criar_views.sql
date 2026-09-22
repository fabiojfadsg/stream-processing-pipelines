-- TRY_CAST devolve NULL em vez de derrubar o job ao receber preço não numérico.
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
