-- Execute cada SELECT em uma célula separada para obter evidências do trabalho.
SELECT * FROM produtos_raw;

-- Não usamos ORDER BY global: em streaming, a primeira ordenação precisa ser
-- um atributo temporal com Watermark, e a tabela de saída já está materializada.
SELECT * FROM produtos_agregados;

SELECT * FROM produtos_invalidos;

-- Diagnóstico do avanço do Watermark.
SELECT
    event_ts,
    CURRENT_WATERMARK(event_ts) AS watermark_atual
FROM produtos_raw;
