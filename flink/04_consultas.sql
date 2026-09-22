-- Execute cada SELECT em uma célula separada para obter evidências do trabalho.
SELECT * FROM produtos_raw;

SELECT *
FROM produtos_agregados
ORDER BY janela_inicio;

SELECT * FROM produtos_invalidos;

-- Diagnóstico do avanço do Watermark.
SELECT
    event_ts,
    CURRENT_WATERMARK(event_ts) AS watermark_atual
FROM produtos_raw;
