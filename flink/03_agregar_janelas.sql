-- Job contínuo principal: janela fixa de 1 minuto em Event Time.
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
