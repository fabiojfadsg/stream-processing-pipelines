"""Pipeline Spark Structured Streaming: JSON de produtos para Parquet agregado."""
from pathlib import Path
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    round as spark_round,
    sum as spark_sum,
    count,
    window,
    to_timestamp,
)
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType


ROOT = Path(__file__).resolve().parents[1]


def configured_path(variable: str, default: str) -> str:
    """Resolve caminhos relativos à raiz do projeto."""
    value = Path(os.getenv(variable, default))
    return str(value if value.is_absolute() else ROOT / value)


PRODUCT_SCHEMA = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("nome", StringType(), nullable=False),
    StructField("preco", StringType(), nullable=False),
    StructField("quantidade", IntegerType(), nullable=False),
    # Campo acrescentado pelo adaptador de entrada; não altera o dataset original.
    StructField("event_time", StringType(), nullable=False),
])


def main() -> None:
    input_path = configured_path("INPUT_PATH", "data/input")
    output_path = configured_path("OUTPUT_PATH", "data/output/parquet")
    checkpoint_path = configured_path("CHECKPOINT_PATH", "data/checkpoints/produtos_aggregation")
    window_duration = os.getenv("WINDOW_DURATION", "1 minute")
    max_files = int(os.getenv("MAX_FILES_PER_TRIGGER", "1"))

    spark = (
        SparkSession.builder
        .appName(os.getenv("APP_NAME", "fiap-streaming-produtos"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Cada arquivo criado pelo script representa um microbatch do stream.
    source = (
        spark.readStream
        .schema(PRODUCT_SCHEMA)
        .option("maxFilesPerTrigger", max_files)
        .json(input_path)
    )

    # O adaptador cria event_time para que a janela use Event Time de forma reproduzível.
    validated = (
        source
        .withColumn("preco_decimal", col("preco").cast(DecimalType(12, 2)))
        .withColumn("event_time", to_timestamp(col("event_time")))
        .filter(
            col("id").isNotNull()
            & col("nome").isNotNull()
            & col("preco_decimal").isNotNull()
            & (col("preco_decimal") > 0)
            & col("quantidade").isNotNull()
            & (col("quantidade") > 0)
        )
        .withColumn("valor_total", col("preco_decimal") * col("quantidade"))
    )

    aggregated = (
        validated
        .withWatermark("event_time", "1 minute")
        .groupBy(window(col("event_time"), window_duration))
        .agg(
            count("id").alias("total_produtos"),
            spark_round(spark_sum("quantidade"), 2).alias("total_itens"),
            spark_round(spark_sum("valor_total"), 2).alias("valor_total_estoque"),
        )
        .select(
            col("window.start").alias("janela_inicio"),
            col("window.end").alias("janela_fim"),
            "total_produtos",
            "total_itens",
            "valor_total_estoque",
        )
    )

    query = (
        aggregated.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
