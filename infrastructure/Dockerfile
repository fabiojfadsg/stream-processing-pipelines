FROM apache/spark:3.5.3-python3

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY scripts ./scripts
COPY data/reference ./data/reference

CMD ["spark-submit", "src/streaming_pipeline.py"]
