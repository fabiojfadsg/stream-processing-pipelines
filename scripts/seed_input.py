"""Converte o array JSON do Lab 02 em arquivos que simulam microbatches."""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data/reference/Lab 02 - Dataset.json"
TARGET = ROOT / "data/input"
BATCH_SIZE = 10


def main() -> None:
    TARGET.mkdir(parents=True, exist_ok=True)
    for file in TARGET.glob("*.json"):
        file.unlink()

    products = json.loads(SOURCE.read_text(encoding="utf-8"))
    # O dataset não tem data de evento. Esta marca temporal sintética só é usada
    # para demonstrar Window e Watermark; o arquivo de referência permanece intacto.
    base_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    for number, start in enumerate(range(0, len(products), BATCH_SIZE), start=1):
        batch = products[start:start + BATCH_SIZE]
        target = TARGET / f"produtos_batch_{number:02d}.json"
        events = []
        for offset, product in enumerate(batch):
            event = product.copy()
            event_time = base_time + timedelta(seconds=(start + offset) * 5)
            event["event_time"] = event_time.isoformat().replace("+00:00", "Z")
            events.append(event)
        target.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in events) + "\n", encoding="utf-8")
        print(f"Criado: {target.relative_to(ROOT)} ({len(batch)} registros)")


if __name__ == "__main__":
    main()
