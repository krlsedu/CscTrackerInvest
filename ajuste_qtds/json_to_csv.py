import sys
import json
import csv


def json_to_csv(json_file: str, csv_file: str = None):
    with open(json_file, "r", encoding="utf-8") as f:
        items = json.load(f)

    if not items:
        print("Nenhum item encontrado no JSON.")
        return

    # Coleta todos os kpi keys presentes nos dados
    kpi_keys = []
    for item in items:
        for kpi in item.get("quote", {}).get("kpis", []):
            if kpi["key"] not in kpi_keys:
                kpi_keys.append(kpi["key"])

    fieldnames = ["ticker", "nome", "tipo", "rating"] + kpi_keys

    if csv_file is None:
        csv_file = json_file.replace(".json", ".csv")

    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for item in items:
            quote = item.get("quote", {})
            row = {
                "ticker": item.get("code", ""),
                "nome": item.get("name", ""),
                "tipo": item.get("type", ""),
                "rating": quote.get("rating", ""),
            }
            for kpi in quote.get("kpis", []):
                val = kpi["value"]
                if isinstance(val, str) and val.strip().lower() == "n/a":
                    val = None
                row[kpi["key"]] = val
            writer.writerow(row)

    print(f"CSV gerado: {csv_file} ({len(items)} itens)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python json_to_csv.py <arquivo.json> [arquivo.csv]")
        print("Exemplo: python json_to_csv.py stock.json")
        sys.exit(1)

    json_file = sys.argv[1]
    csv_file = sys.argv[2] if len(sys.argv) >= 3 else None
    json_to_csv(json_file, csv_file)
