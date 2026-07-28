import sys
import json
import requests

BASE_URL = "https://analitica.auvp.com.br/api/assets-list/query"

ASSET_TYPE = "stock"  # default, pode ser sobrescrito via argumento
COUNTRY = "BRA"
SORT_KEY = "ticker"
ORDER = "asc"
NATIONAL = "true"

COUNTRY_USA = "USA"
NATIONAL_USA = "false"
# KPIS = [
#     "dividend_yield", "p_l", "p_vp", "lpa", "vpa", "ev_ebitda", "ev_ebit",
#     "p_ebitda", "p_ebit", "p_receita", "p_fco", "p_fcl", "ev_rl", "ev_fco",
#     "ev_fcl", "earning_yield", "ev", "valor_mercado", "liquidez_media_diaria",
#     "cagr_receita_5_anos", "cagr_ebitda_5anos", "cagr_ebit_5anos",
#     "cagr_lucro_operacional_5_anos", "cagr_lucro_liquido_5_anos",
#     "roe", "roic", "roa", "payout", "giro_ativos", "retorno_12_meses",
#     "margem_bruta", "margem_ebitda", "margem_ebit", "margem_liquida",
#     "divida_liquida_ebitda", "divida_liquida_pl", "divida_liquida",
#     "liquidez_corrente", "pl_ativos", "passivos_ativos",
#     "liquidez_seca", "liquidez_imediata",
# ]
KPIS = ["dividend_yield", "p_l", "p_vp", "ev_ebitda", "ev_ebit"]

# KPIS_FII = [
#     "dividend_yield", "p_vp", "vpa", "patrimonio_liquido", "ffo_yield",
#     "imoveis_pl", "valor_mercado", "vacancia_fisica", "vacancia_financeira",
#     "ffo", "alavancagem_financeira_pl", "tx_adm",
# ]

KPIS_FII = [
    "dividend_yield", "p_vp", "ffo_yield",
]

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "priority": "u=1, i",
    "referer": "https://analitica.auvp.com.br/acoes?limit=50&page=1",
    "sec-ch-ua": '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
}

COOKIES = {
    "_gcl_au": "1.1.768004925.1785169845",
    "AMP_MKTG_12d187df1a": "JTdCJTIycmVmZXJyZXIlMjIlM0ElMjJodHRwcyUzQSUyRiUyRnd3dy5nb29nbGUuY29tJTJGJTIyJTJDJTIycmVmZXJyaW5nX2RvbWFpbiUyMiUzQSUyMnd3dy5nb29nbGUuY29tJTIyJTdE",
    "_ga": "GA1.1.541232464.1785169846",
    "AdoptConsent": "N4Ig7gpgRgzglgFwgSQCIgFwgGYDYoAcAzEVAMYC02AnAIYAsF9BE1FBATEdhQAy69eAdnpDaUACa8CIADQgAbnHgIA9gCdkEzCAa8AjBGFEKRAKxQ2osrQrVcF9mf0cCZCbTf25IVQAcEZAA7ABVaAHMYTABtAF15fwQAeQBXBDDImPiQMlUgmAggwO0sODAACwBpAGUAGR8IBUL0gE8/CB0wAig4AAkALwgAOR9c/OaANQh1eDzMXnkUvw8kCQBBBB0OXg5cPiEKDiEQ/VwMIg4MaQA6Xf0ALRAAXyA===",
    "AdoptVisitorId": "IYFgDAjApmDsDMBaeBWARgTkSWBjYiGAbOogBwoQBMZuAJsLcUA=",
    "kc-state": "",
    "kc-id-token": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJSTF9DNHJQWTJYd3YwWlBSVU9zT2VGNGlmdXpkZFZUTF9UN1R3aXNXR2swIn0.eyJleHAiOjE3ODUxNzAyNzYsImlhdCI6MTc4NTE2OTk3NiwiYXV0aF90aW1lIjoxNzg1MTY5OTc2LCJqdGkiOiIwMDkxMDYxNy1mZGY0LTAwNzgtY2E5MS1kYmU5ZTU3YWJkM2EiLCJpc3MiOiJodHRwczovL3Nzby5hdXZwLmNvbS5ici9yZWFsbXMvQVVWUCIsImF1ZCI6ImFuYWxpdGljYSIsInN1YiI6ImQzMzE1YzBlLTZkZTItNDM4MS1iN2M5LWU0Nzc0MmJkZTkyYiIsInR5cCI6IklEIiwiYXpwIjoiYW5hbGl0aWNhIiwic2lkIjoiUjFEVHVsbkxvejI2RzRqVy1wNlM1ZnpoIiwiYXRfaGFzaCI6Ik1zZVBiX0JwcnZRTTUzU0JxLVZiSGciLCJhY3IiOiIxIiwidGF4X2lkZW50aWZpY2F0aW9uIjoiMDAzMzY2ODAwMDciLCJiaXJ0aGRhdGUiOiIxOTg0LTEyLTEwIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsInBob25lIjoiKzU1NTE5OTgyMTI3NjYiLCJuYW1lIjoiQ2FybG9zIEVkdWFyZG8gRHVhcnRlIFNjaHdhbG0iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJrcmxzZWR1QGdtYWlsLmNvbSIsImdpdmVuX25hbWUiOiJDYXJsb3MiLCJmYW1pbHlfbmFtZSI6IkVkdWFyZG8gRHVhcnRlIFNjaHdhbG0iLCJlbWFpbCI6ImtybHNlZHVAZ21haWwuY29tIn0.Yza4bXigxMXs9DIpC_RrdAieDfk9mghOxSuowCcSLmUpAi7e1XKKJN29pUW06cGr7e-eX1MP9nzd_xxgFfGb1XVLGaPB6t1kRVuR4k7u9fZTdsueS6csStrjlVQhhne3fKvipNq54pwO4FjINyfuzX8fBAttkSVEZNQP_ymahCPzfTLfUqzZXTbSSeX0SR4IHcZ3zREetD0pVA5qNEWixaTh5nOEN3oPvq81D3AFa0BgaFTuRuTtm6iHGks9Kdq2bAk9QqT0yDNIYKBkvjRJ8euQ9jh1txxcJhaWbRffWBtyS9l7zZjBn4EMVURC56K9orw5qLjACWrfpPrry5s5gg",
    "analitica-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY0NWYyMTM1LWQwNTAtNGZmOC04NTE0LTc0ZTBlNDk2NzgwOSIsImVtYWlsIjoia3Jsc2VkdUBnbWFpbC5jb20iLCJuYW1lIjoiQ2FybG9zIEVkdWFyZG8gRHVhcnRlIFNjaHdhbG0iLCJleHAiOjE3ODc3NjE5NzYsImlhdCI6MTc4NTE2OTk3NiwibmJmIjoxNzg1MTY5OTc2fQ.lKr4Uah59kjfVEiKizm_BLjSKS-gEuTxX5_JtFmVn5s",
    "preference-page": "",
    "last-view": "%7B%22asset%22%3A%22ISAE4%22%2C%22time%22%3A1785176008199%7D",
    "guest-id": "a7a38da3-b363-4aac-9519-4e16a0526326",
    "_clck": "1zquxz%5E2%5Eg84%5E0%5E2399",
    "_ga_56H8LKLCLE": "GS2.1.s1785253036$o4$g1$t1785253038$j58$l0$h1649853739",
    "_ga_L65B4EVWKZ": "GS2.1.s1785253036$o4$g1$t1785253038$j58$l0$h0",
    "_clsk": "1b95d7x%5E1785253964553%5E4%5E1%5El.clarity.ms%2Fcollect",
    "_dd_s": "aid=c22655a2-80b3-4bd8-81fb-241be1886d44&rum=1&id=55d0a5d0-e8e2-40a4-b085-c66bc403013a&created=1785253022821&expire=1785254879843&logs=1",
    "AMP_12d187df1a": "JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJjMGY5YjI0Mi0yZjZlLTQyMzAtOWU3MC04MTNhZGU3NzEzYTYlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzg1MjUzMDIyODcxJTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc4NTI1MzAzOTQxNCUyQyUyMmxhc3RFdmVudElkJTIyJTNBMTY3JTJDJTIycGFnZUNvdW50ZXIlMjIlM0ExJTdE",
}


def build_params(page: int, limit: int, total: int, asset_type: str = ASSET_TYPE, country: str = COUNTRY, national: str = NATIONAL) -> dict:
    params = {
        "assetType": asset_type,
        "country": country,
        "page": page,
        "limit": limit,
        "total": total,
        "sortKey": SORT_KEY,
        "order": ORDER,
        "national": national,
    }
    return params


def fetch_page(page: int, limit: int, total: int, asset_type: str = ASSET_TYPE, country: str = COUNTRY, national: str = NATIONAL) -> dict:
    params = build_params(page, limit, total, asset_type, country, national)
    kpis = KPIS_FII if asset_type == "fii" else KPIS
    kpi_params = "&".join(f"kpis={k}" for k in kpis)
    base_params = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{BASE_URL}?{base_params}&{kpi_params}"

    response = requests.get(url, headers=HEADERS, cookies=COOKIES)
    response.raise_for_status()
    data = response.json()
    return convert_na_to_null(data)


def convert_na_to_null(obj):
    if isinstance(obj, dict):
        return {k: convert_na_to_null(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_na_to_null(i) for i in obj]
    elif isinstance(obj, str) and obj.strip().lower() == "n/a":
        return None
    return obj


def fetch_all(limit: int, asset_type: str = ASSET_TYPE, country: str = COUNTRY, national: str = NATIONAL, force: bool = False):
    import os
    import glob

    suffix = f"_{country}" if country != COUNTRY else ""
    output_file = f"{asset_type}{suffix}.json"
    page_file_pattern = f"{asset_type}{suffix}_page_*.json"

    # Descobre páginas já baixadas
    existing_pages = sorted(
        int(f.split("_page_")[-1].replace(".json", ""))
        for f in glob.glob(page_file_pattern)
    )

    if force:
        for f in glob.glob(page_file_pattern):
            os.remove(f)
        existing_pages = []
        print("Modo force: páginas anteriores removidas.")

    # Fetch first page to discover pagination info
    print(f"Buscando página 1 com limit={limit}, asset_type={asset_type}, country={country}...")
    first_data = fetch_page(page=1, limit=limit, total=0, asset_type=asset_type, country=country, national=national)

    pagination = first_data.get("pagination", {})
    total = pagination.get("total", first_data.get("total", 0))
    total_pages = pagination.get("totalPages", 1)
    print(f"Total de ativos: {total}, Total de páginas: {total_pages}")

    if total_pages <= 5:
        # Sem paginação por arquivo: comportamento original
        all_items = list(first_data.get("data", []))
        for page in range(2, total_pages + 1):
            print(f"Buscando página {page}/{total_pages}...")
            data = fetch_page(page=page, limit=limit, total=total, asset_type=asset_type, country=country, national=national)
            all_items.extend(data.get("data", []))
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_items, f, ensure_ascii=False, indent=2)
        print(f"Todos os {len(all_items)} itens salvos em {output_file}")
    else:
        # Salva página a página; retoma de onde parou
        start_page = 1
        if existing_pages:
            start_page = max(existing_pages) + 1
            print(f"Retomando a partir da página {start_page} (páginas já baixadas: {existing_pages})")

        for page in range(start_page, total_pages + 1):
            page_file = f"{asset_type}{suffix}_page_{page}.json"
            if page in existing_pages:
                print(f"Página {page} já baixada, pulando...")
                continue
            print(f"Buscando página {page}/{total_pages}...")
            if page == 1:
                data = first_data
            else:
                data = fetch_page(page=page, limit=limit, total=total, asset_type=asset_type, country=country, national=national)
            with open(page_file, "w", encoding="utf-8") as f:
                json.dump(data.get("data", []), f, ensure_ascii=False, indent=2)
            print(f"Página {page} salva em {page_file}")

        # Consolida todas as páginas
        all_items = []
        for page in range(1, total_pages + 1):
            page_file = f"{asset_type}{suffix}_page_{page}.json"
            with open(page_file, "r", encoding="utf-8") as f:
                all_items.extend(json.load(f))
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(all_items, f, ensure_ascii=False, indent=2)
        print(f"Todos os {len(all_items)} itens consolidados em {output_file}")

    print("Concluído!")


if __name__ == "__main__":
    args = [a for a in sys.argv[1:] if a != "--force"]
    force_arg = "--force" in sys.argv

    if len(args) >= 3:
        limit = int(args[0])
        asset_type = args[1]
        country_arg = args[2].upper()
    elif len(args) >= 2:
        limit = int(args[0])
        asset_type = args[1]
        country_arg = input("País (country) [BRA]: ").strip().upper() or "BRA"
    elif len(args) == 1:
        limit = int(args[0])
        asset_type = input("Tipo de ativo (asset_type) [stock]: ").strip() or "stock"
        country_arg = input("País (country) [BRA]: ").strip().upper() or "BRA"
    else:
        limit = int(input("Tamanho da página (limit): ").strip())
        asset_type = input("Tipo de ativo (asset_type) [stock]: ").strip() or "stock"
        country_arg = input("País (country) [BRA]: ").strip().upper() or "BRA"

    national_arg = "false" if country_arg != "BRA" else "true"
    fetch_all(limit, asset_type, country_arg, national_arg, force=force_arg)
