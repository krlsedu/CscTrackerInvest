import sys
import json
import requests


def fetch_indicators(ticker: str):
    url = (
        f"https://analitica.auvp.com.br/api/indicators"
        f"?indicator=ev_ebit&asset={ticker}&aggregate=ANUAL&period=5Y&company_type=stock"
    )

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "priority": "u=1, i",
        "referer": f"https://analitica.auvp.com.br/acoes/{ticker}",
        "sec-ch-ua": '"Not;A=Brand";v="8", "Chromium";v="150", "Google Chrome";v="150"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36",
    }

    cookies = {
        "_gcl_au": "1.1.768004925.1785169845",
        "_ga": "GA1.1.541232464.1785169846",
        "guest-id": "096b98fc-cc4f-413c-a987-97d41a4fd2ab",
        "kc-state": "",
        "kc-id-token": "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJSTF9DNHJQWTJYd3YwWlBSVU9zT2VGNGlmdXpkZFZUTF9UN1R3aXNXR2swIn0.eyJleHAiOjE3ODUxNzAyNzYsImlhdCI6MTc4NTE2OTk3NiwiYXV0aF90aW1lIjoxNzg1MTY5OTc2LCJqdGkiOiIwMDkxMDYxNy1mZGY0LTAwNzgtY2E5MS1kYmU5ZTU3YWJkM2EiLCJpc3MiOiJodHRwczovL3Nzby5hdXZwLmNvbS5ici9yZWFsbXMvQVVWUCIsImF1ZCI6ImFuYWxpdGljYSIsInN1YiI6ImQzMzE1YzBlLTZkZTItNDM4MS1iN2M5LWU0Nzc0MmJkZTkyYiIsInR5cCI6IklEIiwiYXpwIjoiYW5hbGl0aWNhIiwic2lkIjoiUjFEVHVsbkxvejI2RzRqVy1wNlM1ZnpoIiwiYXRfaGFzaCI6Ik1zZVBiX0JwcnZRTTUzU0JxLVZiSGciLCJhY3IiOiIxIiwidGF4X2lkZW50aWZpY2F0aW9uIjoiMDAzMzY2ODAwMDciLCJiaXJ0aGRhdGUiOiIxOTg0LTEyLTEwIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsInBob25lIjoiKzU1NTE5OTgyMTI3NjYiLCJuYW1lIjoiQ2FybG9zIEVkdWFyZG8gRHVhcnRlIFNjaHdhbG0iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJrcmxzZWR1QGdtYWlsLmNvbSIsImdpdmVuX25hbWUiOiJDYXJsb3MiLCJmYW1pbHlfbmFtZSI6IkVkdWFyZG8gRHVhcnRlIFNjaHdhbG0iLCJlbWFpbCI6ImtybHNlZHVAZ21haWwuY29tIn0.Yza4bXigxMXs9DIpC_RrdAieDfk9mghOxSuowCcSLmUpAi7e1XKKJN29pUW06cGr7e-eX1MP9nzd_xxgFfGb1XVLGaPB6t1kRVuR4k7u9fZTdsueS6csStrjlVQhhne3fKvipNq54pwO4FjINyfuzX8fBAttkSVEZNQP_ymahCPzfTLfUqzZXTbSSeX0SR4IHcZ3zREetD0pVA5qNEWixaTh5nOEN3oPvq81D3AFa0BgaFTuRuTtm6iHGks9Kdq2bAk9QqT0yDNIYKBkvjRJ8euQ9jh1txxcJhaWbRffWBtyS9l7zZjBn4EMVURC56K9orw5qLjACWrfpPrry5s5gg",
        "analitica-token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY0NWYyMTM1LWQwNTAtNGZmOC04NTE0LTc0ZTBlNDk2NzgwOSIsImVtYWlsIjoia3Jsc2VkdUBnbWFpbC5jb20iLCJuYW1lIjoiQ2FybG9zIEVkdWFyZG8gRHVhcnRlIFNjaHdhbG0iLCJleHAiOjE3ODc3NjE5NzYsImlhdCI6MTc4NTE2OTk3NiwibmJmIjoxNzg1MTY5OTc2fQ.lKr4Uah59kjfVEiKizm_BLjSKS-gEuTxX5_JtFmVn5s",
    }

    response = requests.get(url, headers=headers, cookies=cookies)
    response.raise_for_status()
    data = response.json()

    output_file = f"{ticker}_indicators.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Dados salvos em {output_file}")
    return data


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python fetch_indicators.py <TICKER>")
        print("Exemplo: python fetch_indicators.py PINE4")
        sys.exit(1)

    ticker = sys.argv[1].upper()
    fetch_indicators(ticker)
