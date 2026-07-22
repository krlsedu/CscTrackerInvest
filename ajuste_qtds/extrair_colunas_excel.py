# -*- coding: utf-8 -*-
import pandas as pd

# Nome do seu arquivo Excel
arquivo_excel = "posicao-2026-07-17-15-45-09.xlsx"

# --- CONFIGURAÇÃO ---
# Defina aqui quais colunas pegar de cada aba.
# Você pode usar os nomes das colunas (ex: "Data", "Valor") ou as letras "A,B".
config_abas = {
    "Acoes": ["Código de Negociação", "Quantidade"],  # Pega pelo nome do cabeçalho
    "BDR": ["Código de Negociação", "Quantidade"],
    "ETF": ["Código de Negociação", "Quantidade"],
    "Fundo de Investimento": ["Código de Negociação", "Quantidade"],
}

lista_dfs = []

# Carrega o arquivo Excel (sem ler tudo ainda, para economizar memória)
xls = pd.ExcelFile(arquivo_excel)

for nome_aba, colunas in config_abas.items():
    if nome_aba in config_abas:
        try:
            # Lê a aba específica com as colunas configuradas
            df = pd.read_excel(xls, sheet_name=nome_aba, usecols=colunas)

            # Opcional: Renomear as colunas para que fiquem padronizadas no CSV final
            # Se não renomear, o CSV terá várias colunas se os nomes forem diferentes.
            # Aqui forçamos para "Coluna 1" e "Coluna 2" para empilhar tudo.
            df.columns = ["ticker", "quantidade"]

            # Adiciona uma coluna extra para saber de qual aba veio (opcional)
            df["Origem"] = nome_aba

            lista_dfs.append(df)
            print(f"Sucesso ao ler: {nome_aba}")

        except Exception as e:
            print(f"Erro ao ler a aba '{nome_aba}': {e}")

# Junta todos os dados em um único DataFrame
if lista_dfs:
    df_final = pd.concat(lista_dfs, ignore_index=True)

    # remover linhas com tickers vazios
    df_final = df_final[df_final["ticker"].str.strip() != ""]
    # remover linhas com tickers nulos
    df_final = df_final[df_final["ticker"].notna()]
    # Salva em CSV
    df_final.to_csv("tickers.csv", index=False, encoding="utf-8-sig")
    print("Arquivo 'relatorio_final.csv' criado com sucesso!")
else:
    print("Nenhum dado foi processado.")
