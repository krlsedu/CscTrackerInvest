Aqui está o Release Notes técnico para a v26.30.001:

# Release Notes - v26.30.001

## 🚀 Features
- **Ajuste de Dados:** Adicionado script SQL e utilitário Python para ajustes de quantidade de ativos.
- **Cálculo de Ganhos:** Refatorado o cálculo de ganho diário e total no `investment_handler`, incluindo filtro por tipo de movimentação.
- **Recomendações de Compra/Venda:** Melhorias na lógica do `investment_handler` para recomendações mais precisas.
- **Atualização de Preços:** Otimizada a atualização de preços de ações, passando de individual para em lote (bulk).
- **Agendamento:** Substituído o uso de `threading` pelo `SchedulerService` no `app.py`.

## 🐛 Fixes
- **Importação de Preços:** Adicionado log de erros durante a importação de preços para fundos.
- **Cálculo de Dividendos:** Refatorado o cálculo de dividendos no serviço `att_stocks`.
- **Movimentações:** Separadas as movimentações de compra e venda de ações no `investment_handler`.
- **Validação de Preço:** Atualizada a validação de preço no `investment_handler`.

## 🔧 Chore
- **Dependências:** Atualizadas as dependências do projeto, incluindo a adição do `numpy` ao `requirements.txt`.
- **Limpeza de Código:** Removidos diversos arquivos Python não utilizados da base de código.
- **Logs:** Substituídos comandos `print` por `logging` no `app.py`.
- **Refatoração:** Renomeados arquivos de serviço para o padrão snake_case (ex: `AttStocks.py` para `att_stocks.py`).
- **CI/CD:** Atualizações no `Jenkinsfile` e `.github/workflows/release-file.yml`.