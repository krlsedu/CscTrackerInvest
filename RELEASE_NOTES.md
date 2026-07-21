# Release Notes - v26.30.004

## 🚀 Features
*(Nenhuma nova funcionalidade adicionada nesta versão)*

## 🐛 Fixes
* **Payloads de Investimento:** Remoção da chave redundante `user_id` nos payloads do serviço de investimentos (`investment_handler.py`), garantindo a consistência e otimizando a estrutura de dados trafegada.

## 🔧 Chore
* **Infraestrutura/Docker:** Adição do arquivo `.dockerignore` para excluir arquivos desnecessários do contexto de build. Isso melhora o tempo de build, reduz o tamanho final da imagem e evita o vazamento de arquivos sensíveis ou de ambiente local.