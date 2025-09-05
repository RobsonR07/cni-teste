# Bot de Captura de Dados do IPCA (IBGE)

Este projeto é um robô (bot) em Python que captura metadados sobre a série histórica do IPCA, diretamente da API SIDRA do IBGE. Ele processa as informações e salva os dados em múltiplos arquivos `.parquet` em uma pasta local.

## 📁 Estrutura do Projeto

- `bot.py`: Script principal do bot.
- `requirements.txt`: Lista de dependências do projeto.
- `saida_dados/`: Pasta onde os arquivos gerados são salvos.
- `questao.txt`: Textos com a solução de navegação em dados indisponiveis.

## 🚀 Como Executar

**1. Clone o Repositório**

```bash
git clone https://github.com/RobsonR07/teste-cni.git
```

**2. Acesse a Pasta do Projeto**

```bash
cd teste-cni
```

**3. Instale as Dependências**

```bash
pip install -r requirements.txt
```

**4. Execute o Bot**

```bash
python bot.py
```

## ✅ Resultado

Após a execução, uma pasta chamada `saida_dados` será criada no diretório, contendo os arquivos `.parquet` com os dados que foram extraídos.

## 👨‍💻 Autor

- **RobsonR07**
