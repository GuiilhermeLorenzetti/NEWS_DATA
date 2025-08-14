## Análise de dados de viagens (NCR Ride Bookings)

Este repositório contém um notebook de análise (`Teste_estudo/analise.ipynb`) que carrega um conjunto de dados de corridas, realiza inspeções iniciais e permite consultas SQL sobre o `DataFrame` via `pandasql`.

### Objetivos
- **Explorar** o dataset `Teste_estudo/ncr_ride_bookings (1).csv`.
- **Visualizar** amostras dos dados com `df.head()`.
- **Habilitar consultas SQL** sobre o `DataFrame` com `pandasql`.

### Estrutura do projeto
- `Teste_estudo/analise.ipynb`: Notebook principal da análise.
- `Teste_estudo/ncr_ride_bookings (1).csv`: Dataset usado no notebook.

### Requisitos
- Python 3.13 (ou compatível)
- Pacotes Python:
  - `pandas`
  - `numpy`
  - `matplotlib`
  - `seaborn`
  - `pandasql`

Instalação sugerida (fora do notebook):

```bash
pip install pandas numpy matplotlib seaborn pandasql
```

Observação: O notebook possui células com `!pip install ...` para garantir dependências no ambiente de execução.

### Como executar o notebook
1. Garanta que as dependências estejam instaladas no seu ambiente Python.
2. Abra o Jupyter (ou VS Code/Cursor com suporte a notebooks) na raiz do repositório.
3. Execute o notebook `Teste_estudo/analise.ipynb` célula a célula.
4. Verifique se o caminho do CSV está correto. Exemplo de leitura no notebook:

```python
df, sql = ler_csv_sql(r'C:\\Users\\guilherme.lorenzetti\\Documents\\GitHub\\teste_CE\\Teste_estudo\\ncr_ride_bookings (1).csv')
df.head()
```

Caso prefira caminho relativo (recomendado quando estiver na raiz do repo):

```python
df, sql = ler_csv_sql('Teste_estudo/ncr_ride_bookings (1).csv')
```

### Funções e consultas SQL
O notebook define a função `ler_csv_sql(path_csv)` para carregar o CSV e facilitar consultas SQL com `pandasql`.

Para realizar uma consulta diretamente com `pandasql` sobre o `DataFrame` global `df`:

```python
import pandasql as psql

resultado = psql.sqldf(
    """
    SELECT *
    FROM df
    WHERE "Pickup Location" = 'Palam Vihar'
    """,
    globals(),  # importante: df precisa estar visível no escopo passado
)
```

Se desejar manter um wrapper que não dependa de `globals()`, uma alternativa robusta é fechar o `df` no escopo do wrapper:

```python
import pandas as pd
import pandasql as psql

def ler_csv_sql(path_csv: str):
    df = pd.read_csv(path_csv)

    def sql(query: str):
        # expõe apenas df no ambiente da consulta
        return psql.sqldf(query, {"df": df})

    return df, sql

# uso
df, sql = ler_csv_sql('Teste_estudo/ncr_ride_bookings (1).csv')
sql("SELECT COUNT(*) AS total FROM df")
```

### Dicas e observações
- Se encontrar erro de escopo com `pandasql`, garanta que o `df` esteja no ambiente passado a `sqldf` (via `globals()` ou um dicionário contendo `{"df": df}`).
- Em Windows, paths com espaços e parênteses devem ser corretamente escapados ou colocados entre aspas.

### Licença
Uso interno/estudo.


