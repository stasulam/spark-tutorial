# spark-tutorial

Materiały ze szkolenia 26/02/19: *From small data in Python to Big Data model in Apache Spark*.

# Notebooks

## `01_jupyter_intro`
Notebook wprowadzający do Jupyter Notebooks (z przykładowymi typami wykorzystywanymi w Pythonie). Notebook kończy się zadaniem polegającym na implementacji funkcji zwracającej informacje o tym, czy podana liczba jest parzyta, czy nieparzysta.

```python
def odd_or_even(number: int) -> str:
    if number % 2 == 0:
        return 'even'
    else:
        return 'odd'
```

## `02_dataframes_api_walkthrough`
Notebook wprowadzający do `DataFrameAPI` w `pyspark`. Prezentuje sposoby wczytywania zbiorów danych (`SQL`, `csv`). Przedstawia podstawowe operacje, które możemy wykonywać na `DataFrameAPI`:

Notebook wprowadzający do `DataFrameAPI` w `pyspark`. Prezentuje sposoby wczytywania zbiorów danych (`sql`, `csv`) i dokonywania podstawowych operacji, m.in. stosowaniu `udf` (*user-defined functions*) na wybranych kolumnach, dodawaniu nowych kolumn, filtrowaniu zbiorów danych na podstawie informacji o wartości kolumny, łączeniu i sortowaniu tabel, etc. 

Notebook kończy ćwiczenie polegające na implementacji własnej funkcji, która przypisze *zero-jedynkową* flagę wskazującą na typ płatności. Przykładowa implementacja:

```python
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

def payment_type(value):
    mapping = {'Cash': 1, 'Credit Card': 0}
    return mapping[value]

udf_payment_type = udf(payment_type, IntegerType())

# simple query with *udf* function
taxi.select(udf_payment_type(taxi.payment_type)).show(10)
```
