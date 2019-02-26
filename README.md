# spark-tutorial

Materiały ze szkolenia 26/02/19: *From small data in Python to Big Data model in Apache Spark*.

# Notebooks

## [`01_jupyter_intro`](https://github.com/stasulam/spark-tutorial/blob/notebooks/notebooks/01_jupyter_starter.ipynb)

Notebook wprowadzający do Jupyter Notebooks (z przykładowymi typami wykorzystywanymi w Pythonie). Notebook kończy się zadaniem polegającym na implementacji funkcji zwracającej informacje o tym, czy podana liczba jest parzyta, czy nieparzysta.

```python
def odd_or_even(number: int) -> str:
    if number % 2 == 0:
        return 'even'
    else:
        return 'odd'
```

## [`02_dataframes_api_walkthrough`](https://github.com/stasulam/spark-tutorial/blob/notebooks/notebooks/02_dataframes_api_walkthrough.ipynb)

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

## [`03_data_preparation`](https://github.com/stasulam/spark-tutorial/blob/notebooks/notebooks/03_data_preparation.ipynb)

Notebook, którego celem jest przygotowanie danych do modelowania. Zauważmy jednak, że proponowany sposób przygotowania zbioru danych, który zostanie wykorzystany na etapie modelowania, może implikować problem leaku informacyjnego przy niektórych metodach `feature engineering`. Np. wyznaczenie wartości kwantyli zmiennej `trip_miles` na całej tabeli - po dokonaniu podziału na zbiór *treningowy* i *testowy* - będzie przenosiło informację do zbioru *testowego*.

## [`04_mllib_model_training`](https://github.com/stasulam/spark-tutorial/blob/notebooks/notebooks/04_mllib_model_training.ipynb)

Notebook, którego celem jest wprowadzenie do [`MLLib`](https://spark.apache.org/docs/2.3.2/ml-guide.html). Podstawowe informacje dot. [`MLLib`](https://spark.apache.org/docs/2.3.2/ml-guide.html):

* działa wyłącznie z danymi w postaci numerycznej,
* instancje klas modeli `ML` wymagają definicji zmiennych objaśniających (w postaci nazwy będącej wynikiem zastosowania `VectorAssembler`) i zmiennej objaśnianej,
* `API` zbliżone do konwencji stosowanej w `scikit-learn`, tj. `.fit`, `.transform`, etc.

Notebook prezentuje sposób stosowania klas wykorzystywanych w *preprocessingu*, tj. `VectorAssembler`, `Binarizer`, `StringIndexer`, `OneHotEncoder`, etc. W sekcji dotyczącej modelowania wykorzystano klasę `LogisticRegression`. Następnie, poszukiwano optymalnych wartości *hyperparametrów* modelu z wykorzystaniem `CrossValidator` i `ParamGridBuilder`.

Zaproponowaną miarą oceny *performance* modeli był błąd na zbiorze testowym mierzony jako `1 - accuracy`:

* wypełnienie samymi zerami: `0.462161`,
* regresja logistyczna na wszystkich zmiennych: `0.40878`,
* regresja logistyczna po *feature engineering* (6 cech): `0.022286`,
* regresja logistyczna po *feature engineering* i optymalizacji *hyperparameters*: `0.022286`.

## [`05_pipelines`](https://github.com/stasulam/spark-tutorial/blob/notebooks/notebooks/05_pipelines.ipynb)

Notebook, którego celem jest wprowadzenie do budowania `Pipeline` w `pyspark`. Powtórzono proces budowy modelu z poprzedniego notebooka z wykorzystaniem klasy `Pipeline` (dzięki temu możemy uniknąć problemu z leakiem informacyjnym, o którym wspominano wcześniej). Notebook został dodany poglądowo (bez egzekucji komórek). 
