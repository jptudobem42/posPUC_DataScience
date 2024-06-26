# -*- coding: utf-8 -*-
"""Forecast de Vendas.ipynb

# BASE DE DADOS

Descrição dos atributos:

* DATA: Data da venda
* TOTAL: Total em R$ das vendas

O dataset apresenta dados reais de uma empresa de comércio do varejo onde o objetivo é prever o valor das vendas do futuro utilizado o modelo SARIMA.
"""

#!pip install pmdarima

# Commented out IPython magic to ensure Python compatibility.
# Bibliotecas

import pandas as pd
import numpy as np
import seaborn as sns
import pmdarima as pm
import matplotlib.pyplot as plt
# %matplotlib inline

import plotly.graph_objects as go
from sklearn.model_selection import train_test_split
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error
from statsmodels.tsa.stattools import adfuller
from pandas.plotting import autocorrelation_plot
from pmdarima.arima import auto_arima
import statsmodels.api as sm

import warnings
warnings.filterwarnings("ignore")

"""# UPLOAD DATASET

"""

data = '/content/dataset_vendas.xlsx'
df = pd.read_excel(data)

"""# VISÃO GERAL DOS DADOS"""

df.shape

df.head()

df.tail()

df.info()

df['TOTAL'].describe()

"""# PREPARAÇÃO DOS DADOS"""

# Agrupando os dados por mês
df2 = df.resample('M', on='DATA').sum()

df2.head()

df2.tail()

"""# ANÁLISE EXPLORATÓRIA"""

# Boxplot (Identificar Outliers)
plt.boxplot(df2['TOTAL'])
plt.show()

# Gráfico de linhas
anos = df2.index.year.unique()

for ano in anos:
    df_ano = df2[df2.index.year == ano]

    plt.figure(figsize=(10, 5))
    plt.plot(df_ano.index, df_ano['TOTAL'])
    plt.title(f'Gráfico de Série Temporal para {ano}')
    plt.xlabel('Mês')
    plt.ylabel('Valor Vendas')
    plt.xticks(pd.date_range(start=f"{ano}-01-01", end=f"{ano}-12-31", freq='M'),
               [d.strftime('%b') for d in pd.date_range(start=f"{ano}-01-01", end=f"{ano}-12-31", freq='M')],
               rotation=45)
    plt.tight_layout()
    plt.show()

"""## TESTE DE ESTACIONARIDADE

* Estacionariedade refere-se a uma série temporal cujas propriedades estatísticas (como média, variância) não mudam ao longo do tempo. Uma série estacionária não tem tendência ou sazonalidade.

* Não-estacionariedade refere-se a uma série temporal que possui tendências ou sazonalidades, o que significa que suas propriedades estatísticas mudam ao longo do tempo.
"""

# Teste de Dickey-Fuller
def adfuller_test(sales):
  result = adfuller(sales)
  labels = ['ADF Test Statistic', 'p-value', '#Lags Used', 'Number of Observations Used']
  for value,label in zip(result,labels):
    print(label+': '+str(value))
  if result[1] <= 0.05: # p-value
    print("\nTipo de série: Estacionária")
  else:
    print("\nTipo de série: Não-estacionária")

adfuller_test(df2['TOTAL'])

"""## DECOMPOSIÇÃO"""

# Decomposição de sazonalidade
result = seasonal_decompose(df2, model='multiplicative') # parâmetro 'model' ajustado para "multiplicative" pois apresenta uma sazonalidade constante ao longo do tempo

result.plot()
plt.show()

"""## AUTOCORRELAÇÃO

A autocorrelação mede a relação entre uma observação e suas observações anteriores (ou lags) em uma série temporal. Analisar a autocorrelação é fundamental ao modelar séries temporais para entender a dependência entre os valores ao longo do tempo.
"""

# Autocorrelação
autocorrelation_plot(df2['TOTAL'])
plt.title(f'Autocorrelação')
plt.show()

# Autocorrelação (ACF) - Ajuda na definição dos parâmetros MA (q,Q)
fig, ax = plt.subplots(figsize=(10, 5))
sm.graphics.tsa.plot_acf(df2['TOTAL'], lags=40, ax=ax)
ax.set_title(f'Autocorrelação (ACF)')
plt.show()

# Autocorrelação parcial (PACF) - Ajuda na definição dos parâmetros AR (p,P)
fig, ax = plt.subplots(figsize=(10, 5))
sm.graphics.tsa.plot_pacf(df2['TOTAL'], lags=30, ax=ax)
ax.set_title(f'Autocorrelação (PACF)')
plt.show()

"""# TRATAMENTO DE OUTLIERS

Iremos substituir todos os valores abaixo do limite inferior pelo próprio limite inferior, e todos os valores acima do limite superior pelo limite superior. Esse método é conhecido como "winsorizing" e ajuda a minimizar o impacto de outliers extremos sem excluir os pontos de dados.




"""

Q1 = df2['TOTAL'].quantile(0.25)
Q3 = df2['TOTAL'].quantile(0.75)
IQR = Q3 - Q1

# Definir os limites para outliers
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR

# Tratar outliers substituindo-os pelos limites
df2['TOTAL'] = np.where(df2['TOTAL']< lower_bound, lower_bound, df2['TOTAL'])
df2['TOTAL'] = np.where(df2['TOTAL'] > upper_bound, upper_bound, df2['TOTAL'])

plt.boxplot(df2['TOTAL'])
plt.show()

"""# SARIMA

O modelo SARIMA (Seasonal Autoregressive Integrated Moving Average) é uma técnica para previsão e análise de séries temporais, considerando tendências, ciclos sazonais e padrões autocorrelacionados. Ele combina componentes autoregressivos, integrados e de médias móveis, adaptando-se a diferentes padrões e flutuações nos dados. É utilizado para realizar previsões com base em observações passadas.
"""

# Divide o modelo em treino e teste (Pega quatro anos completos para o treino e o restante para teste)
train = df2[:60]
test = df2[60:]

print(f"Train dates : {train.index.min()} --- {train.index.max()}  (n={len(train)})")
print(f"Test dates  : {test.index.min()} --- {test.index.max()}  (n={len(test)})")

# Cria o modelo SARIMA utilizando o auto_arima (ajusta automaticamente os hiperparâmetros)
model = pm.auto_arima(train,
                      start_p=1, d=None, start_q=1,
                      test='adf',
                      max_p=5, max_q=5,
                      m=12, # considerar a sazonalidade a cada 12 observações (ou meses)
                      start_P=1, D=None, start_Q=1,
                      stationarity=False,
                      seasonal=True,
                      #seasonal_test='ch',
                      trace=True,
                      error_action='ignore',
                      suppress_warnings=True,
                      stepwise=True,
                      information_criterion='aic')

# Previsão para os dados de treino
predictions_train = model.predict_in_sample()

# Previsão para os primeiros 9 meses de 2023
predictions_test = model.predict(n_periods=len(test))

# Retreinando o modelo com todos os dados até Setembro de 2023 - Base real
model_full = model.update(df2)

# Previsão para Outubro, Novembro e Dezembro de 2023
future_predictions = model_full.predict(n_periods=3)

"""# AVALIAÇÃO DO MODELO

## SUMÁRIO
"""

print(model.summary())

"""Os p-values (P>|z|) para os coeficientes são menores do que 0,05, indicando que os coeficientes são estatisticamente significativos.

## GRÁFICO
"""

# Criar um gráfico
fig = go.Figure()

# Adicionar linhas ao gráfico
fig.add_trace(go.Scatter(x=train.index, y=train['TOTAL'], mode='lines+markers', name='Dados Reais de Treino', line=dict(color='blue')))
fig.add_trace(go.Scatter(x=test.index, y=test['TOTAL'], mode='lines+markers', name='Dados Reais de Teste', line=dict(color='green')))
fig.add_trace(go.Scatter(x=test.index, y=predictions_test, mode='lines+markers', name='Previsões para Teste', line=dict(color='red', dash='dash')))
fig.add_trace(go.Scatter(x=pd.date_range(start='2023-10', periods=3, freq='M'), y=future_predictions, mode='lines+markers', name='Previsões Futuras', line=dict(color='orange', dash='dash')))

# Adicionar título e legenda
fig.update_layout(
    title={
        'text': 'Previsões SARIMA',
        'x': 0.5,
        'xanchor': 'center'
    },
    xaxis_title='PERÍODO',
    yaxis_title='TOTAL',
    showlegend=True
)

# Ajustar o tamanho do gráfico
fig.update_layout(autosize=False, width=1500, height=700)

# Mostrar o gráfico
fig.show()

# Exibe as previsões em um dataframe
dates = pd.date_range(start='2023-10', periods=3, freq='M')

future_df = pd.DataFrame(future_predictions, index=dates, columns=["previsao"])
future_df['previsao'] = future_df['previsao'].apply(lambda x: 'R$ {:,.2f}'.format(x).replace(',', 'x').replace('.', ',').replace('x', '.'))
future_df

"""## MÉDIA DOS ERROS"""

# Calcula as métricas de desempenho para os dados de treino
print('Métricas de desempenho para os dados de treino:')
rmse_train = np.sqrt(mean_squared_error(train, predictions_train))
print('RMSE:', rmse_train)
mae_train = mean_absolute_error(train, predictions_train)
print('MAE:', mae_train)
mape_train = mean_absolute_percentage_error(train, predictions_train)
print('MAPE:', mape_train)

# Calcula as métricas de desempenho para os dados de teste
print('\nMétricas de desempenho para os dados de teste:')
rmse_test = np.sqrt(mean_squared_error(test, predictions_test))
print('RMSE:', rmse_test)
mae_test = mean_absolute_error(test, predictions_test)
print('MAE:', mae_test)
mape_test = mean_absolute_percentage_error(test, predictions_test)
print('MAPE:', mape_test)

"""> Analisando as médias dos erros:


* RMSE: A raiz do erro quadrático médio é uma métrica que expressa a média das diferenças ao quadrado entre os valores reais e previstos. Valores menores indicam um melhor ajuste do modelo. Nota-se uma melhoria significativa do RMSE quando se passa dos dados de treino para os dados de teste, o que é um bom sinal.

* MAE: O erro absoluto médio representa a média das diferenças absolutas entre os valores previstos e reais. Assim como o RMSE, o MAE é menor nos dados de teste em comparação com os dados de treino, indicando uma melhora no desempenho do modelo.

* MAPE: O erro percentual absoluto médio mostra a média das diferenças percentuais absolutas entre os valores previstos e reais. Uma redução substancial de 20.26% para 7.48% é observada, o que sugere que o modelo é consideravelmente mais preciso ao fazer previsões nos dados de teste.

# CONCLUSÃO

A análise exploratória revela padrões que se repetem em intervalos regulares. O gráfico de decomposição exibe uma tendência ascendente, indicando um crescimento nas vendas ao longo dos anos, e também destaca picos sazonais.

A avaliação do modelo pelo gráfico "Previsões SARIMA" mostra que as projeções para a base teste seguem de perto os dados reais, sugerindo um bom ajuste do modelo. As previsões para os últimos meses também parecem promissoras, corroborando com a tendência de crescimento anual e o aumento sazonal nas vendas observados nos anos anteriores.

Vale ressaltar que estamos lidando com um conjunto de dados real e conhecido, especialmente no que se refere à tendência de crescimento das vendas e à sazonalidade. Apesar de algumas preocupações, especialmente com os valores mais elevados de RMSE e MAE nos dados de treino (que podem ser justificados por contextos ausentes na base, como a pandemia e investimentos realizados na loja), a melhora nas métricas ao passar dos dados de treino para os de teste é um indicativo positivo. No entanto, é essencial avaliar esses resultados no contexto específico das necessidades e objetivos de negócios para determinar a adequação e confiabilidade do modelo para previsões de vendas futuras. Como procedimento de validação, seria prudente aguardar os resultados de vendas dos meses subsequentes para confrontá-los com as previsões do modelo, a fim de avaliar sua precisão e confiabilidade.
"""