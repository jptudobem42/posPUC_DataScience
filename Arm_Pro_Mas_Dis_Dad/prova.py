# %%
!pip install boto boto3

# %%
import boto3
import os
import ssl

#%%
# DEFINIÇÃO DE VARIÁVEIS

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCES_KEY = ''
bucket_name = 'datalake-turma5.1'
amazon_path = 'prova/'

#%%

class AmazonS3:
    bucket = None
    bucket_name = None
    aws_secret_access_key = None
    aws_access_key_id = None
    region_name = None
    resource = None
    file_name = None

    def __init__(self):
        ssl._create_default_https_context = ssl._create_unverified_context
        self.aws_access_key_id = AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = AWS_SECRET_ACCES_KEY
        self.bucket = None
        self.bucket_name = bucket_name
        self.amazon_path = amazon_path
        self.region_name = 'sa-east-1'
        self.resource = 's3'

    def connect_s3(self):
        try:
            session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
            s3 = session.resource(self.resource)
            self.bucket = s3.Bucket(self.bucket_name)
        except Exception as e:
            print(e)

    def put_object_bucket(self, file_path):
        file_name = os.path.basename(file_path)
        amazon_destiny = self.amazon_path + file_name
        try:
            with open(file_path, 'rb') as data:
                self.bucket.put_object(Key=amazon_destiny, Body=data)
            url = f'https://{self.bucket_name}.s3.amazonaws.com/{amazon_destiny}'
            return url
        except Exception as e:
            print(e)
            return None

    def post_files_to_s3(self, file_paths):
        self.connect_s3()
        urls = []
        for file_path in file_paths:
            url = self.put_object_bucket(file_path)
            if url:
                print(f'Arquivo enviado com sucesso: {url}')
                urls.append(url)
            else:
                print(f'Erro ao enviar o arquivo: {file_path}')
        return urls
    
# %%
#!pip install requests
import requests

def get_file(sigla_uf):
    codigo_uf = {
        "AC": "12", "AL": "27", "AP": "16", "AM": "13",
        "BA": "29", "CE": "23", "DF": "53", "ES": "32",
        "GO": "52", "MA": "21", "MT": "51", "MS": "50",
        "MG": "31", "PA": "15", "PB": "25", "PR": "41",
        "PE": "26", "PI": "22", "RJ": "33", "RN": "24",
        "RS": "43", "RO": "11", "RR": "14", "SC": "42",
        "SP": "35", "SE": "28", "TO": "17"
    }
    
    if sigla_uf in codigo_uf:
        codigo_uf = codigo_uf[sigla_uf]
    else:
        print(f"A sigla {sigla_uf} não é válida.")
        return

    url_base = "https://sidra.ibge.gov.br/geratabela?format=xlsx&name=tabela9514_UF_MUN_{uf}.xlsx&terr=N&rank=-&query=t/9514/n3/{uf}/n6/in%20n3%20{uf}/v/all/p/all/c2/all/c287/6653,49108,49109,60040,60041,93070,93084,93085,93086,93087,93088,93089,93090,93091,93092,93093,93094,93095,93096,93097,93098,100362/c286/113635/d/v1000093%202/l/v,p%2Bc2%2Bc287,t%2Bc286" 
    url_completa = url_base.format(uf=codigo_uf)

    resposta = requests.get(url_completa)

    if resposta.status_code == 200:
        nome_arquivo = f"tabela9514_UF_MUN_{sigla_uf}.xlsx"
        with open(nome_arquivo, "wb") as arquivo:
            arquivo.write(resposta.content)
        print(f"Arquivo {nome_arquivo} salvo com sucesso!")
    else:
        print(f"Erro ao baixar o arquivo. Código de status: {resposta.status_code}")

# %%
import numpy as np
import pandas as pd

def carrega_dados(arquivo):
    df = pd.read_excel(arquivo, header=5)
    df = df.iloc[1:-1]
    return df

def processa_colunas(df):
    colunas_homem = ['Unnamed: 0'] + [col for col in df.columns[2:34] if '.1' not in col and '.2' not in col]
    colunas_mulher = ['Unnamed: 0'] + [col for col in df.columns[35:] if '.2' in col]
    return colunas_homem, colunas_mulher

def prepara_df(df, colunas, genero):
    df_preparado = df[colunas].copy()
    df_preparado.columns = [col.replace('.2', '') for col in colunas]
    df_preparado.rename(columns={'Unnamed: 0': 'municipio'}, inplace=True)
    df_preparado['genero'] = genero
    df_preparado = converte_para_numerico(df_preparado)
    return df_preparado

def converte_para_numerico(df):
    for col in df.columns[1:-1]:
        df[col] = pd.to_numeric(df[col].replace('-', 0), errors='coerce').fillna(0).astype(int)
    return df

def melt_df(df):
    colunas_faixas_etarias = [col for col in df.columns if "anos" in col]
    df_melted = df.melt(id_vars=['municipio', 'genero'], value_vars=colunas_faixas_etarias, var_name='faixa_etaria', value_name='populacao')
    return df_melted

#%%
def faixa_etaria(idade):
    if idade < 5:
        return "0 a 4 anos"
    elif idade < 10:
        return "5 a 9 anos"
    elif idade < 15:
        return "10 a 14 anos"
    elif idade < 20:
        return "15 a 19 anos"
    elif idade < 25:
        return "20 a 24 anos"
    elif idade < 30:
        return "25 a 29 anos"
    elif idade < 35:
        return "30 a 34 anos"
    elif idade < 40:
        return "35 a 39 anos"
    elif idade < 45:
        return "40 a 44 anos"
    elif idade < 50:
        return "45 a 49 anos"
    elif idade < 55:
        return "50 a 54 anos"
    elif idade < 60:
        return "55 a 59 anos"
    elif idade < 65:
        return "60 a 64 anos"
    elif idade < 70:
        return "65 a 69 anos"
    elif idade < 75:
        return "70 a 74 anos"
    elif idade < 80:
        return "75 a 79 anos"
    elif idade < 85:
        return "80 a 84 anos"
    elif idade < 90:
        return "85 a 89 anos"
    elif idade < 95:
        return "90 a 94 anos"
    elif idade < 100:
        return "95 a 99 anos"
    else:
        return "100 anos ou mais"

#%%
UF = input("Digite a sigla do seu estado (UF): ")
get_file(UF)
file = f'tabela9514_UF_MUN_{UF}.xlsx'

df_original = carrega_dados(file)
colunas_homem, colunas_mulher = processa_colunas(df_original)
df_homem = prepara_df(df_original, colunas_homem, 'Homem')
df_mulher = prepara_df(df_original, colunas_mulher, 'Mulher')
df_homem_melted = melt_df(df_homem)
df_mulher_melted = melt_df(df_mulher)
df_final = pd.concat([df_homem_melted, df_mulher_melted], ignore_index=True)

#%%
# VISUALIZA O DF FINAL COM O ARQUIVO TRATADO ANTES DE RESPONDER AS PERGUNTAS

df_final

# DESCOMENTE A LINHA ABAIXO CASO QUEIRA GERAR UM XLSX DO ARQUIVO TRATADO
#df_final.to_excel('df_final.xlsx', index=False)

# %%
## PERGUNTAS DA PROVA

municipio_usuario = str(input("Digite seu município (case sensitive): "))
genero_usuario = input("Digite seu gênero (Homem/Mulher): ")
idade_usuario = int(input("Digite sua idade: "))

faixa_etaria_usuario = faixa_etaria(idade_usuario)

# a) 
faixa_etaria_mais_pessoas = df_final.groupby('faixa_etaria')['populacao'].sum().idxmax()
populacao_faixa_etaria_mais_pessoas = df_final.groupby('faixa_etaria')['populacao'].sum().max()

# b)
municipio_mais_genero_faixa_etaria = df_final[(df_final['faixa_etaria'] == faixa_etaria_usuario) & (df_final['genero'] == genero_usuario)].nlargest(1, 'populacao')
municipio_mais_genero_faixa_etaria_populacao = municipio_mais_genero_faixa_etaria['populacao'].values[0]

# c)
municipio_menos_genero_faixa_etaria = df_final[(df_final['faixa_etaria'] == faixa_etaria_usuario) & (df_final['genero'] == genero_usuario)].nsmallest(1, 'populacao')
municipio_menos_genero_faixa_etaria_populacao = municipio_menos_genero_faixa_etaria['populacao'].values[0]

# d)
maior_faixa_etaria_municipio = df_final[df_final['municipio'].str.contains(municipio_usuario)].groupby('faixa_etaria')['populacao'].sum().idxmax()
populacao_maior_faixa_etaria_municipio = df_final[df_final['municipio'].str.contains(municipio_usuario)].groupby('faixa_etaria')['populacao'].sum().max()

resultados = pd.DataFrame({
    'Perguntas': [
        'A) Faixa Etária com Mais Pessoas',
        f'B) Município com Mais Pessoas ({faixa_etaria_usuario})',
        f'C) Município com Menos Pessoas ({faixa_etaria_usuario})',
        f'D) Maior Faixa Etária em {municipio_usuario}'
    ],
    'Respostas': [
        faixa_etaria_mais_pessoas,
        municipio_mais_genero_faixa_etaria['municipio'].values[0],
        municipio_menos_genero_faixa_etaria['municipio'].values[0],
        maior_faixa_etaria_municipio
    ],
    'População': [
        populacao_faixa_etaria_mais_pessoas,
        municipio_mais_genero_faixa_etaria_populacao,
        municipio_menos_genero_faixa_etaria_populacao,
        populacao_maior_faixa_etaria_municipio
    ]
})

# %%
# VISUALIZA OS RESULTADOS ANTES DE GERAR OS ARQUIVOS

resultados
# %%
# GERA OS ARQUIVOS NO DIRETÓRIO LOCAL COM OS RESULTADOS
# ALTERE O NOME DOS ARQUIVOS PARA O SEU NOME

to_excel = 'nome_aluno.xlsx'
resultados.to_excel(to_excel, index=False)

to_json = 'nome_aluno.json'
resultados.to_json(to_json, orient='records', lines=True)

# %%

amazon_s3 = AmazonS3()
arquivos = ['nome_aluno.json',  'nome_aluno.xlsx']
urls = amazon_s3.post_files_to_s3(arquivos)