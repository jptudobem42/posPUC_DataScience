!pip install boto3

from boto.s3.connection import S3Connection
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
conn = S3Connection('access_key_id', 'secret_access_key')
bucket = conn.get_bucket('datalake-turma5.1')

for key in bucket.list():
    print(key.name.encode('utf-8'))

import boto3
import matplotlib.pyplot as plt

class CampeonatoBrasileiro:
    def __init__(self, linha):
        dados = linha.split(";")
        self.rodada = dados[0]
        self.data_jogo = dados[1]
        self.hora = dados[2]
        self.time_mandante = dados[4]
        self.time_visitante = dados[5]
        self.time_vencedor = dados[6]
        self.campo = dados[7]
        self.placar_mandante = int(dados[8])
        self.placar_visitante = int(dados[9])
        self.estado_mandante = dados[10]
        self.estado_visitante = dados[11]
        self.estado_vencedor = dados[12]

def le_arquivoS3():
    s3 = boto3.resource('s3', aws_access_key_id='access_key_id', aws_secret_access_key='secret_access_key')
    bucket = s3.Bucket('datalake-turma5.1')
    jogos = []
    for obj in bucket.objects.filter(Prefix='arquivos_sequencial/jogos.txt'):
        corpo = obj.get()['Body'].read().decode('utf-8')
        linhas = corpo.splitlines()
        for linha in linhas[1:]:
            jogos.append(CampeonatoBrasileiro(linha))
    return jogos

def analisa_jogos(jogos, nome_time):
    confrontos = {}
    for jogo in jogos:
        if jogo.time_mandante == nome_time or jogo.time_visitante == nome_time:
            outro_time = jogo.time_visitante if jogo.time_mandante == nome_time else jogo.time_mandante
            if outro_time not in confrontos:
                confrontos[outro_time] = {
                    'vitorias_casa': 0, 'vitorias_fora': 0,
                    'derrotas_casa': 0, 'derrotas_fora': 0,
                    'gols_marcados_casa': 0, 'gols_marcados_fora': 0,
                    'gols_sofridos_casa': 0, 'gols_sofridos_fora': 0
                }

            if jogo.time_vencedor == nome_time:
                if jogo.time_mandante == nome_time:
                    confrontos[outro_time]['vitorias_casa'] += 1
                else:
                    confrontos[outro_time]['vitorias_fora'] += 1
            elif jogo.time_vencedor == outro_time:
                if jogo.time_mandante == nome_time:
                    confrontos[outro_time]['derrotas_casa'] += 1
                else:
                    confrontos[outro_time]['derrotas_fora'] += 1

            if jogo.time_mandante == nome_time:
                confrontos[outro_time]['gols_marcados_casa'] += jogo.placar_mandante
                confrontos[outro_time]['gols_sofridos_casa'] += jogo.placar_visitante
            else:
                confrontos[outro_time]['gols_marcados_fora'] += jogo.placar_visitante
                confrontos[outro_time]['gols_sofridos_fora'] += jogo.placar_mandante

    mais_vencidos = sorted(confrontos.items(), key=lambda x: (x[1]['vitorias_casa'] + x[1]['vitorias_fora']), reverse=True)
    mais_perdidos = sorted(confrontos.items(), key=lambda x: (x[1]['derrotas_casa'] + x[1]['derrotas_fora']), reverse=True)
    return mais_vencidos[0] if mais_vencidos else ("", {}), mais_perdidos[0] if mais_perdidos else ("", {})

def main():
    jogos = le_arquivoS3()
    nome_time = input("Digite o nome do time: ")
    times_encontrados = set(jogo.time_mandante for jogo in jogos) | set(jogo.time_visitante for jogo in jogos)

    if nome_time not in times_encontrados:
        print(f"Time '{nome_time}' não encontrado nos jogos disponíveis.")
        return
    mais_vencido, mais_perdido = analisa_jogos(jogos, nome_time)
    if mais_vencido[0]:
        print(f"\nTime que mais venceu: {mais_vencido[0]}")
        print(f"Vitórias em casa: {mais_vencido[1]['vitorias_casa']}, Vitórias fora: {mais_vencido[1]['vitorias_fora']}")
        print(f"Gols marcados em casa: {mais_vencido[1]['gols_marcados_casa']}, Gols marcados fora: {mais_vencido[1]['gols_marcados_fora']}")
        print(f"Gols sofridos em casa: {mais_vencido[1]['gols_sofridos_casa']}, Gols sofridos fora: {mais_vencido[1]['gols_sofridos_fora']}")
    if mais_perdido[0]:
        print(f"\nTime que mais perdeu: {mais_perdido[0]}")
        print(f"Derrotas em casa: {mais_perdido[1]['derrotas_casa']}, Derrotas fora: {mais_perdido[1]['derrotas_fora']}")
        print(f"Gols marcados em casa: {mais_perdido[1]['gols_marcados_casa']}, Gols marcados fora: {mais_perdido[1]['gols_marcados_fora']}")
        print(f"Gols sofridos em casa: {mais_perdido[1]['gols_sofridos_casa']}, Gols sofridos fora: {mais_perdido[1]['gols_sofridos_fora']}")

if __name__ == "__main__":
    main()