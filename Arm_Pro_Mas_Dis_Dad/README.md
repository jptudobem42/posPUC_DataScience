# PROVA FINAL MATÉRIA ARMAZENAMENTO E PROCESSAMENTO MASSIVO DE DADOS

## ENUNCIADO DA QUESTÃO

Com base no censo demografico de 2022 (https://www.ibge.gov.br/estatisticas/sociais/populacao/22827-censo-demografico-2022.html?edicao=38166&t=resultadosLinks to an external site.), baixe o arquivo  População por idade e sexo referente a seu estado de nascimento e faça:

a) Faça um programa em python que identifique qual a faixa etaria possui mais pessoas entre homens e mulheres.

b) Faça um programa python que com base na sua faixa etária e sexo identifique o municipio com mais pessoas.

c) Faça um programa python que com base na sua faixa etária e sexo identifique o municipio com menos pessoas.

d) Faça um programa em python que com base no seu municipio de nascimento identifique a maior faixa etária em anos  entre homens e mulheres.

e) Faça um programa em pyhton que gere dois arquivos um em .xlsx e o outro .json com o resultado das questões acima e salve no  datalake-prova\prova com seu nome a extensão de cada arquivo.

ex: nome_aluno.xlxs, nome_aluno.json

## INTRUÇÕES DO CÓDIGO

a) Substuir as keys referente à conexão com o bucket da Amazon S3;

b) O script é interativo e fará 4 perguntas onde deverão ser respondidas respeitando as letras maiúsculas e acentução pois são case-sensitve:

    - Seu estado (UF)

    - Seu município

    - Gênero (Homem/Mulher)

    - Idade

c) Altere o nome dos arquivos que serão gerados ao final do código