#--------------------------------------------------------------------------
'''
vigitel.py: - criacao de banco de dados local (sqlite)
            - importacao de dados do Vigitel
            - inicialização de variaveis
autor: Manuel Antonio
linguagem: Python
versao: maio/2020
'''
#--------------------------------------------------------------------------
'''
Tabela VIGITEL de DB_VIGITEL.db (SQLite) 
-----------------------------------------------------------------
VIGITEL            TIPO      ORIGEM        DESCRICAO 
-----------------------------------------------------------------
ID                 INTEGER   ------        primary key
ANO                INTEGER   ano           ano
CIDADE             INTEGER   cidade        cidade
IDADE              INTEGER   q6            idade (anos)
SEXO               INTEGER   q7            sexo
CIVIL              INTEGER   civil         estado conjugal atual
ESTUDO_ANOS        INTEGER   q8_anos       anos de estudo
PESO               REAL      q9            peso (kg)
ALTURA             INTEGER   q11           altura (cm)
EXERCICIO_FISICO   INTEGER   q42           exercício físico
EXERCICIO_FREQ     INTEGER   q45           freq exercicio
FUMANTE            INTEGER   q60           fumante
COR                INTEGER   q69           cor
PRESSAO_ALTA       INTEGER   q75           pressão alta
DIABETES           INTEGER   q76 ou q76a   diabetes
IMC                REAL      imc           imc
'''
#--------------------------------------------------------------------------

# Modulos
import os
import numpy as np
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

#--------------------------------------------------------------------------

# variaveis

# Arquivo local SQLite com dados importados
db_vigitel = 'DB_VIGITEL.db'

# tuplas com nomes de arquivos e campos a serem importados
tp_arquivos = (\
"Vigitel-2009-peso-rake.xls", "Vigitel-2010-peso-rake.xls", \
"Vigitel-2011-peso-rake.xls", "Vigitel-2012-peso-rake.xls", \
"Vigitel-2013-peso-rake.xls", "Vigitel-2014-peso-rake.xls", \
"Vigitel-2015-peso-rake.xls", "Vigitel-2016-peso-rake.xls", \
"Vigitel-2017-peso-rake.xls", "Vigitel-2018-peso-rake.xls", \
"Vigitel-2019-peso-rake.xls")
tp_campos = (\
'ano','cidade','q6','q7','civil','q8_anos','q9','q11',\
'q42','q45','q60','q69','q75','q76','imc')
tp_campos_2014 = (\
'ano','cidade','q6','q7','civil','q8_anos','q9','q11',\
'q42','q45','q60','q69','q75','q76a','imc')

# instrucoes sql
# estes campos precisam ser criados na mesma ordem que tp_campos, 
# com excecao do campo ID
sql_create = 'create table VIGITEL ('\
'ID               integer primary key autoincrement not null, '\
'ANO              integer, '\
'CIDADE           integer, '\
'IDADE            integer, '\
'SEXO             integer, '\
'CIVIL            integer, '\
'ESTUDO_ANOS      integer, '\
'PESO             real, '\
'ALTURA           integer, '\
'EXERCICIO_FISICO integer, '\
'EXERCICIO_FREQ   integer, '\
'FUMANTE          integer, '\
'COR              integer, '\
'PRESSAO_ALTA     integer, '\
'DIABETES         integer, '\
'IMC              real)'

# insercao de dados na mesma ordem que tp_campos
sql_insert = 'insert into VIGITEL (ANO,CIDADE,IDADE,SEXO,CIVIL,\
ESTUDO_ANOS,PESO,ALTURA,EXERCICIO_FISICO,EXERCICIO_FREQ,FUMANTE,\
COR,PRESSAO_ALTA,DIABETES,IMC) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

# dicionarios de variaveis
dic_cidade = {1:"aracaju",2:"belem",3:"belo horizonte",4:"boa vista",\
              5:"campo grande",6:"cuiaba",7:"curitiba",8:"florianopolis",\
              9:"fortaleza",10:"goiania",11:"joao pessoa",12:"macapa",\
              13:"maceio",14:"manaus",15:"natal",16:"palmas",\
              17:"porto alegre",18:"porto velho",19:"recife",\
              20:"rio branco",21:"rio de janeiro",22:"salvador",\
              23:"sao luis",24:"sao paulo",25:"teresina",\
              26:"vitoria",27:"distrito federal"}
dic_sexo = {1:"masculino",2:"feminino"}
dic_civil = {1:"solteiro",2:"casado legalmente",\
             3:"tem união estável há mais de seis meses",\
             4:"viúvo",5:"separado ou divorciado",888:"não quis informar"}
dic_peso = {777:"não sabe",888:"não quis informar"}
dic_altura = {777:"não sabe",888:"não quis informar"}
dic_exercicio_fisico = {1:"sim",2:"não"}
dic_exercicio_freq = {1:"1 a 2 dias por semana",2:"3 a 4 dias por semana",\
                      3:"5 a 6 dias por semana",\
                      4:"todos os dias ( inclusive sábado e domingo)"}
dic_fumante = {1:"sim, diariamente",2:"sim, mas não diariamente",3:"não"}
dic_cor = {1:"branca",2:"preta",3:"amarela",4:"parda",5:"indígena",\
           777:"não sabe",888:"não quis informar"}
dic_pressao_alta = {1:"sim",2:"não",777:"não lembra"}
dic_diabetes = {1:"sim",2:"não",777:"não lembra"}

#--------------------------------------------------------------------------

# Funcao para importacao dados do Vigitel para um arquivo SQLite local
def importa_vigitel(origem='http://svs.aids.gov.br/download/Vigitel/', \
                    destino=db_vigitel):
    # a origem defaut é a internet
    # Remove o arquivo com o banco de dados SQLite (caso exista)
    os.remove(destino) if os.path.exists(destino) else None
    # Cria uma conexão com o banco de dados. 
    # Se o banco de dados não existir, ele é criado neste momento.
    con = sqlite3.connect(destino)
    # Criando um cursor 
    # (Um cursor permite percorrer todos os registros em um conjunto de dados)
    cur = con.cursor()
    # Executando a instrução sql no cursor
    cur.execute(sql_create)
    # Importando dados excel para um bd sqlite usando Dataframe
    for arquivo in tp_arquivos:
        # informando o arquivo a ser lido
        print('Importando '+arquivo)
        # no teste de importacao de dados foi verificado que um campo ('q76a')
        # possui nome diferente num arquivo ("Vigitel-2014-peso-rake.xls") 
        if arquivo!="Vigitel-2014-peso-rake.xls":
            excelDf = pd.read_excel(origem+arquivo, usecols=tp_campos)
        else:
            excelDf = pd.read_excel(origem+arquivo, usecols=tp_campos_2014)
        # Inserindo os registros
        for rec in excelDf.values:
            cur.execute(sql_insert, rec)
        # Grava a transação
        con.commit()
    print('*** Fim da importacao ***')
    # Fecha a conexão
    con.close()

#--------------------------------------------------------------------------

# reliza consulta ao banco  de dados local
def consulta (sql_select):
    # Criando uma conexão e cursor
    con = sqlite3.connect(db_vigitel)
    c = con.cursor()
    # Realizando a consulta
    c.execute(sql_select)
    dados = c.fetchall()
    # Fecha a conexão
    con.close()
    return dados

#--------------------------------------------------------------------------

