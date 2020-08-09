#!pip install flatten_json
#from flatten_json import flatten
#!pip install json-flatten

import warnings
warnings.filterwarnings("ignore")

#Import de Bibliotecas

import pandas as pd

import time

#Criação da Função Customizada flatten_json
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

#Leitura do Arquivo CSV
start = time.time()
dados_flix = pd.read_csv('LOAD00000001.csv')
end = time.time()
print("Tempo de Execução: {:.2f} min".format((end - start)/60))    

start = time.time()
dados_flix = pd.read_csv('LOAD00000002.csv')
end = time.time()
print("Tempo de Execução: {:.2f} min".format((end - start)/60))

teste = dados_flix['object']

teste.describe()

linha = teste.loc[32]

import json
json_acceptable_string = linha.replace("'", "\"")
d = json.loads(json_acceptable_string)
#d = json.loads(linha)

#Opção 01
from pandas.io.json import json_normalize
linha_normalize = json_normalize(d)

#Opção 02
flat = flatten_json(d)
linha_normalize2 = json_normalize(flat)

#Script Automatizado

import json
from pandas.io.json import json_normalize

df = pd.DataFrame()

#Leitura do Arquivo e Flatten Json
start = time.time()
contador = 0
while contador < len(dados_flix):
    print(contador)
    linha = dados_flix.loc[contador]
    registro = linha['object']
    #json_acceptable_string = registro.replace("'", "\"")
    #variavel = json.loads(json_acceptable_string)
    variavel = json.loads(registro)
    flat = flatten_json(variavel)
    registro_normalize = json_normalize(flat)
    df = df.append(registro_normalize.iloc[0], ignore_index = True)
    contador = contador + 1
end = time.time()
print("Tempo de Execução: {:.2f} min".format((end - start)/60))

df = df.add_prefix('object_')

novo = dados_flix[['id', 'bill_id', 'gateway_id', 'vendor_id', 'customer_id', 'amount', 'subscription_id', 'payment_method_id', 'object', 'installments', 'due_date', 'status', 'created_at']].join(df)


