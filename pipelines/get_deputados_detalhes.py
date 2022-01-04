#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Sep 25 12:49:53 2021

@author: andre
"""

from src import project_utils as pu 
import pandas as pd 

con = pu.get_conection()

query = 'select distinct id from deputados.deputados' 

df = pd.read_sql(sql= query, con= con)
df_list = []

for id in df['id']:
    uri = 'https://dadosabertos.camara.leg.br/api/v2/deputados/{}'.format(id)
    answer = pu.get_request(uri)
    answer = answer['dados']
    df_list.append(answer)

df_input = pd.DataFrame.from_dict(df_list)

df_input['ultimoStatus_nome']=df_input['ultimoStatus'].apply(lambda x: x.get('nome'))
df_input['ultimoStatus_siglaPartido']=df_input['ultimoStatus'].apply(lambda x: x.get('siglaPartido'))
df_input['ultimoStatus_uriPartido']=df_input['ultimoStatus'].apply(lambda x: x.get('uriPartido'))
df_input['ultimoStatus_urlFoto']=df_input['ultimoStatus'].apply(lambda x: x.get('urlFoto'))
df_input['ultimoStatus_nomeEleitoral']=df_input['ultimoStatus'].apply(lambda x: x.get('nomeEleitoral'))
df_input['ultimoStatus_email']=df_input['ultimoStatus'].apply(lambda x: x.get('email'))
df_input['ultimoStatus_siglaUf']=df_input['ultimoStatus'].apply(lambda x: x.get('siglaUf'))
df_input['ultimoStatus_siglaPartido']=df_input['ultimoStatus'].apply(lambda x: x.get('siglaPartido'))
df_input['ultimoStatus_uriPartido']=df_input['ultimoStatus'].apply(lambda x: x.get('uriPartido'))

df_input['ultimoStatus_gabinete']=df_input['ultimoStatus'].apply(lambda x: x.get('gabinete'))
df_input['ultimoStatus_gabinete_nome']=df_input['ultimoStatus_gabinete'].apply(lambda x: x.get('nome'))
df_input['ultimoStatus_gabinete_predio']=df_input['ultimoStatus_gabinete'].apply(lambda x: x.get('predio'))
df_input['ultimoStatus_gabinete_sala']=df_input['ultimoStatus_gabinete'].apply(lambda x: x.get('sala'))
df_input['ultimoStatus_gabinete_andar']=df_input['ultimoStatus_gabinete'].apply(lambda x: x.get('andar'))
df_input['ultimoStatus_gabinete_telefone']=df_input['ultimoStatus_gabinete'].apply(lambda x: x.get('telefone'))
df_input['ultimoStatus_gabinete_email']=df_input['ultimoStatus_gabinete'].apply(lambda x: x.get('email'))

df_input.drop('ultimoStatus', axis='columns', inplace=True)
df_input.drop('ultimoStatus_gabinete', axis='columns', inplace=True)






df_input.to_sql(name='deputados_detalhes',schema='deputados', con=con, if_exists = 'replace')