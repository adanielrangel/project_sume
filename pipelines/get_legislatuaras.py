#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug  7 12:48:35 2021

@author: andre
"""

import sys
sys.path.insert(0, '/home/andre/project_sume/utils')

import project_utils as pu 
import pandas as pd

legislaturas_id = list(range(1,57))
df_List = []
engine = pu.get_conection()

for id_ in legislaturas_id :
    answer = pu.get_request('https://dadosabertos.camara.leg.br/api/v2/legislaturas?id={}&ordem=DESC&ordenarPor=id'.format(id_))
    answer = answer['dados'][0]
    df_List.append(answer)
    
df = pd.DataFrame.from_dict(df_List)

df.to_sql(name='legislaturas',schema='deputados', con=engine, if_exists = 'replace')


