#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 22:15:30 2021

@author: andre
"""

import project_utils as pu 



legislaturas_id = list(range(1,56))

engine = pu.get_conection()

for id_ in legislaturas_id :
    response = pu.get_request('https://dadosabertos.camara.leg.br/api/v2/partidos?idLegislatura={}&ordem=ASC&ordenarPor=sigla'.format(id_))
    df = pu.response_to_df(response)
    df.to_sql(name='partidos',schema='deputados', con=engine, if_exists = 'append')