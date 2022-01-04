#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 15 13:30:22 2021

@author: andre
"""

import pandas as pd 
import sys
sys.path.insert(0, '/home/andre/project_sume/utils')

from src import project_utils as pu 

engine = pu.get_conection()

query = """
select d.id,d.nome,d."siglaPartido",ap.alinhamento_pulitioco as alinhamento_pulitoco,
d."siglaUf",cast(l."dataInicio" as date) as dt_inicio_legslatuara ,
cast(l."dataFim" as date) as dt_fim_legslatuara
from deputados.deputados as d
left join deputados.legislaturas as l 
on d."idLegislatura"=l.id
left join deputados.alinhamento_partidos as ap
on ap."siglaPartido" = d."siglaPartido" """

df =  pd.read_sql(query, engine)

df.to_sql(name='deputados',schema='silver', con=engine)