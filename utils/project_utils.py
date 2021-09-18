#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 21:34:50 2021

@author: andre
"""

import requests
import pandas as pd
from dotenv import load_dotenv
import os
load_dotenv()
from sqlalchemy import create_engine

def get_request ( url: str ):
    response =requests.request("GET", url= url)
    response = response.json()
    return response 

def response_to_df (response):
    df = pd.DataFrame.from_dict(response['dados'])
    return df

def get_conection():
    host=os.environ.get("host")
    port=os.environ.get("port")
    username=os.environ.get("username")
    password=os.environ.get("password")
    
    engine = create_engine('postgresql://{}:{}@{}:{}/postgres'.format(username,password,host,port))
    return engine
    



    