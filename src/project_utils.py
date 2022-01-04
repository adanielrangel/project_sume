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
import pyspark.sql.functions as f

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

def test():
    print('jacaguey')
    

# spark con

def spark_write(df,schema,table,mode):
    #modes = 
    #append: Append contents of this DataFrame to existing data.
    #overwrite: Overwrite existing data.
    #ignore: Silently ignore this operation if data already exists.
    #error or errorifexists (default case): Throw an exception if data already exists.
    host=os.environ.get("host")
    port=os.environ.get("port")
    username=os.environ.get("username")
    password=os.environ.get("password")
    df.write.format("jdbc").option("url", "jdbc:postgresql://{}:{}/postgres".format(host,port)).option("dbtable", "{}.{}".format(schema,table)).option("user", "{}".format(username)).option("password", "{}".format(password)).option("driver", "org.postgresql.Driver").mode('{}'.format(mode)).save()

def spark_read(schema,table):
    from pyspark.sql import SparkSession
    host=os.environ.get("host")
    port=os.environ.get("port")
    username=os.environ.get("username")
    password=os.environ.get("password")
    spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.jars", "/home/andre/spark/jars/postgresql-42.3.1.jar").getOrCreate()
    df = spark.read.format("jdbc").option("url", "jdbc:postgresql://{}:{}/postgres".format(host,port)).option("dbtable", "{}.{}".format(schema,table)).option("user", "{}".format(username)).option("password", "{}".format(password)).option("driver", "org.postgresql.Driver").load()
    return df 

def flatten_df_struct_columns_to_root(nested_df):
    """
    Extracts nested struct columns to the root level of a dataframe.
    Args:
        nested_df: Dataframe with `n` levels of StructType nesting
    Returns:
        Dataframe where nested columns are extracted to the root level
    """
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            f.col(".".join(parents + (column_type[0],))).alias("_".join(parents + (column_type[0],)))
            for column_type in df.dtypes
            if column_type[1][:6] != "struct"
        ]

        nested_cols = [
            column_type[0] for column_type in df.dtypes if column_type[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)

    