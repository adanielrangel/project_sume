#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 21 12:22:11 2021

@author: andre
"""
# import das bibliotecas 
from src import project_utils as pu 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField
from pyspark.sql import functions as f 

# inicialisando spark
spark = SparkSession.builder.appName("pipe_discurssos").config("spark.jars", "/home/andre/spark/jars/postgresql-42.3.1.jar").getOrCreate()

# url de request para essa tabela 
url = 'https://dadosabertos.camara.leg.br/api/v2/deputados/{}/discursos?dataInicio={}&dataFim={}'

# schemas para transformar os jsons recebidos em spark 
schema = StructType([
        StructField('dataHoraInicio', StringType(), True),
        StructField('dataHoraFim', StringType(), True),
        StructField('uriEvento', StringType(), True),
        StructField('faseEvento', StringType(), True),
        StructField('tipoDiscurso', StringType(), True),
        StructField('urlTexto', StringType(), True),
        StructField('urlAudio', StringType(), True),
        StructField('urlVideo', StringType(), True),
        StructField('keywords', StringType(), True),
        StructField('sumario', StringType(), True),
        StructField('transcricao', StringType(), True)
    ])



subschema = StructType([
        StructField('titulo', StringType(), True),
        StructField('dataHoraInicio', StringType(), True),
        StructField('dataHoraFim', StringType(), True)
    ])


# buscando no banco os id,data inicio das legilaturas e data fim das legislaturas 
df_input = pu.spark_read("silver","deputados")

df_input = df_input.select("id","dt_inicio_legslatuara","dt_fim_legslatuara").distinct().filter(f.col("dt_inicio_legslatuara")=='2019-02-01').collect()

for i in df_input:
    #inicializando variaveis, vamos subir 1 deputado em uma legislatura por vez 
    # se não o volume de dados esplode a minha memoria disponivel 
    id_deputado = i[0]
    dt_inicio = i[1]
    dt_fim = i[2]
    request = pu.get_request(url.format(id_deputado,dt_inicio,dt_fim))
    
    # aqui recebemos os json do request e tratamos eles 
    df = spark.createDataFrame(request['dados'],schema)
    # tem um json interno, vamos jogar ele para a raiz 
    df = df.withColumn("faseEvento",f.from_json(df.tipoDiscurso,subschema))
    df = pu.flatten_df_struct_columns_to_root(df)
    df = df.withColumnRenamed("faseEvento_titulo", "faseevento_titulo").withColumnRenamed("faseEvento_dataHoraInicio", "faseevento_dataHoraInicio").withColumnRenamed("faseEvento_dataHoraFim", "faseevento_dataHoraFim")
    #ultimas converçoes de tipo 
    df = df.selectExpr("cast(dataHoraInicio as timestamp) as dataHoraInicio", "cast(dataHoraFim as timestamp) as dataHoraFim" ,"uriEvento" ,"tipoDiscurso","urlTexto" ,"urlAudio" ,"urlVideo" ,"keywords" ,"transcricao","faseevento_titulo","cast(faseevento_dataHoraInicio as timestamp) as faseevento_dataHoraInicio","cast(faseevento_dataHoraFim as timestamp) as faseevento_dataHoraFim")
    #prciso adicionar o id do deputado no df 
    df = df.withColumn("id_deputado",f.lit(id_deputado))
    # subindo para o banco 
    pu.spark_write(df, schema="deputados", table="discurssos", mode="append")
