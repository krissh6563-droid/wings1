# -*- coding: utf-8 -*-
import os
import shutil
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import traceback


def read_data(spark, customSchema):

    #Mention the Bucket name inside the bucket_name variable
    bucket_name = "loan-data1234"  # Replace with your bucket name
    s3_input_path = "s3://" + bucket_name + "/inputfile/loan_data.csv"

    df = spark.read.csv(s3_input_path, header=True, schema=customSchema)

    return df

def clean_data(input_df):

    df = input_df.dropna().dropDuplicates()
    df = df.filter(df.purpose != 'null')

     
        
    return df



def s3_load_data(data,file_name):

    #Mention the bucket name inside the bucket_name variable
    bucket_name = "loan-data1234"
    output_path = "s3://" + bucket_name + "/output"+ file_name
    
    if data.count() != 0:
        print("Loading the data", output_path)
        #write the s3 load data command here
        data.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
        
    else:
        print("Empty dataframe, hence cannot save the data", output_path)



def result_1(input_df):

    df = input_df.filter((col("purpose") == "educational") | (col("purpose") == "small_business"))
    df = df.withColumn("income_to_installment_ratio", col("log_annual_inc") / col("installment"))
    df = df.withColumn("int_rate_category", 
        when(col("int_rate") < 0.1, "low")
        .when((col("int_rate") >= 0.1) & (col("int_rate") < 0.15), "medium")
        .otherwise("high")
    )
    df = df.withColumn("high_risk_borrower", 
        when((col("dti") > 20) | (col("fico") < 700) | (col("revol_util") > 80), 1)
        .otherwise(0)
    )  
    

    
    
    return df


def result_2(input_df):

    df = input_df.groupBy("purpose").agg(
        (sum(col("not_fully_paid")) / count("*")).alias("default_rate")
    )
    df = df.withColumn("default_rate", round(col("default_rate"), 2))
    

    return df

def redshift_load_data(data):


    if data.count() != 0:
        print("Loading the data into Redshift...")
        jdbcUrl = "your-jdbc-url"  # Replace with your Redshift JDBC URL
        username = "awsuser"
        password = "Awsuser1"
        table_name = "result_2"
        
        
        #Write the redshift load data command here
        data.write \
        .format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .mode("overwrite") \
        .save()
        
        
    else:
        print("Empty dataframe, hence cannot load the data")

  