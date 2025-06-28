# -*- coding: utf-8 -*-
import os
import shutil
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import traceback

from pyspark.sql.functions import sum, count, round
from pyspark.sql.functions import col, when

#*********************************************************************//
# Note: Please refer to problem statements from challenge.htm file    //
# found under instructions folder for detailed requirements. We have  //
# defined placeholder functions where you need to add your code that  //
# may solve one or more of the the problem statements. Within the     // 
# functions we have added DEFAULT code which when executed without    //
# any modification would create empty dataframe. This approach will   // 
# enable you to test your code intermediate without worrying about    // 
# syntax failure outside of your code. To SOLVE the problem, you are  //
# expected to REPLACE the code which creates dummy dataframe with     //
# your ACTUAL code.                                                   //
#*********************************************************************//
# Note: We have also added code to print sample data to console in    //
# each of the functions to help you visualize your intermediate data  //
#*********************************************************************//


def read_data(spark, customSchema):
    ''' 
    spark_session : spark 
    customSchema : we have given the custom schema
    '''
    print("-------------------")
    print("Starting read_data")
    print("-------------------")

    #Mention the Bucket name inside the bucket_name variable
    bucket_name = " "
    s3_input_path = "s3://" + bucket_name + "/inputfile/loan_data.csv"
    
    df = spark.read.csv(s3_input_path, header=True, schema=customSchema)
    

    return df

def clean_data(input_df):
    '''
    for input file: input_df is output of read_data function
    '''
    print("-------------------")
    print("Starting clean_data")
    print("-------------------")

    df = input_df.dropna().dropDuplicates()
    df = df.filter(df.purpose != 'null')  
    
    return df



def s3_load_data(data,file_name):
    '''
    data  : the output data of result_1 and result_2 function
    file_name : the name of the output to be stored inside the s3
    '''
    #Mention the bucket name inside the bucket_name variable
    bucket_name = " "
    output_path = "s3://" + bucket_name + "/output"+ file_name
    
    if data.count() != 0:
        print("Loading the data", output_path)
        data.coalesce(1).write.csv(output_path, header=True, mode="overwrite")
        
    else:
        print("Empty dataframe, hence cannot save the data", output_path)



def result_1(input_df):
    '''
    for input file: input_df is output of clean_data function
    '''
    print("-------------------------")
    print("Starting result_1")
    print("-------------------------")

    df = input_df.filter((col("purpose") == "educational") | (col("purpose") == "small_business"))
    df = df.withColumn("income_to_installment_ratio", col("log_annual_inc") / col("installment"))
    df = df.withColumn("int_rate_category",when(col("int_rate") < 0.1, "low").when((col("int_rate") >= 0.1) & (col("int_rate") < 0.15), "medium").otherwise("high"))
    df = df.withColumn("high_risk_borrower",when((col("dti") > 20) | (col("fico") < 700) | (col("revol_util") > 80), 1).otherwise(0))   
    
    return df


def result_2(input_df):
    '''
    for input file: input_df is output of clean_data function
    '''
    print("-------------------------")
    print("Starting result_2")
    print("-------------------------")

    df = input_df.groupBy("purpose").agg((sum(col("not_fully_paid"))/count("*")).alias("default_rate"))
    df = df.withColumn("default_rate", round(col("default_rate"), 2)) 
    

    return df

def redshift_load_data(data):


    if data.count() != 0:
        print("Loading the data into Redshift...")
        jdbcUrl = " " #Mention the redshift jdbc url
        username = " " #Mention redshift username
        password = " " #Mention  redshift password
        table_name = " " #Mention redshift table name
        
        
        #Write the redshift load data command here
        data.write.format("jdbc").option("url", jdbcUrl).option("dbtable", table_name).option("user", username).option("password", password).mode("overwrite").save()
        
        
    else:
        print("Empty dataframe, hence cannot load the data")

    

def main():
    """ Main driver program to control the flow of execution.
        Please DO NOT change anything here.
    """

    spark = (SparkSession.builder.appName("loan Data analysis").getOrCreate())    
    spark.sparkContext.setLogLevel("ERROR")
    customSchema = StructType([ 
        StructField("sl_no", IntegerType(), nullable=True),
        StructField("credit_policy", IntegerType(), nullable=True),
        StructField("purpose", StringType(), nullable=True),
        StructField("int_rate", DoubleType(), nullable=True),
        StructField("installment", DoubleType(), nullable=True),
        StructField("log_annual_inc", DoubleType(), nullable=True),
        StructField("dti", DoubleType(), nullable=True),
        StructField("fico", IntegerType(), nullable=True),
        StructField("days_with_cr_line", DoubleType(), nullable=True),
        StructField("revol_bal", IntegerType(), nullable=True),
        StructField("revol_util", DoubleType(), nullable=True),
        StructField("inq_last_6mths", IntegerType(), nullable=True),
        StructField("delinq_2yrs", IntegerType(), nullable=True),
        StructField("pub_rec", IntegerType(), nullable=True),
        StructField("not_fully_paid", IntegerType(), nullable=True)
    ])
    clean_data_path = "/cleaned_data"
    result_1_path = "/result_1"
    
    
    try:
        task_1 = read_data(spark, customSchema)
    except Exception as e:
        print("Getting error in the read_data function", e)
        traceback.print_exc()
    try:
        task_2 = clean_data(task_1)
    except Exception as e:
        print("Getting error in the clean_data function", e)
        traceback.print_exc()
    try:
        task_3 = result_1(task_2)
    except Exception as e:
        print("Getting error in the result_1 function", e)
        traceback.print_exc()
    try:
        task_4 = result_2(task_2)      
    except Exception as e:  
        print("Getting error in the result_2 function", e)
        traceback.print_exc()

    try:
        redshift_load_data(task_4)
    except Exception as e:
        print("Getting error while loading redshift data", e)
        traceback.print_exc()
    try:
        s3_load_data(task_2, clean_data_path)
    except Exception as e:
        print("Getting error while loading result_2", e)
        traceback.print_exc()
    try:
        s3_load_data(task_3, result_1_path)
    except Exception as e:
        print("Getting error while loading result_3", e)
        traceback.print_exc()

    spark.stop()


if __name__ == "__main__":
    main()
