import json
from datetime import datetime, timedelta
import os 
import time
import cx_Oracle

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import *

import pandas as pd
import numpy as np
from builtins import min

OUT_DATA_NAME = "outdata.csv"

def init_spark():
    sparkProps = {
    "spark.master": "yarn",
    "spark.driver.cores": 2,
    "spark.driver.memory": "8g",
	"spark.num.executors": "2", 
    "spark.executor.cores": 2,
    "spark.executor.memory": "16g",
    "spark.port.maxRetries":"100",
    "spark.dynamicAllocation.enabled": True, # Set to False if Dynamic Allocation is not required
    #"spark.dynamicAllocation.initialExecutors": 1, # Remove it if Dynamic Allocation is not required
    "spark.dynamicAllocation.maxExecutors": 8 # Remove if Dynamic Allocation is not required
    #"spark.dynamicAllocation.minExecutors": 1 # Remove if Dynamic Allocation is not required
    }
    conf = SparkConf().setAll(list(sparkProps.items()))
    spark = (
        SparkSession.builder
                    .appName("duysession5")
                    .config(conf=conf)
                    .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_ora_conn():
    # The file contains your Oracle Account
    with open(os.path.join(os.getcwd(), "config.json"), "r") as f:
            config = json.load(f)


    email = config["email"]
    ora_conn = config["ora_conn"] 
    ora_user = config["ora_user"] 
    ora_password = config["ora_password"]
    host = config["host"] 
    port = config["port"] 
    service_name = config["service_name"] 

    spark = init_spark()

    # Oracle Session
    dsn_tns = cx_Oracle.makedsn(host, port, service_name= service_name)
    conn = cx_Oracle.connect(user=ora_user, password=ora_password, dsn=dsn_tns)
    cursor = conn.cursor()
    
    return conn

if __name__ == "__main__":
    conn = get_ora_conn()
    
    df = pd.read_sql("""
        select *
        from ap_crm.test_customer_flow_forward_052021   
    """, con = conn)
    
    df.to_csv(os.path.join(os.getcwd(), OUT_DATA_NAME))