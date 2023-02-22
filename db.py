
import os, platform
import pyodbc
import inspect
import warnings
import pandas.io.sql as sql
from databricks import sql
import pandas as pd


def connect(alias):
    try:
        conn = None
        if platform.system().lower().find("darwin") != -1:
            driver = "/usr/local/lib/libtdsodbc.so"
        elif platform.system().lower().find("linux") != -1:     
            driver = "/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so"        
        conn = pyodbc.connect(DRIVER=driver, 
                            TDS_Version="7.3", 
                            SERVER={
                                    "rainier": os.environ.get("DB_SERVER_RAINIER", ""), 
                                    "mariana": os.environ.get("DB_SERVER_MARIANA", ""), 
                                    "rossby": os.environ.get("DB_SERVER_ROSSBY", "")
                                    }[alias], 
                            PORT=os.environ.get("DB_PORT", ""), 
                            DATABASE="Opedia", 
                            Uid=os.environ.get("DB_REST_ADMIN", ""), 
                            Pwd=os.environ.get("DB_REST_ADMIN_PASSWORD", "") 
                            )
    except Exception as e:
        print(f"Exception in connect: {str(e)}")    
    return conn



def query(statement, servers=["rainier"]):
    try:
        data = {}
        message = ""
        err = False
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            conn = connect(servers[0])    
            data = sql.read_sql(statement, conn)
            message = "success"
    except Exception as e:
        err = True
        message = f"{inspect.stack()[0][3]}:: {str(e).strip()}"
        if message.find("[SQL Server]") != -1: message = message.split("[SQL Server]")[1]                 
    finally:
        try:
            conn.close()      
        except Exception as e:
            err = True
            message += f" {str(e).strip()}"    
    return data, message, err



def db_execute(query, servers):
    try:
        conn = connect(servers[0]) 
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
    except Exception as e:    
        print(f"error in db_execute: {str(e)}")
    return


def cluster_query(query):
    """
    Run a sql query on cluster and return the results in form of a pandas dataframe.
    """
    try:
        data, message, err = pd.DataFrame({}), "", False
        conn = sql.connect(
                                server_hostname = os.environ["CLUSTER_HOST"],
                                http_path = os.environ["CLUSTER_WAREHOUSE_PATH"],
                                access_token = os.environ["CLUSTER_WAREHOUSE_TOKEN_PYTHON"])

        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cols = [column[0] for column in cursor.description]
        data = pd.DataFrame.from_records(rows, columns=cols) 
        message = "success"   
    except Exception as e:
        err = True
        message = f"{inspect.stack()[0][3]}:: {str(e).strip()}"            
    finally:
        try:
            cursor.close()
            conn.close() 
        except Exception as e:
            err = True
            message += f" {str(e).strip()}"    
    return data, message, err



