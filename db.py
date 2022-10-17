
import os
import pyodbc
import inspect
import warnings
import pandas.io.sql as sql



def connect(alias):
    try:
        conn = None
        conn = pyodbc.connect(DRIVER="/usr/local/lib/libtdsodbc.so", 
                            TDS_Version="7.3", 
                            server={
                                    "rainier": os.environ.get("DB_SERVER", ""), 
                                    "rossby": os.environ.get("DB_SERVER_MARIANA", ""), 
                                    "mariana": os.environ.get("DB_SERVER_ROSSBY", "")
                                    }[alias], 
                        port=os.environ.get("DB_PORT", ""), 
                            DATABASE="Opedia", 
                            Uid=os.environ.get("DB_READ_ONLY_USER", ""), 
                            Pwd=os.environ.get("DB_READ_ONLY_PASSWORD", "") 
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
            message = "success!"
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








