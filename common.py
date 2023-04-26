import os
from settings import EXPORT_DIR, UPLOAD_DIR, UPLOAD_EXCEL_DIR
from db import query, db_execute
from user_agents import parse
from datetime import datetime
import re
from databricks import sql
import pandas as pd
import inspect, random, string, shutil


def make_dir(directory):
    """
    Creates a new directory if doesn't exist.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return


def project_init():
    """
    Steps at project stratup.
    """
    make_dir(EXPORT_DIR)
    make_dir(UPLOAD_DIR)
    make_dir(UPLOAD_EXCEL_DIR)
    return
    

    
def make_random_dir(baseDir):
    """
    Create a directory with a randomly generated name.
    """
    randDir = f"{baseDir}{''.join(random.choices(string.ascii_letters, k=16))}/" 
    make_dir(randDir)
    return randDir  

async def remove_dir(dirPath: str):
    """
    Remove a directory and its entire contents (recursively).
    """
    if os.path.isdir(dirPath): 
        shutil.rmtree(dirPath)      
    return


def store_call(req, ua_string):
    ua = parse(ua_string)

    now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    host = req.headers["host"]
    ip = req.client.host
    if req.headers.get("x-forwarded-for"):
        ip = req.headers.get("x-forwarded-for").split(",")[0]
    elif req.headers.get("X-Forwarded-For"):
        ip = req.headers.get("X-Forwarded-For").split(",")[0]

    # print(dict(req))
    # print("method: ", req.method)  # >>>> to save rest metod
    
    # url_list = [
    #     {"path": route.path, "name": route.name} for route in req.app.routes
    # ]
    # print(url_list)


    if ua.browser.family == "ELB-HealthChecker": return
    if not re.compile(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$").match(ip): return


    command = f"""INSERT INTO tblValidation_API_Calls (
            Path, IP, HOST, Browser, Browser_Version, OS, OS_Version, 
            Device, Device_Brand, Device_Model, Mobile, Tablet, Touch, PC, Bot, Date_Time ) 
            VALUES (
                '{req.url.path}', '{ip}', '{host}', '{ua.browser.family}', '{ua.browser.version_string}', '{ua.os.family}', '{ua.os.version_string}',
                '{ua.device.family}', '{ua.device.brand}', '{ua.device.model}', 
                {int(ua.is_mobile)}, {int(ua.is_tablet)}, {int(ua.is_touch_capable)}, {int(ua.is_pc)}, {int(ua.is_bot)}, '{now}'
                )"""          
    db_execute(command, servers=["rossby"]) 
    return 
            


def get_catalog():
    return query("select * from dbo.udfCatalog()", servers=["rainier"])

def get_datasets():
    return query("select * from tbldatasets order by id desc", servers=["rainier"])

def get_dataset_refs():
    return query("select * from tblDataset_References r join tblDatasets d on r.Dataset_ID=d.id order by r.dataset_id desc", servers=["rainier"])    


def find_cruise(name):
    cruiseDF, _, _ = query(f"select * from tblCruise where [Name]='{name}' or Nickname='{name}'", servers=["rainier"]) 
    if len(cruiseDF) == 1: 
        return cruiseDF
    else:
        cdf, _, _ = query(f"select distinct cruise_ID from tblCruise_Keywords where keywords like '%{name}%'", servers=["rainier"])
        if len(cdf) == 1:
            cruiseDF, _, _ = query(f"select * from tblCruise where ID={cdf.cruise_ID.values[0]}", servers=["rainier"])
            return cruiseDF
        else:    
            return cruiseDF   



def get_regions():
    return query(f"select * from tblRegions", servers=["rainier"])    





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
