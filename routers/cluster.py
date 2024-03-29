


from fastapi import APIRouter, status, Response, BackgroundTasks, Request, HTTPException
from typing import Optional, List
from fastapi.responses import FileResponse
import pandas as pd
import inspect


# relative path imports
import sys
sys.path.append("..")
from db import query
from settings import API_VERSION, SUCCESS_MSG, CLUSTER_DESCRIPTION, tags_metadata, ResponseModel as RESMOD, RESPONSE_MODEL_DESCIPTION, EXPORT_DIR
from common import cluster_query, make_random_dir, remove_dir



router = APIRouter(
                   prefix="/cluster",
                   tags=[tags_metadata[3]["name"]]
                   )


@router.get(
            "/", 
            tags=[], 
            summary="cluster checks root",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def cluster_root():
    return {"data": {}, "message":  CLUSTER_DESCRIPTION, "error": False, "version": API_VERSION}
                      

def validate_table_name(table_name):
    try:
        tables, _, _ = cluster_query("show tables")
        tables = [t.lower() for t in tables["tableName"].values]
    except Exception as e:
        print(f"Error in validate_table_name: {str(e)}")
        return False
    return table_name.lower() in tables


@router.get(
            "/tables", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="List table names that are stored on the cluster",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def cluster_tables(response: Response):
    """
    Return a list of table names that are hosted in the big data cluster.
    """
    try:
        data, msg, err = pd.DataFrame({}), "", False
        data, msg, err = cluster_query("show tables")
        data = data[["tableName"]]
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        data = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": data.to_dict(), "message": msg, "error": err, "version": API_VERSION}


@router.get(
            "/temporalRange", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Return temporal range of a table",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def cluster_temporal_range(response: Response, table_name: str):
    """
    Return the time range (`min(time), max(time)`) of a table.
    """
    try:
        data, msg, err = pd.DataFrame({}), "", False
        if not validate_table_name(table_name):
            response.status_code = status.HTTP_404_NOT_FOUND
            return {"data": pd.DataFrame({}), "message": "invalid table name", "error": True, "version": API_VERSION}
        data, msg, err = cluster_query(f"select min(time) min_time, max(time) max_time from {table_name}")
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        data = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": data.to_dict(), "message": msg, "error": err, "version": API_VERSION}



# @router.get(
#             "/query", 
#             tags=[], 
#             status_code=status.HTTP_200_OK,
#             summary="Run a custom ANSI SQL:2003",
#             description="",
#             response_description=RESPONSE_MODEL_DESCIPTION,
#             response_model=RESMOD
#             )
# async def custom_cluster_query(response: Response, query: str):
#     """
#     Run a custom ANSI SQL:2003 on the cluster and return the results.
#     """
#     try:
#         data, msg, err = pd.DataFrame({}), "", False
#         data, msg, err = cluster_query(query)
#         msg = SUCCESS_MSG
#         data = data.fillna("")
#     except Exception as e:
#         response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
#         data = pd.DataFrame({})
#         msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
#         err = True
#         print(msg)        
#     return {"data": data.to_dict(), "message": msg, "error": err, "version": API_VERSION}



@router.get(
            "/query", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Run a custom ANSI SQL:2003",
            description="",
            # response_description=RESPONSE_MODEL_DESCIPTION,
            # response_model=RESMOD
            )
async def custom_cluster_query(background_tasks: BackgroundTasks, response: Response, request: Request, sql: str):
    """
    Run a custom ANSI SQL:2003 on the cluster and return the results in form of a parquet file.
    """
    if not request.headers.get("Authorization") in list(query("select Api_Key from tblApi_Keys where User_ID in (408, 4) ", servers=["rainier"])[0]["Api_Key"].values):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")     
    try:
        data, msg, err = cluster_query(sql)
        dataDir = make_random_dir(EXPORT_DIR)
        fn = f"{dataDir}/data.parquet"
        data.to_parquet(fn, index=False) 
        msg = SUCCESS_MSG
        background_tasks.add_task(remove_dir, dataDir)
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        print(msg)  
        return   
    return FileResponse(fn) 


