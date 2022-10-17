
from distutils.log import error
from fastapi import APIRouter
import pandas as pd
import inspect

# relative path imports
import sys
sys.path.append("..")
from db import query





router = APIRouter(
                   prefix="/db",
                   tags=["Ingested Data"]
                   )



@router.get(
            "/", 
            tags=[], 
            summary="Database checks root",
            description="",
            response_description=""
            )
async def db_checks():
    return "Database checks root"                   



@router.get(
            "/strandedtables", 
            tags=[], 
            summary="Search for stranded tables in catalog",
            description="",
            response_description=""
            )
async def stranded_tables():
    """
    Return a list of table names that are mentioned in the catalog but they don't exist in the database.
    """
    try:
        strandedTablesDF, msg, err = pd.DataFrame({}), "", False
        catalogTables = query("select distinct Table_Name from dbo.udfCatalog()", servers=["rainier"])[0]["Table_Name"].values
        rainierTables = query("select * from information_schema.tables", servers=["rainier"])[0]["TABLE_NAME"].values
        rossbyTables = query("select * from information_schema.tables", servers=["rossby"])[0]["TABLE_NAME"].values
        marianaTables = query("select * from information_schema.tables", servers=["mariana"])[0]["TABLE_NAME"].values
        strandedTables = []
        for t in catalogTables:
            if (not t in rainierTables) and (not t in rossbyTables) and (not t in marianaTables):
                strandedTables.append(t)
        strandedTablesDF = pd.DataFrame({"Table": strandedTables})
        msg = "success"
    except Exception as e:
        strandedTablesDF = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": strandedTablesDF.to_dict(), "message": msg, "error": err}



