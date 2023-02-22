

from fastapi import APIRouter, status, Response
from typing import Optional, List
import pandas as pd
import inspect
import language_tool_python

# relative path imports
import sys
sys.path.append("..")
from db import query
from settings import API_VERSION, SUCCESS_MSG, DB_DESCRIPTION, tags_metadata, SERVERS, ResponseModel as RESMOD, RESPONSE_MODEL_DESCIPTION
from common import get_datasets, get_dataset_refs

sys.path.append("../utils") 
from utils.utils_dead_links import dead_links, get_links

import time


router = APIRouter(
                   prefix="/db",
                   tags=[tags_metadata[2]["name"]]
                   )



@router.get(
            "/", 
            tags=[], 
            summary="Database checks root",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def db_checks():
    return {"data": {}, "message":  DB_DESCRIPTION, "error": False, "version": API_VERSION}
                      


@router.get(
            "/strandedTables", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Search for stranded tables in the catalog",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def stranded_tables(response: Response):
    """
    Return a list of table names that are mentioned in the catalog but they don't exist in the database systems.
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
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        strandedTablesDF = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": strandedTablesDF.to_dict(), "message": msg, "error": err, "version": API_VERSION}




def vars_exist(table, vars, servers):
    """
    Check if `vars` exist in `table` on any database server.
    Doesn't check if the table is on the designated server or not (using tbldataset_servers).
    """
    found = False
    try:
        sql = f"select top 5 lat,{vars} from {table}"
        for server in servers:
            df, msg, err = query(sql, servers=[server])
            if err: continue
            if len(df) > 0: found = True
    except:
        return None, None, None
    return found, df, msg        



@router.get(
            "/strandedVariables", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Search for stranded variables in the catalog",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def stranded_variables(response: Response):
    """
    Return a list of variables names that are mentioned in the catalog but they don't exist in their associated table.
    """
    try:
        strandedVarsDF, msg, err = pd.DataFrame({}), "", False
        df, _, _ = query("SELECT Dataset_ID, Dataset_Name, Table_Name, STRING_AGG(CONVERT(NVARCHAR(max),CONCAT('[',Variable, ']')),',' ) Variable FROM dbo.udfCatalog() GROUP BY Table_Name, Dataset_ID, Dataset_Name ORDER by Dataset_ID DESC")
        for index, row in df.iterrows():
            print(f"checking for stranded vars in table ({index+1}/{len(df)}): {row.Table_Name} ...")
            varsExist, _, _ = vars_exist(row["Table_Name"], row["Variable"], SERVERS)
            if not varsExist:
                for v in row["Variable"].split(","):
                    vExist, _, msg = vars_exist(row["Table_Name"], v, SERVERS)
                    if not vExist:
                        rowDF = row.to_frame().T
                        rowDF["Variable"] = v
                        rowDF["Message"] = msg
                        if len(strandedVarsDF ) < 1:
                            strandedVarsDF = rowDF
                        else:
                            strandedVarsDF = pd.concat([strandedVarsDF, rowDF], ignore_index=True)   
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        strandedVarsDF = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": strandedVarsDF.to_dict(), "message": msg, "error": err, "version": API_VERSION}


@router.get(
            "/varsInTableNotCatalog", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Look for table fields that are not present in the catalog",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def vars_in_table_not_catalog(response: Response, table_name: Optional[str]=None):
    """
    Return a list of table fields that are not present in the catalog. Note that the returned fields don't necessarily reflect a bug. 
    If `table_name` is empty all tables will be checked, otherwise only the specified table will be checked. Tables on cloud clusters are not checked.
    """
    try:
        ignore = ["time", "lat", "lon", "depth", "month", "year", "week", "day", "dayofyear", "hour"]
        fieldsNotInCatalogDF, msg, err = pd.DataFrame({}), "", False
        catalogVars, _, _ = query("select Short_Name, Table_Name from tblVariables", servers=["rainier"])
        server_tables = {
                        SERVERS[0]: query("select * from information_schema.tables", servers=["rainier"])[0]["TABLE_NAME"].values,
                        SERVERS[1]: query("select * from information_schema.tables", servers=["rossby"])[0]["TABLE_NAME"].values,
                        SERVERS[2]: query("select * from information_schema.tables", servers=["mariana"])[0]["TABLE_NAME"].values
                        }
        fields_nic, servers_nic, tables_nic = [], [], []  # nic: not in catalog        
        for server in SERVERS:
            if table_name is None: 
                tables = server_tables[server]
            else:
                tables = [table_name]
            for t in tables:
                if t not in catalogVars["Table_Name"].unique(): continue
                fields = query(f"select column_name from information_schema.columns where table_name='{t}'", servers=[server])[0]["column_name"].values
                for field in fields:
                    if field in ignore: continue
                    print(f"Checking: Server: {server}, Table: {t}, Field: {field}")
                    if len(catalogVars.query(f"Short_Name=='{field}' and Table_Name=='{t}'")) < 1:
                        servers_nic.append(server)
                        tables_nic.append(t)
                        fields_nic.append(field)
        if len(servers_nic) > 0:
            fieldsNotInCatalogDF = pd.DataFrame({"Server": servers_nic, "Table": tables_nic, "Field": fields_nic})
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        fieldsNotInCatalogDF = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": fieldsNotInCatalogDF.to_dict(), "message": msg, "error": err, "version": API_VERSION}


@router.get(
            "/numericLeadingVariables", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Find variable names starting with a number",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def numeric_leading_variables(response: Response):
    """
    Return a list of variables names (short names) that start with a number.
    """
    try:
        numericVarsDF, msg, err = pd.DataFrame({}), "", False
        numericVarsDF, _, _ = query("select Short_Name, Table_Name from tblVariables where Short_Name like '[0-9]%'")
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        numericVarsDF = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": numericVarsDF.to_dict(), "message": msg, "error": err, "version": API_VERSION}


@router.get(
            "/duplicateVarLongName", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Look for duplicate variable long names in a dataset",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def duplicate_var_long_name(response: Response):
    """
    Return a list of duplicate variable long names in a table (dataset).
    """
    try:
        duplicates, msg, err = pd.DataFrame({}), "", False
        duplicates = query("select count(Long_Name) repetition, table_name, Long_Name from udfCatalog() GROUP by table_name, Long_Name having count(Long_Name)>1", servers=["rainier"])[0]
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        duplicates = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": duplicates.to_dict(), "message": msg, "error": err, "version": API_VERSION}


@router.get(
            "/duplicateDatasetLongName", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Look for duplicate variable long names in a dataset",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def duplicate_dataset_long_name(response: Response):
    """
    Return a list of duplicate dataset long name.
    """
    try:
        duplicates, msg, err = pd.DataFrame({}), "", False
        duplicates = query("""
                            with cte as(select Dataset_Long_Name from tblDatasets GROUP by Dataset_Long_Name having count(Dataset_Long_Name)>1) 
                            select distinct Dataset_Name, cte.Dataset_Long_Name, Table_Name from cte 
                            join tblDatasets on tblDatasets.Dataset_Long_Name=cte.Dataset_Long_Name 
                            join tblVariables on tblVariables.Dataset_ID=tblDatasets.ID        
                            """, servers=["rainier"])[0]
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        duplicates = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": duplicates.to_dict(), "message": msg, "error": err, "version": API_VERSION}


@router.get(
            "/datasetsWithBlankSpace", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Look for datasets with blank space in their name",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def datasets_with_blank_space(response: Response):
    """
    Return a list of datasets that their name include blank space character.
    """
    try:
        data, msg, err = pd.DataFrame({}), "", False
        data = query("select Dataset_Name [Dataset_Short_Name] from tblDatasets where Dataset_Name like '% %'", servers=["rainier"])[0]
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        data = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": data.to_dict(), "message": msg, "error": err, "version": API_VERSION}


@router.get(
            "/varsWithBlankSpace", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Look for variables with blank space in their name",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def vars_with_blank_space(response: Response):
    """
    Return a list of variables that their name include blank space character.
    """
    try:
        data, msg, err = pd.DataFrame({}), "", False
        data = query("select Table_Name, Short_Name, len(Short_Name) [length] from tblVariables where Short_Name like '% %'", servers=["rainier"])[0]
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        data = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": data.to_dict(), "message": msg, "error": err, "version": API_VERSION}


def language_check(lTool, dataset):
    """
    Detect grammar errors and spelling mistakes.
    """
    try:
        results = pd.DataFrame()
        matches = lTool.check(dataset["Description"])
        for match in matches:
            row = pd.DataFrame([{
                "ruleID": match.ruleId,
                "message": match.message,
                "sentence": str(match.sentence),
                "replacements": str(match.replacements),
                "offsetInContext": str(match.offsetInContext),
                "context": str(match.context),
                "offset": str(match.offset),
                "errorLength": str(match.errorLength),
                "category": str(match.category),
                "ruleIssueType": str(match.ruleIssueType)            
            }])
            results = pd.concat([results, row], ignore_index=True)

        if len(results) > 0:    
            results["Dataset_ID"] = dataset["ID"]
            results["Dataset_Name"] = dataset["Dataset_Name"]
            results["Dataset_Long_Name"] = dataset["Dataset_Long_Name"]    
    except:
        results = pd.DataFrame()
    return results



@router.get(
            "/langDescription", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Spell/Grammar check on dataset descriptions",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def language_check(response: Response, dataset_name: Optional[str]=None):
    """
    Return a list of potential spell/grammar mistakes in dataset description text.\n 
    `dataset_name` is the (short) name of the dataset to be chaecked. 
    `dataset_name` is case-sensitive and must not contain blank space or special characters.
    When `dataset_name` is not provided, the entire list of CMAP dataset descriptions are language-checked.\n \n
    **Note**: This method tends to return a large number of false-positive signals, especially with the technical/scientific/jargon terms.
    """
    try:
        dfCompiled, msg, err = pd.DataFrame({}), "", False
        lTool = language_tool_python.LanguageTool('en-US')
        datasets, _, _ = get_datasets()
        if dataset_name is not None: datasets = datasets.query(f"Dataset_Name=='{dataset_name}'")
        for _, row in datasets.iterrows():
            print(f"checking dataset (ID {row.ID}): {row.Dataset_Name} ... \n---------------------")
            checks = language_check(lTool, row)
            if len(dfCompiled ) < 1:
                dfCompiled = checks
            else:
                dfCompiled = pd.concat([dfCompiled, checks], ignore_index=True)        
        lTool.close()
        # dfCompiled.to_csv("./language_ckecks.csv", index=False)
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        dfCompiled = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": dfCompiled.to_dict(), "message": msg, "error": err, "version": API_VERSION}







@router.get(
            "/deadLinks", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Look for dead links in dataset metadata",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def dead_links_check(response: Response, dataset_name: Optional[str]="Mercator_Pisces_Biogeochem_Climatology"):
    """
    Return a list of dead links in dataset description, data_source, distributor and references. 
    Links that resolve in status codes >= 400 and <503 are considered 'dead links'. Currently, I cannot find a definitive solution to identify 
    redirected links that do not return the intended page (example: removed CMEMS datasets).\n 
    `dataset_name` is the (short) name of the dataset to be chaecked. 
    `dataset_name` is case-sensitive and must not contain blank space or special characters.
    When `dataset_name` is not provided, the entire list of CMAP datasets is searched for dead links.\n \n
    """
    try:
        dfCompiled, msg, err = pd.DataFrame({}), "", False
        datasets, _, _ = get_datasets()
        refs, _, _ = get_dataset_refs()
        if dataset_name is not None: 
            datasets = datasets.query(f"Dataset_Name=='{dataset_name}'")
            refs = refs.query(f"Dataset_Name=='{dataset_name}'")

        for index, row in datasets.iterrows():
            print(f"looking for dead links in dataset ({index+1}/{len(datasets)}): {row.Dataset_Name}")
            dlDF = pd.DataFrame()
            dlDF = dead_links(get_links(row["Data_Source"]), dlDF)
            dlDF = dead_links(get_links(row["Distributor"]), dlDF)
            dlDF = dead_links(get_links(row["Description"]), dlDF)

            # dead links in dataset refrences    
            dsRefs = refs.query(f"ID=={row.ID}")
            for _, refRow in dsRefs.iterrows():
                dlDF = dead_links(get_links(refRow["Reference"]), dlDF)

            if len(dlDF) > 0:
                dlDF["ID"] = row["ID"]
                dlDF["Dataset_Name"] = row["Dataset_Name"]
                dlDF["Dataset_Long_Name"] = row["Dataset_Long_Name"]
                if len(dfCompiled ) < 1:
                    dfCompiled = dlDF
                else:
                    dfCompiled = pd.concat([dfCompiled, dlDF], ignore_index=True)    
        # dfCompiled.to_csv("./dead_links.csv", index=False)
        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        dfCompiled = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": dfCompiled.to_dict(), "message": msg, "error": err, "version": API_VERSION}

