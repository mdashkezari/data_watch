import glob, shutil, inspect
from pathlib import Path
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from typing import IO
from fastapi import Response, APIRouter, File, Header, Depends, UploadFile, HTTPException
from fastapi.responses import FileResponse
from starlette import status
from tempfile import NamedTemporaryFile
import shutil
from http import HTTPStatus


# relative path imports
import sys
sys.path.append("..")
from settings import tags_metadata, EXCEL_DIR, EXPORT_DIR, SHORT_VAR_REGEX, ResponseModel as RESMOD, RESPONSE_MODEL_DESCIPTION
from common import make_dir, project_init, language_check, dead_links, get_links, find_cruise

 


def get_sheets(path):
    print("PPPPPPPPPPPPPPP")
    print(path)
    try:
        dataDF = pd.read_excel(path, sheet_name="data")
    except Exception as e:
        print(str(e))  
        dataDF = pd.DataFrame({})
    try:
        datasetDF = pd.read_excel(path, sheet_name="dataset_meta_data")
    except Exception as e:
        print(str(e))  
        datasetDF = pd.DataFrame({})
    try:
        varsDF = pd.read_excel(path, sheet_name="vars_meta_data")
    except Exception as e:
        print(str(e))  
        varsDF = pd.DataFrame({})

    print(")))))))))))))))))")  
    print(len(dataDF), len(datasetDF), len(varsDF))    
    return dataDF, datasetDF, varsDF


def validate_schema(schema, df, exportPath=""):
    try:
        print(f"\tschema check to be exported at: {exportPath}")
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as err:
        if len(exportPath) > 0: err.failure_cases.to_csv(exportPath, index=False)
        try:
            result = err.failure_cases.fillna("").to_dict()
        except:
            result = {}    
        return result
    return {}



varSchema = DataFrameSchema(
    {
        "var_short_name": Column(str,[ 
                                      Check.str_length(min_value=1, max_value=100), 
                                      Check.str_matches(SHORT_VAR_REGEX), 
                                      ],
                                      nullable=False, 
                                      unique=True,
                                      required=True
                                ),
        "var_long_name": Column(str, Check.str_length(min_value=1, max_value=500), nullable=False, unique=True, required=True),
        "var_sensor": Column(str, Check.str_length(min_value=1, max_value=50), nullable=False, required=True),
        "var_unit": Column(str, nullable=True, required=True),
        "var_spatial_res": Column(str, nullable=False, required=True),
        "var_temporal_res": Column(str, nullable=False, required=True),
        "var_discipline": Column(str, nullable=False, required=True),
        "visualize": Column(int, Check.isin([0, 1]), nullable=False, required=True),
        "var_keywords": Column(str, nullable=False, unique=True, required=True),
        "var_comment": Column(str, nullable=True, required=True),
        "org_id": Column(float, nullable=True, required=False),
        "conversion_coefficient": Column(float, nullable=True, required=False),
    },
    strict=True,
    coerce=False,
)


datasetSchema = DataFrameSchema(
    {
        "dataset_short_name": Column(str,[ 
                                      Check.str_length(min_value=1, max_value=100), 
                                      Check.str_matches(SHORT_VAR_REGEX), 
                                      ],
                                      nullable=False,
                                      unique=True, 
                                      required=True
                                ),
        "dataset_long_name": Column(str, Check.str_length(min_value=1, max_value=500), nullable=False, unique=True, required=True),
        "dataset_version": Column(str, nullable=True, required=True),
        "dataset_release_date": Column(pa.DateTime, nullable=True, required=True),
        "dataset_make": Column(str, Check(lambda s: s.str.lower().isin(["observation", "model"])), nullable=False, required=True),
        "dataset_source": Column(str, nullable=False, required=True),
        "dataset_distributor": Column(str, nullable=False, required=True),
        "dataset_acknowledgement": Column(str, nullable=False, required=True),
        "dataset_history": Column(str, nullable=True, required=True),
        "dataset_description": Column(str, nullable=False, required=True),
        "dataset_references": Column(str, nullable=True, required=True),
        "climatology": Column(int, Check.isin([0, 1]), nullable=False, required=False),
        "cruise_names": Column(str, nullable=True, required=False),
    },
    strict=True,
    coerce=False,
)



dataSchema = DataFrameSchema(
    {
        "time": Column(pa.DateTime, nullable=False, required=True),
        "lat": Column(float, Check.in_range(-90, 90, include_min=True, include_max=True), nullable=False, required=True),
        "lon": Column(float, Check.in_range(-180, 180, include_min=True, include_max=True), nullable=False, required=True),
        "depth": Column(float, Check.ge(0), nullable=False, required=False),
    },
    strict=False,
    coerce=False,
)




def cross_validate_data_vars(dataDF, varsDF, exportPath=""):
    """
    check the variables listed in the vars-meta_data sheet exist in data sheet and vice-versa.
    """
    failure_case = []
    try:
        print(f"\tcross-validating data sheet vs vars sheet")
        dataCols = list(set(dataDF.columns) - set(["time", "lat", "lon", "depth"]))
        vars = list(varsDF["var_short_name"].values)
        notInData = list(set(vars) - set(dataCols))
        notInVars = list(set(dataCols) - set(vars))
        if len(set(dataDF.columns)) != len(list(dataDF.columns)):
            failure_case.append("column names in the data sheet must be unique.")
        if len(dataCols) != len(vars):
            failure_case.append("number of variable columns in the data sheet do not match the number of variables in the vars_meta_data sheet.")
        if len(notInData) > 0:    
            failure_case.append(f"{notInData} defined in vars_meta_data sheet but not found the data sheet.")
        if len(notInVars) > 0:    
            failure_case.append(f"{notInVars} are variable columns in the data sheet but not defined in the vars_meta_data sheet.")
    except Exception as e:
        failure_case.append(str(e))
    errDF = pd.DataFrame({"failure_case": failure_case})    
    if len(exportPath) > 0: errDF.to_csv(exportPath, index=False)
    try:
        result = errDF.fillna("").to_dict()
    except:
        result = {}    
    return result 



def lang_datasetDF(datasetDF, exportPath=""):
    print(f"\tlooking for potential language corrections in dataset_description")
    resultsDF = pd.DataFrame()
    if "dataset_description" in list(datasetDF.columns):
        resultsDF = language_check(datasetDF.at[0, "dataset_description"])
    if len(exportPath) > 0 and len(resultsDF) > 0: resultsDF.to_csv(exportPath, index=False)          
    try:
        resultsDF = resultsDF.fillna("")
        result = resultsDF.to_dict()
    except:
        result = {}    
    return result         



def dead_links_datasetDF(datasetDF, exportPath=""):
    print(f"\tlooking for dead links in datasetDF")
    dlDF = pd.DataFrame()
    if "dataset_source" in list(datasetDF.columns):
        dlDF = dead_links(get_links(datasetDF.at[0, "dataset_source"]), dlDF)
    if "dataset_distributor" in list(datasetDF.columns):
        dlDF = dead_links(get_links(datasetDF.at[0,"dataset_distributor"]), dlDF)
    if "dataset_description" in list(datasetDF.columns):
        dlDF = dead_links(get_links(datasetDF.at[0,"dataset_description"]), dlDF)
    if "dataset_references" in list(datasetDF.columns):
        dlDF = dead_links(get_links(datasetDF.at[0,"dataset_references"]), dlDF)

    if len(exportPath) > 0 and len(dlDF) > 0: dlDF.to_csv(exportPath, index=False)          
    try:
        result = dlDF.fillna("").to_dict()
    except:
        result = {}    
    return result 


def check_cruises(datasetDF, dataDF, exportPath=""):
    failure_case = []
    try:
        if "cruise_names" in list(datasetDF.columns):        
            if len(dataDF)>0:
                dataTimes = pd.to_datetime(dataDF["time"])
                dataStartTime, dataEndTime = min(dataTimes).replace(tzinfo=None), max(dataTimes).replace(tzinfo=None)
                dataMinLat, dataMaxLat = min(dataDF["lat"]), max(dataDF["lat"])
                dataMinLon, dataMaxLon = min(dataDF["lon"]), max(dataDF["lon"])
            cruiseNames = datasetDF["cruise_names"].values
            for index, cruise in enumerate(cruiseNames):
                print(f"\tchecking cruise ({index+1}/{len(cruiseNames)}): {cruise}")
                cruiseDF, _, _ = find_cruise(cruise)
                if len(cruiseDF) != 1: 
                    failure_case.append(f"cannot identify {cruise} in the CMAP cruise list")
                elif len(dataDF)>0:
                    try:
                        cruiseStartTime, cruiseEndTime = pd.to_datetime(cruiseDF["Start_Time"]).values[0], pd.to_datetime(cruiseDF["End_Time"]).values[0]
                        cruiseLatMin, cruiseLatMax = cruiseDF["Lat_Min"].values[0], cruiseDF["Lat_Max"].values[0]
                        cruiseLonMin, cruiseLonMax = cruiseDF["Lon_Min"].values[0], cruiseDF["Lon_Max"].values[0]
                        
                        if cruiseStartTime > dataStartTime or cruiseEndTime < dataEndTime: 
                            failure_case.append(f"temporal range in the data sheet do not match those of cruise {cruise}.\n data temporal range: {(dataStartTime, dataEndTime)} \ncruise temporal range: {(cruiseStartTime, cruiseEndTime)}")
                        if cruiseLatMin > dataMinLat or cruiseLatMax < dataMaxLat: 
                            failure_case.append(f"lattitude range in the data sheet do not match those of cruise {cruise}.\n data lat range: {(dataMinLat, dataMaxLat)} \ncruise lat range: {(cruiseLatMin, cruiseLatMax)}")
                        if cruiseLonMin > dataMinLon or cruiseLonMax < dataMaxLon: 
                            failure_case.append(f"longitude range in the data sheet do not match those of cruise {cruise}\n data lon range: {(dataMinLon, dataMaxLon)} \ncruise lon range: {(cruiseLonMin, cruiseLonMax)}")
                    except Exception as e:
                        failure_case.append(f"Exception in inner 'check_cruises': {str(e)}")
    except Exception as e:
        failure_case.append(f"Exception in 'check_cruises': {str(e)}")
    errDF = pd.DataFrame({"failure_case": failure_case})    
    if len(exportPath) > 0: errDF.to_csv(exportPath, index=False)
    try:
        result = errDF.fillna("").to_dict()
    except:
        result = {}    
    return result 






#################################
#                               #
#           endpoints           #
#                               #
#################################


# async def valid_content_length(content_length: int = Header(..., lt=100_000_000)):
async def valid_content_length(content_length: int = Header(10)):
    return content_length


router = APIRouter(
                   prefix="/excel",
                   tags=[tags_metadata[1]["name"]]
                   )


@router.post(
            "/", 
            tags=[], 
            status_code=HTTPStatus.ACCEPTED,
            summary="Submit raw dataset in excel template format",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
async def upload_file(
                response: Response,
                file: UploadFile = File(...), 
                file_size: int = Depends(valid_content_length)
                ):         
    try:            
        ## see https://github.com/tiangolo/fastapi/issues/362  
        msg, err = "", False
        real_file_size = 0
        temp: IO = NamedTemporaryFile(delete=False)
        for chunk in file.file:
            real_file_size += len(chunk)
            if real_file_size > file_size:
                raise HTTPException(
                                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, 
                                    detail="File too large"
                                    )
            temp.write(chunk)
        temp.close()
        excelFName = f"{EXCEL_DIR}{file.filename}"
        shutil.move(temp.name, excelFName)
        excelFiles = [excelFName]
        for index, excelFN in enumerate(excelFiles):
            print(f"checking excel file ({index+1}/{len(excelFiles)}): {excelFN}")
            dataDF, datasetDF, varsDF = get_sheets(f"{excelFN}")    
            basename = Path(excelFN).stem
            EXPORT_EXCEL_DIR = f"{EXPORT_DIR}{basename}/"
            make_dir(EXPORT_EXCEL_DIR)
            shutil.copy(excelFN, EXPORT_EXCEL_DIR)
            varSchemaCases = validate_schema(varSchema, varsDF, exportPath=f"{EXPORT_EXCEL_DIR}var_schema.csv")        
            # just pass the first row of the dataset_meta_data    
            datasetSchemaCases = validate_schema(datasetSchema, datasetDF.head(1), exportPath=f"{EXPORT_EXCEL_DIR}dataset_schema.csv")       
            dataSchemaCases = validate_schema(dataSchema, dataDF, exportPath=f"{EXPORT_EXCEL_DIR}data_schema.csv")
            cvdv = cross_validate_data_vars(dataDF, varsDF, exportPath=f"{EXPORT_EXCEL_DIR}cross_validate_data_vars.csv")        
            lang = lang_datasetDF(datasetDF.head(1), exportPath=f"{EXPORT_EXCEL_DIR}lang.csv")
            dl = dead_links_datasetDF(datasetDF, exportPath=f"{EXPORT_EXCEL_DIR}dead_links.csv")
            cruise = check_cruises(datasetDF, dataDF, exportPath=f"{EXPORT_EXCEL_DIR}cruise.csv")
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        datasetSchemaCases = {}
        dataSchemaCases = {}
        cvdv = {}
        lang = {}
        dl = {}
        cruise = {}
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": {
                     "data_schema": dataSchemaCases,
                     "dataset_schema": datasetSchemaCases,
                     "var_schema": varSchemaCases,
                     "data_vars": cvdv,
                     "lang_dataset": lang,
                     "dead_links": dl,
                     "cruise": cruise,
                     }, 
            "message": msg, 
            "error": err
            }
    # return FileResponse(excelFName)  
    # return FileResponse(excelFName, media_type="video/mp4")  






# checks:
    # schemas (validates only the first row of the dataset_meta_data)
    # cross_validate_data_vars
    # lang typo/grammar in description
    # dead links in description, source, distributor, references   >>>>> needs more test
    # cruises:  names, datetime range, loction range


# to do:
# keywords
# upload file type/size check

