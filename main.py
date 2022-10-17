
from urllib import response
from fastapi import FastAPI
from typing import Optional
from db import query



__api_version__ = "0.1"


tags_metadata = [
    {
        "name": "Root",
        "description": "System roots.",
    },
    {
        "name": "Data Retrieval",
        "description": "Operations associated with retrieving subsets of data. An **SQL** statement input is used to filter the data sets.",
    },
#     {
#         "name": "Upload",
#         "description": "Submit a file as input for an extended operation.",
#     },
#     {
#         "name": "Visualization",
#         "description": "Create visualizations using the retrieved data nd serve the results in form of binary files.",
#     },    
#     {
#         "name": "Data Analytics",
#         "description": "Run server-side data analysis and serve the results.",
#     },    
#     {
#         "name": "System Telemetry",
#         "description": "Return monitoring signals collected from system infrastructure.",
#     },   
]



app = FastAPI(
              title="Simons CMAP Data Integrity API",
              description="TODO: CMAP Data Integrity description placeholder",
              version=__api_version__ ,    
              openapi_tags=tags_metadata,
              openapi_url=f"/openapi.json",
              docs_url=f"/docs",
              redoc_url=f"/redoc",
            #   dependencies=[Depends(check_authentication_header)]
            )






@app.get("/", tags=["Root"], summary="Application root")
async def root():
    return "Simons CMAP Data Integrity API!"




## this is an experimental rndpoint. not sure is ncessary to expose a general qyery method.
@app.get("/query", tags=["Data Retrieval"], summary="SQL command to query CMAP")
async def db_query(sql: str, serverName: Optional[str]="rainier"):
    """
    Executes a `sql` statement and returns the results in form of a dataframe converted to string.
    """
    data, msg, err = query(sql, [serverName])  
    response = {}
    try:  
        response = {"data": data.to_dict(), "message": msg, "error": err}
    except Exception as e:
            msg += f"\n\n error in db_query: {str(e)}"
            response = {"data": {}, "message": msg, "error": True}
    return response
 