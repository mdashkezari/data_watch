

from fastapi import FastAPI
from typing import Optional
from routers import db_checks, excel_checks
from settings import tags_metadata
from common import project_init


__api_version__ = "0.0.31"

app = FastAPI(
              title="Simons CMAP Data Integrity API",
              description="""Simons CMAP collects heterogenous marine datasets from a wide array of public data sources and 
                             curate and harmonize them into a unified data model. 
                             The processed data is then ingested into a database layer where it is exposed to the users. 
                             The Data Integrity API provides utility functions for data validation and database integrity checks. 
                             This API is intended to be used everywhere within the Simons CMAP data pipeline; pre and post ingestion.
                             Also, data producers who are planning to submit their data to Simons CMAP are welcome to validate their data 
                             before submission.
                             """,
              version=__api_version__ ,  
            #   contact={
            #     "name": "Mohammad D. Ashkezari",
            #     "url": "https://simonscmap.org",
            #     "email": "mdehghan@uw.edu",
            #   },  
              openapi_tags=tags_metadata,
              openapi_url=f"/dataapi.json",
              docs_url=f"/try",
              redoc_url=f"/docs",
            #   dependencies=[Depends(check_authentication_header)]
            )

project_init()
app.include_router(excel_checks.router)
app.include_router(db_checks.router)



@app.get("/", tags=["Root"], summary="API root")
async def root():
    return "Simons CMAP Data Integrity API!"



# ## this is an experimental rndpoint. not sure is ncessary to expose a general qyery method.
# @app.get("/query", tags=["Data Retrieval"], summary="SQL command to query CMAP")
# async def db_query(sql: str, serverName: Optional[str]="rainier"):
#     """
#     Executes a `sql` statement and returns the results in form of a dataframe converted to string.
#     """
#     data, msg, err = query(sql, [serverName])  
#     response = {}
#     try:  
#         response = {"data": data.to_dict(), "message": msg, "error": err}
#     except Exception as e:
#             msg += f"\n\n error in db_query: {str(e)}"
#             response = {"data": {}, "message": msg, "error": True}
#     return response
 

