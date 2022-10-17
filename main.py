
from urllib import response
from fastapi import FastAPI
from typing import Optional
from routers import db_checks
from settings import tags_metadata


__api_version__ = "0.21"

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


app.include_router(db_checks.router)



@app.get("/", tags=["Root"], summary="Application root")
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
 

