

from fastapi import FastAPI
from typing import Optional
from routers import db_checks, excel_checks
from settings import tags_metadata, API_VERSION, API_DESCRIPTION
from common import project_init




app = FastAPI(
              title="Simons CMAP Data Integrity API",
              description=API_DESCRIPTION,
              version=API_VERSION ,  
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
    return API_DESCRIPTION.replace('\n', ' ').replace('\r', '')



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
 

