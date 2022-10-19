

from fastapi import FastAPI
from typing import Optional
from routers import db_checks, excel_checks
from settings import tags_metadata, API_VERSION, API_DESCRIPTION
from common import project_init



app = FastAPI(
              title="Simons CMAP Data Integrity API",
              description=API_DESCRIPTION,
              version=API_VERSION,  
            #   contact={
            #     "name": "Mohammad D. Ashkezari",
            #     "url": "https://simonscmap.org",
            #     "email": "mdehghan@uw.edu",
            #   },  
              openapi_tags=tags_metadata,
              openapi_url=f"/dataapi.json",
              docs_url=f"/try",
              redoc_url=f"/docs",
              swagger_ui_parameters={"defaultModelsExpandDepth": -1},
            #   dependencies=[Depends(check_authentication_header)],
            )

project_init()
app.include_router(excel_checks.router)
app.include_router(db_checks.router)

@app.get("/", tags=["Root"], summary="API root")
async def root():
    return {"description": API_DESCRIPTION.replace('\n', ' ').replace('\r', '')}


@app.get("/version", tags=["Root"], summary="API version")
async def root():
    return {"version": API_VERSION}


