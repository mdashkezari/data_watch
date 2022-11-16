

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from routers import db_checks, excel_checks
from settings import RESPONSE_MODEL_DESCIPTION, tags_metadata, API_VERSION, API_DESCRIPTION
from common import project_init, store_call
from settings import ResponseModel as RESMOD


app = FastAPI(
              title="Simons CMAP Data Integrity API",
              description=API_DESCRIPTION,
              version=API_VERSION,  
              # contact={
              #   "name": "Mohammad D. Ashkezari",
              #   # "url": "https://simonscmap.org",
              #   "email": "mdehghan@uw.edu",
              # },  
              openapi_tags=tags_metadata,
              openapi_url=f"/dataapi.json",
              docs_url=f"/try",
              redoc_url=f"/docs",
              swagger_ui_parameters={"defaultModelsExpandDepth": -1},
            #   dependencies=[Depends(check_authentication_header)],
            )




app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



project_init()
app.include_router(excel_checks.router)
app.include_router(db_checks.router)


@app.middleware("http")
async def user_agent(req: Request, next):
    store_call(req, req.headers["user-agent"])
    res = await next(req)    
    return res


@app.get(
         "/", 
         tags=["Root"], 
         summary="API root",
        response_description=RESPONSE_MODEL_DESCIPTION,
        response_model=RESMOD
         )
async def root():
    return {"data": {}, "message": API_DESCRIPTION.replace('\n', ' ').replace('\r', ''), "error": False, "version": API_VERSION}


@app.get(
         "/version", 
         tags=["Root"], 
         summary="API version",
         response_description=RESPONSE_MODEL_DESCIPTION,
         response_model=RESMOD         
         )
async def root():
    return {"data": {"version": API_VERSION}, "message": "", "error": False, "version": API_VERSION}


