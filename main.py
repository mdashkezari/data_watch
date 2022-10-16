
from fastapi import FastAPI



__api_version__ = "1"


tags_metadata = [
    {
        "name": "Root",
        "description": "System roots.",
    },
#     {
#         "name": "Data Retrieval",
#         "description": "Operations associated with retrieving subsets of data. An **SQL** statement input is used to filter the data sets.",
#     },
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
              openapi_url=f"/api/v{__api_version__}/openapi.json",
              docs_url=f"/api/v{__api_version__}/doc",
              redoc_url=f"/api/v{__api_version__}/redoc",
            #   dependencies=[Depends(check_authentication_header)]
            )






@app.get("/", tags=["Root"], summary="Application root")
async def root():
    return "Simons CMAP Data Integrity API!"