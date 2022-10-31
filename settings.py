import os
from pydantic import BaseModel



RESPONSE_MODEL_DESCIPTION = "The results are stored in the `data` field [JSON]. When an error occurs the `error` flag will `True` [Boolean]. Any message will be communicated using the `message` field [String]. The API's version is passed in the `version` field."


HOME = f"{os.path.realpath(os.path.dirname(__file__))}/"
SERVERS=["rainier", "rossby", "mariana"]

SHORT_VAR_REGEX = r"^(?![0-9._])[a-zA-Z0-9_]+$"
EXPORT_DIR = f"{HOME}export/"
UPLOAD_DIR = f"{HOME}upload/"
UPLOAD_EXCEL_DIR = f"{UPLOAD_DIR}excel/"



API_VERSION = "0.0.46"

# for styling reasons avoid multi-line text
API_DESCRIPTION = "Simons CMAP collects heterogenous marine datasets from a wide array of public data sources and "
API_DESCRIPTION += "curate and harmonize them into a unified data model. "
API_DESCRIPTION += "The processed data is then ingested into a database layer where it is exposed to the users. "
API_DESCRIPTION += "The Data Integrity API provides utility functions for data validation and database integrity checks. "
API_DESCRIPTION += "This API is intended to be used everywhere within the Simons CMAP data pipeline; pre and post ingestion. "
API_DESCRIPTION += "Also, data producers who are planning to submit their data to Simons CMAP are welcome to validate their data before submission."


DB_DESCRIPTION = "Database objects may evolve over time in a way that is not consistent with the initial intended integrity plans. "
DB_DESCRIPTION += "This route exposes methods that are critical in maintaining the database performance and integrity."


class ResponseModel(BaseModel):
    data: dict
    message: str = ""
    error: bool = False
    version: str = API_VERSION


tags_metadata = [
    {
        "name": "Root",
        "description": "API root.",
    },
    {
        "name": "Pre Ingestion Checks",
        "description": "Data validation operations for raw data",
    },
    {
        "name": "Post Ingestion Checks",
        "description": "Data integrety and quality checks at the database level",
    },
#     {
#         "name": "Upload",
#         "description": "Submit a file as input for an extended operation",
#     },
#     {
#         "name": "Visualization",
#         "description": "Create visualizations using the retrieved data nd serve the results in form of binary files",
#     },    
#     {
#         "name": "Data Analytics",
#         "description": "Run server-side data analysis and serve the results",
#     },    
#     {
#         "name": "System Telemetry",
#         "description": "Return monitoring signals collected from system infrastructure",
#     },   
]
