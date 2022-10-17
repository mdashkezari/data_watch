import platform


SHORT_VAR_REGEX = r"^(?![0-9._])[a-zA-Z0-9_]+$"
EXPORT_DIR = "/home/ubuntu/data_watch/"
EXCEL_DIR = "/path/to/excels/"






tags_metadata = [
    {
        "name": "Root",
        "description": "System roots.",
    },
    {
        "name": "Ingested Data",
        "description": "Operations associated with data integrety checks at the database level.",
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
