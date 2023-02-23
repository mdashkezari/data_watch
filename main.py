

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from routers import db_checks, excel_checks, cluster
from settings import RESPONSE_MODEL_DESCIPTION, tags_metadata, API_VERSION, API_DESCRIPTION
from common import project_init, store_call
from settings import ResponseModel as RESMOD


app = FastAPI(
              title="Simons CMAP Data Validation API",
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
app.include_router(cluster.router)


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






# ########################################################
# from common import make_dir
# from settings import API_VERSION, UPLOAD_EXCEL_DIR, EXPORT_DIR

# import tensorflow as tf
# import pandas as pd
# import shutil, inspect, random

# from fastapi import Response, File, UploadFile, BackgroundTasks
# from starlette import status
# from http import HTTPStatus



# def classify_image(model, image_path, class_names, img_shape, channels, scale, verbose):  
#   """
#   Inference on a single image.
#   Return the top class probability and its name.
#   """
#   img = tf.io.read_file(image_path)
#   img = tf.image.decode_image(img, channels=channels)
#   img = tf.image.resize(img, [img_shape, img_shape])
#   if scale: img = img / 255.
#   pred_prob = model.predict(tf.expand_dims(img, axis=0), verbose=verbose)
#   return pred_prob.max(), class_names[pred_prob.argmax()]


# def load_model(model_path, class_names_path):
#   return tf.keras.models.load_model(model_path), pd.read_csv(class_names_path)["class_names"].values


# print("loading model ...")
# model_name = "tvt_efficientNetB0_balanced1000_MERGED_ALL_aug"
# model, class_names = load_model(f"model/{model_name}", f"model/{model_name}/class_names.csv")
# print("model loaded")


# @app.post(
#             "/root", 
#             tags=["Root"],  
#             status_code=HTTPStatus.ACCEPTED,
#             summary="Root",
#             description="",
#             response_description=RESPONSE_MODEL_DESCIPTION,
#             response_model=RESMOD
#             )
# async def upload_image(
#                 response: Response,
#                 file: UploadFile = File(...), 
#                 ):         
#     try:            
#         msg, err = "", False
#         uploadID = random.randint(1000, 999999999)
#         make_dir(EXPORT_DIR)
#         make_dir(UPLOAD_EXCEL_DIR)
#         RAND_UPLOAD_EXCEL_DIR = f"{UPLOAD_EXCEL_DIR}{uploadID}/"
#         make_dir(RAND_UPLOAD_EXCEL_DIR)
#         uploadedExcelFName = f"{RAND_UPLOAD_EXCEL_DIR}{file.filename}"
#         with open(uploadedExcelFName, "w+b") as buffer: shutil.copyfileobj(file.file, buffer)            
#         pred_prob, pred_class_name = classify_image(model, uploadedExcelFName, class_names, img_shape=224, channels=1, scale=False, verbose=0)
#         shutil.rmtree(RAND_UPLOAD_EXCEL_DIR) 
#         msg = "success"
#     except Exception as e:
#         response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
#         msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
#         err = True
#         print(msg)     
#     return {"data": {"prediction": pred_class_name, "prediction_probability": float(pred_prob)}, "message": "", "error": err, "version": API_VERSION}     


# @app.get(
#          "/rootList", 
#          tags=["Root"], 
#          summary="Root list",
#          response_description=RESPONSE_MODEL_DESCIPTION,
#          response_model=RESMOD         
#          )
# async def root_list():
#     return {"data": {"class_names": list(class_names)}, "message": "", "error": False, "version": API_VERSION}




# ####################################################################################