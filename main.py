

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from routers import db_checks, excel_checks, cluster, ml
from settings import RESPONSE_MODEL_DESCIPTION, tags_metadata, API_VERSION, API_DESCRIPTION
from common import project_init, store_call
from settings import ResponseModel as RESMOD



app = FastAPI(
              title="Simons CMAP Data Validation API",
              description=API_DESCRIPTION,
              version=API_VERSION,  
              contact={
                "name": "Mohammad D. Ashkezari",
                "url": "https://simonscmap.org",
                "email": "mdehghan@uw.edu",
              },  
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
app.include_router(ml.router)


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






########################################################

##############################################################
#                                                            #
#             Dataset Recommendation Systems                 #
#                                                            #
##############################################################

# @app.get(
#          "/recommender", 
#          tags=["Root"], 
#          summary="A hybrid recommender system to suggest datasets that are likely to match user's interests (Guest Endpoint!)",
#          response_description=RESPONSE_MODEL_DESCIPTION,
#          response_model=RESMOD         
#          )
# async def recommend_dataset(user_id: int):
#     """
#     Return list of datasets based on the overall datasets popularity, the user's recent activities, and collaborative filtering algorithms.
#     The collaborative filtering method computes a list of datasets that users *similar* to `user_id` have used but `user_id` has not used yet.

#     - `user_id`: an integer indicating the user's ID.
   

#     <br/><br/>
#     Returns
#     -------    
#     a JSON object containing the followings:
    
#     - `data`: an empty JSON.

#     - `message`: general description or an error message.

#     - `error`: True when an exception occurs. 

#     - `version`: the API version.

#     """  
#     if not rec.validate_userid(user_id):
#         return {"data": {}, "message": "Invalid user", "error": True, "version": API_VERSION}
    
#     rec_obj = {}
#     pop_ds, _, _ = rec.popular_datasets(top=20)
#     rec_obj["popular"] = list(pop_ds["Table_Name"].values)
#     if user_id != 1:    # if user is unregistered
#         ############### "Use Again" Approach ###############
#         ## recently used datasets by the user him/herself  
#         recent_ds, _, _ = rec.recently_used_datasets(user_id, top=20)
#         rec_obj["use_again"] = list(recent_ds["Table_Name"].values)

#         ############ "Others Have Used" Approach ############
#         # recommend dataset based on other similar users
#         ui_df = rec.update_user_item()  
#         # ui_df = get_user_item()   
#         rec_obj["collaborative"] = list(rec.collaborative_based_filtering(ui_df, user_id))

#     return {"data": rec_obj, "message": "", "error": False, "version": API_VERSION}














# import os
# from common import make_dir
# from settings import API_VERSION, UPLOAD_EXCEL_DIR, EXPORT_DIR

# import tensorflow as tf
# import pandas as pd
# import shutil, inspect, random

# from fastapi import Response, File, UploadFile, BackgroundTasks
# from starlette import status
# from http import HTTPStatus
# from pydantic import BaseModel
# from typing import List


# def classify_image(model, image_path, class_names, img_shape, channels, scale, verbose):  
#   """
#   Inference on a single image.
#   Return the top class probability, its name, the probability associated with each class, and their corresponding class name.
#   """
#   try:
#     img = tf.io.read_file(image_path)
#     img = tf.image.decode_image(img, channels=channels)
#     img = tf.image.resize(img, [img_shape, img_shape])
#     if scale: img = img / 255.
#     pred_prob = model.predict(tf.expand_dims(img, axis=0), verbose=verbose)
#     pred_prob = pred_prob[0]  
#   except Exception as e:
#     return -1, str(e), [-1], ["error"]
#   return pred_prob.max(), class_names[pred_prob.argmax()], pred_prob, class_names


# def load_model(model_path, class_names_path):
#   return tf.keras.models.load_model(model_path), pd.read_csv(class_names_path)["class_names"].values


# models = {}
# model_names = [
#                "tvt_efficientNetB0_balanced1000_MERGED_ALL_plus_SYKE_COWEN_aug",            # all
#                "tvt_efficientNetB0_balanced1000_MERGED_ONLY_IFCB_plus_SYKE_aug",            # IFCB
#                "tvt_efficientNetB0_balanced1000_ZOO_aug",                                   # ZOOSCAN
#             #    "tvt_efficientNetB0_balanced1000_ISIIS_COWEN_aug_hight_dense_1024_2_512",    # ISIIS
#             #    "tvt_efficientNetB0_balanced1000_BERING_aug_hight_dense_1024_2_512",         # ZOOVIS
#             #    "tvt_efficientNetB0_imbalanced_MERGED_ALL_plus_SYKE_COWEN_aug",              # all Imbalanced


#             #    "tvt_efficientNetB0_imbalanced_MERGED_ONLY_IFCB_plus_SYKE_aug",              # only IFCB Imbalanced
#             #    "tvt_efficientNetB0_imbalanced_MERGED_ALL_plus_SYKE_COWEN_aug_epochs15",     # all Imbalanced 15 epochs
#             #    "tvt_efficientNetB0_balanced1000_MERGED_ALL_aug",                            # all - SYKE - COWEN
#                ]
# print("loading models ...")
# for i, model_name in enumerate(model_names):               
#     m, c = load_model(f"models/{model_name}", f"models/{model_name}/class_names.csv")
#     models[i] = {"model": m, "class_names": c}
#     print(f"\t {model_name} loaded")
# print("models loaded")


# class CNNModel(BaseModel):
#     prediction: str = ""
#     prediction_probability: float = 0.0
#     all_probs: List[float] = []
#     all_prob_names: List[str] = []
#     message: str = ""
#     error: bool = False
#     version: str = API_VERSION


# @app.post(
#             "/cnn", 
#             tags=["Root"],  
#             status_code=HTTPStatus.ACCEPTED,
#             summary="Plankton image classifiers (Guest Endpoint!)",
#             description="",
#             # response_description=RESPONSE_MODEL_DESCIPTION,
#             response_model=CNNModel
#             )
# async def inference_image(
#                 response: Response,
#                 model_index: int = 0,
#                 channels: int = 3,
#                 file: UploadFile = File(...), 
#                 ):
#     """
#     Classify a plankton image captured by plankton imaging devices.

#     - `model_index`: an integer (default 0) indicating which model to be used for inference. Below are the available models and their corresponding index:
#         * `0`: a general purpose model covering all imaging instruments. 
        
#         * `1`: a dedicated model trained on IFCB images. 
        
#         * `2`: a dedicated model trained on ZOOSCAN images. 
        
#         * `3`: a dedicated model trained on ISIIS images. 
        
#         * `4`: a dedicated model trained on ZOOVIS images. 
        
#         * `5`: a general purpose model covering all imaging instruments. The model is trained on natural plankton distributions (imbalanced classes).
    
#     - `channels`: number of image channels (default 3). Note That the underlying models are trained on three-channels images, so we recommend to keep it at default value, unless necessary. 
    
#     - `file`: an image file.

#     <br/><br/>
#     Returns
#     -------    
#     a JSON object containing the followings:
    
#     - `prediction`: name of the most probable plankton.

#     - `prediction_probability`: relative probability of the classified class.

#     - `all_probs`: relative probability values associated with all classes.

#     - `all_prob_names`: the plankton name associated with all classes.

#     - `message`: general description or an error message.

#     - `error`: True when an exception occurs. 

#     - `version`: the API version.

#     """         
#     try:            
#         msg, err = "", False
#         uploadID = random.randint(1000, 999999999)
#         make_dir(EXPORT_DIR)
#         make_dir(UPLOAD_EXCEL_DIR)
#         RAND_UPLOAD_EXCEL_DIR = f"{UPLOAD_EXCEL_DIR}{uploadID}/"
#         make_dir(RAND_UPLOAD_EXCEL_DIR)
#         uploadedExcelFName = f"{RAND_UPLOAD_EXCEL_DIR}{file.filename}"
#         with open(uploadedExcelFName, "w+b") as buffer: shutil.copyfileobj(file.file, buffer)            
#         pred_prob, pred_class_name, all_probs, all_prob_names = classify_image(models[model_index]["model"], uploadedExcelFName, models[model_index]["class_names"], img_shape=224, channels=channels, scale=False, verbose=0)
#         shutil.rmtree(RAND_UPLOAD_EXCEL_DIR) 
#         msg = "success"
#     except Exception as e:
#         response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
#         msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
#         err = True
#         print(msg)     
#     finally:
#         if os.path.exists(RAND_UPLOAD_EXCEL_DIR): shutil.rmtree(RAND_UPLOAD_EXCEL_DIR)     
#     return {"prediction": pred_class_name, 
#             "prediction_probability": float(pred_prob),
#             "all_probs": list(all_probs),
#             "all_prob_names": list(all_prob_names),                     
#             "message": "", 
#             "error": err, 
#             "version": API_VERSION
#             }     


# @app.get(
#          "/plankton", 
#          tags=["Root"], 
#          summary="List of plankton species covered by the CNN models (Guest Endpoint!)",
#          response_description=RESPONSE_MODEL_DESCIPTION,
#          response_model=RESMOD         
#          )
# async def plankton_list(model_index: int = 0):
#     """
#     Return a list of plankton names that are covered by each model.

#     - `model_index`: an integer (default 0) indicating a trained model. Below are the available models and their corresponding index:
#         * `0`: a general purpose model covering all imaging instruments. 
        
#         * `1`: a dedicated model trained on IFCB images. 
        
#         * `2`: a dedicated model trained on ZOOSCAN images. 
        
#         * `3`: a dedicated model trained on ISIIS images. 
        
#         * `4`: a dedicated model trained on ZOOVIS images. 
        
#         * `5`: a general purpose model covering all imaging instruments. The model is trained on natural plankton distributions (imbalanced classes).
    

#     <br/><br/>
#     Returns
#     -------    
#     a JSON object containing the followings:
    
#     - `data`: an empty JSON.

#     - `message`: general description or an error message.

#     - `error`: True when an exception occurs. 

#     - `version`: the API version.

#     """  
#     return {"data": {"class_names": list(models[model_index]["class_names"])}, "message": "", "error": False, "version": API_VERSION}


####################################################################################