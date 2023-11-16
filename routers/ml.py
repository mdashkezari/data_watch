


from fastapi import APIRouter, status, Response, BackgroundTasks, Request, HTTPException
from typing import Optional, List
from fastapi.responses import FileResponse
import pandas as pd
import inspect


# relative path imports
import sys
sys.path.append("..")
from db import query
from settings import API_VERSION, SUCCESS_MSG, ML_DESCRIPTION, tags_metadata, ResponseModel as RESMOD, RESPONSE_MODEL_DESCIPTION, EXPORT_DIR
from common import cluster_query, make_random_dir, remove_dir
import recommender as rec


router = APIRouter(
                   prefix="/ml",
                   tags=[tags_metadata[4]["name"]]
                   )


@router.get(
            "/", 
            tags=[], 
            summary="ML/Analytics root",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
def cluster_root():
    return {"data": {}, "message":  ML_DESCRIPTION, "error": False, "version": API_VERSION}
                      



@router.get(
            "/recommend/popular", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="List of popular datasets",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
def popular_datasets(response: Response):
    """
    Return list of top 30 dataset tables based on the overall popularity.

    <br/><br/>
    Returns
    -------    
    a JSON object containing the followings:
    
    - `data`: JSON object containing the popular dataset tables.

    - `message`: general description or an error message.

    - `error`: True when an exception occurs. 

    - `version`: the API version.

    """ 
    try:
        data, msg, err = pd.DataFrame({}), "", False

        pop_ds, _, _ = rec.popular_datasets(top=30)
        data = pop_ds["Table_Name"]

        msg = SUCCESS_MSG
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        data = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": data.to_dict(), "message": msg, "error": err, "version": API_VERSION}



@router.get(
            "/recommend/again", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Suggest previously used datasets by the user",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
def recommend_again(user_id: int, response: Response):
    """
    Return a list of top 30 dataset tables that user has previously called.

    - `user_id`: an integer indicating the user's ID.

    <br/><br/>
    Returns
    -------    
    a JSON object containing the followings:
    
    - `data`: JSON object containing the used dataset tables.

    - `message`: general description or an error message.

    - `error`: True when an exception occurs. 

    - `version`: the API version.

    """ 
    try:
        data, msg, err = pd.DataFrame({}), "", False
        if user_id > 1:    # if user is unregistered
            recent_ds, _, _ = rec.recently_used_datasets(user_id, top=30)
            data= recent_ds["Table_Name"]
            msg = SUCCESS_MSG
        else:
            msg = "invalid user_id"    
            err = True        
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        data = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": data.to_dict(), "message": msg, "error": err, "version": API_VERSION}




@router.get(
            "/recommend/also", 
            tags=[], 
            status_code=status.HTTP_200_OK,
            summary="Suggest datasets based on other user activities with similar interests",
            description="",
            response_description=RESPONSE_MODEL_DESCIPTION,
            response_model=RESMOD
            )
def recommend_also(user_id: int, response: Response):
    """
    Return list of datasets based on collaborative filtering algorithm. 
    The collaborative filtering method computes a list of datasets that users *similar* to `user_id` 
    have used but `user_id` has not used yet.

    - `user_id`: an integer indicating the user's ID.

    <br/><br/>
    Returns
    -------    
    a JSON object containing the followings:
    
    - `data`: JSON object containing the potentially interesting dataset tables.

    - `message`: general description or an error message.

    - `error`: True when an exception occurs. 

    - `version`: the API version.

    """ 

    try:
        data, msg, err = pd.DataFrame({}), "", False
        ############ "Others Have Used" Approach ############
        # recommend dataset based on other similar users
        if user_id > 1:    # if user is unregistered
            ui_df = rec.update_user_item()  
            # ui_df = rec.get_user_item()   
            data= rec.collaborative_based_filtering(ui_df, user_id)
            msg = SUCCESS_MSG
        else:
            msg = "invalid user_id"    
            err = True        
    except Exception as e:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        data = pd.DataFrame({})
        msg = f"{inspect.stack()[0][3]}: {str(e).strip()}"   
        err = True
        print(msg)        
    return {"data": { i: v for i,v in enumerate(data)}, "message": msg, "error": err, "version": API_VERSION}
