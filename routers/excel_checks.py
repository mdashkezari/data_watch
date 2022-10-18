from typing import IO
from fastapi import APIRouter, File, Header, Depends, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from starlette import status
from tempfile import NamedTemporaryFile
import shutil
from http import HTTPStatus


# relative path imports
import sys
sys.path.append("..")
from db import query
from settings import tags_metadata, EXCEL_DIR


 

# async def valid_content_length(content_length: int = Header(..., lt=100_000_000)):
async def valid_content_length(content_length: int = Header(10)):
    return content_length


router = APIRouter(
                   prefix="/excel",
                   tags=[tags_metadata[1]["name"]]
                   )


@router.post(
            "/", 
            tags=[], 
            status_code=HTTPStatus.ACCEPTED,
            summary="Submit raw dataset in excel template format",
            description="",
            response_description="",
            )
def upload_file(
                file: UploadFile = File(...), 
                file_size: int = Depends(valid_content_length)
                ):         
    ## see https://github.com/tiangolo/fastapi/issues/362  

           
    real_file_size = 0
    temp: IO = NamedTemporaryFile(delete=False)
    for chunk in file.file:
        real_file_size += len(chunk)
        if real_file_size > file_size:
            raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="File too large")
        temp.write(chunk)
    temp.close()
    excelFName = f"{EXCEL_DIR}{file.filename}"
    shutil.move(temp.name, excelFName)
    return FileResponse(excelFName)  
    # return FileResponse(excelFName, media_type="video/mp4")  