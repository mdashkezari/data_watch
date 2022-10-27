import os
from settings import EXPORT_DIR, UPLOAD_DIR, UPLOAD_EXCEL_DIR
from db import query



def make_dir(directory):
    """
    Creates a new directory if doesn't exist.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return


def project_init():
    """
    Steps at project stratup.
    """
    make_dir(EXPORT_DIR)
    make_dir(UPLOAD_DIR)
    make_dir(UPLOAD_EXCEL_DIR)
    return
    



def get_catalog():
    return query("select * from dbo.udfCatalog()", servers=["rainier"])

def get_datasets():
    return query("select * from tbldatasets order by id desc", servers=["rainier"])

def get_dataset_refs():
    return query("select * from tblDataset_References r join tblDatasets d on r.Dataset_ID=d.id order by r.dataset_id desc", servers=["rainier"])    


def find_cruise(name):
    return query(f"select * from tblCruise where [Name]='{name}' or Nickname='{name}'", servers=["rainier"])    





