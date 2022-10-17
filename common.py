import os
import language_tool_python
import pandas as pd
import requests
import re
from settings import EXPORT_DIR
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
    return
    



def get_catalog():
    return query("select * from dbo.udfCatalog()", servers=["rainier"])

def get_datasets():
    return query("select * from tbldatasets order by id desc", servers=["rainier"])

def get_dataset_refs():
    return query("select * from tblDataset_References r join tblDatasets d on r.Dataset_ID=d.id order by r.dataset_id desc", servers=["rainier"])    


def find_cruise(name):
    return query(f"select * from tblCruise where [Name]='{name}' or Nickname='{name}'", servers=["rainier"])    




def language_check(text):
    """
    detect grammar errors and spelling mistakes.
    """
    text = str(text)
    lTool = language_tool_python.LanguageTool('en-US')
    matches = lTool.check(text)

    results = pd.DataFrame()
    for match in matches:
        row = pd.DataFrame([{
            "ruleID": match.ruleId,
            "message": match.message,
            "sentence": str(match.sentence),
            "replacements": str(match.replacements),
            "offsetInContext": str(match.offsetInContext),
            "context": str(match.context),
            "offset": str(match.offset),
            "errorLength": str(match.errorLength),
            "category": str(match.category),
            "ruleIssueType": str(match.ruleIssueType)            
        }])
        results = pd.concat([results, row], ignore_index=True)
    return results



def check_link(url):
    """
    check the passed link and return the status-code and whether it's redirected.
    """    
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'})
        redirect = False
        if response.history: redirect = True
        # if url.find("http://") != -1 or url.find("doi.") != -1: redirect = False
        return response.status_code, redirect
    except:    
        return -1, False


def get_links(text):
    """
    extract links in a text.
    """
    try:
        text = str(text)
        regex = r'((?:(https?|s?ftp):\/\/)?(?:www\.)?((?:(?:[A-Z0-9][A-Z0-9-]{0,61}[A-Z0-9]\.)+)([A-Z]{2,6})|(?:\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}))(?::(\d{1,5}))?(?:(\/\S+)*))'
        ulist = re.compile(regex, re.IGNORECASE).findall(text)
        links = []
        for url in ulist:
            if url is not None and url[0] is not None: 
                if url[0][-2:] == ").": url[0] = url[0][:-2]
                if url[0][-1:] in [")", ".", ",", ";", "'", '"', "`"]: url[0] = url[0][:-1]
                links.append(url[0].strip())
    except Exception as e:
        return []        
    return links



def dead_links(links, results=pd.DataFrame()):  
    """
    consider the link dead, if the response status code is between 400 and 503.
    currently, the redirected links are considered ok because all http and doi links are almost-always 
    redirected and therefore generate lots of false-positive dead links. The consequence, of accepting 
    all redirected links as valid links is that, some of the redirected links are in fact redirected because
    the requested resource doesn't exist (such as URIs with query strings that doesn't resolve anymore).
    """  
    for l in links:
        print(f"\tchecking link: {l}")
        status, redirect = check_link(l)
        if (status>=400 and status<503):# or redirect:
            row = pd.DataFrame([{
                "link": l,
                "status_code": status,
                "redirect": str(redirect)
            }])
            results = pd.concat([results, row], ignore_index=True)
    return results
