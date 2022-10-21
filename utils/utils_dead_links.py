import pandas as pd
import requests
import re



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
