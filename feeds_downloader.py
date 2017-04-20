from multiprocessing import Pool, TimeoutError, current_process
import time
from random import shuffle
import datetime as dtt
import time
import shutil
from urllib.parse import urlparse
from urllib.request import urlopen
import requests
import os

def download_file(url, path = ".", file_name = None, length = 16*1024):

    if ("http://" in url) or (("https://" in url)):
        response = requests.get(url, stream = True)

        response.raise_for_status() # Raise an exception if status code is not 200

        # If file name is not provided it will be generated automatically
        if(not file_name):
            file_name = generate_file_name(
                url, 
                response.headers["Content-Type"], 
                suffix = dtt.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S%f'))

        path_file_name = os.path.join(path, file_name)
                
        with open(path_file_name, 'wb') as fp:
            for chunk in response.iter_content(length):
                fp.write(chunk)

    elif("ftp://" in url) or (("sftp://" in url)):
        response = urlopen(url, timeout = 60)
        
        # If file name is not provided it will be generated automatically
        if(not file_name):
            file_name = generate_file_name(
                url, 
                response.info()["Content-Type"],
                suffix = dtt.datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S%f'))

        path_file_name = os.path.join(path, file_name)
            
        with open(path_file_name, 'wb') as fp:
            shutil.copyfileobj(response, fp, length)
    
    return path_file_name

"""
def generate_file_name(response):
    dtt_str = 
    file_name = urlparse(response.geturl()).hostname.replace('.', '_') + "_" + dtt_str
    if("text" in response.info()["Content-Type"]):
        file_name += ".xml"
    elif("zip" in response.info()["Content-Type"]):
        file_name += ".zip"
    elif("rar" in response.info()["Content-Type"]):
        file_name += ".rar"    
    elif("json" in response.info()["Content-Type"]):
        file_name += ".json"

    return file_name 

"""

def generate_file_name(url, content_type, prefix = "", suffix = ""):
    file_name = prefix + urlparse(url).hostname.replace('.', '_') + suffix
    if("xml" in content_type):
        file_name += ".xml"
    elif("zip" in content_type):
        file_name += ".zip"
    elif("rar" in content_type):
        file_name += ".rar"    
    elif("json" in content_type):
        file_name += ".json"
    elif("text" in content_type):
        file_name += ".txt"

    return file_name 



def _download_file(args):
    try:
        return download_file(*args)
    except Exception as e:
        print( e)


# If num_workers = None then the number of CPUs will be used
def download(urls, path = None, num_workers = None):

    param = [(url, path) for url in urls]

    with Pool(processes = num_workers) as pool:
        return (pool.map_async(_download_file, param, error_callback = lambda e: print("Wacho")).get())


if __name__ == '__main__':

    urls = [
    'http://fotoautos.com/xml/trovit/trovit_br.xml'
    ,'http://fotoautos.com/xml/trovit/trovit_ec.xml'
    ,'http://fotoautos.com/xml/trovit/trovit_bo.xml'
    ,'http://fotoautos.com/xml/trovit/trovit_co.xml'
    ,'http://fotoautos.com/xml/trovit/trovit_mx.xml'
    ,'http://fotoautos.com/xml/trovit/trovit_pe.xml'

    ];

#    shuffle(urls)

    start = time.time()

    download(urls, "feeds", num_workers = 4)
    end = time.time()

    print (end - start)