from feeds_downloader import download_file
from feeds_preprocessor import preprocess
from multiprocessing import Pool, cpu_count, current_process
from tools import cleaner
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from feeds_model import *
from db import DBSession

def process_feed(_url, download_folder):

    file_name = ""

    feed_in = DBSession().query(FeedIn).filter_by(url = _url ).one()

    url = feed_in.url

    try:
        file_name = download_file(url, download_folder)
        result = preprocess(file_name, feed_in.bulk_insert, ())
        for res in result:
            print( res)
            """            db_log(db_connection, 
                            url = url, 
                            file = file_name, 
                            status = res['status'], 
                            inserted = res['inserted'],
                            e_msg = res['e_msg'])"""
    except Exception as e:
        print(e)
        """db_log(db_connection, 
            url = url, 
            file = file_name, 
            status = type(e).__name__, 
            e_msg = str(e))"""


def db_log(db_connection, **kwargs):
    field_list = ("url", "file", "status", "inserted", "e_msg")
    value_list = []

    sql = """INSERT INTO fp_feeds_in_log (%s, %s, %s, %s, %s) 
                VALUES (%s, %s, %s, %s, %s)"""
    
    for field in field_list:
        if (field not in kwargs) or (kwargs[field] == "" or kwargs[field] == None):
            value = "NULL"
        elif(type(kwargs[field]) == str):
            value = db_connection.escape(kwargs[field])
        else:
            value = kwargs[field]

        value_list.append(value)

    
    value_list = tuple(value_list)
    sql = sql % (field_list + value_list)

    with db_connection.cursor() as cursor:
        cursor.execute(sql)

    db_connection.commit() 

def run(download_folder, urls, num_workers = None):

    with Pool(processes = num_workers) as pool:
        #results = pool.map_async(process_feed, range(5)).get()
        responses = [pool.apply_async(process_feed, (url, download_folder)) for url in urls]

        for response in responses:
            response.get()

if __name__ == '__main__':

    # Number of process to be create
    num_workers = cpu_count() * 2

    # Folder to save the feeds
    feeds_folder = "./feeds"

    urls = [
        'http://fotoautos.com/xml/trovit/trovit_br.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_ec.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_bo.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_co.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_mx.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_pe.xml'
    ]

    #urls = ['http://allhouses.com.br/feeds/trovit']
    #urls = ['http://www.hispacasas.com/views/nl/admin/xml/immo_xml/trovit.xml']
    run("./feeds", num_workers = num_workers, urls = urls)

