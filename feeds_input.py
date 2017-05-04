from feeds_downloader import download_file
from feeds_preprocessor import preprocess
from multiprocessing import Pool, cpu_count, current_process
from tools import cleaner
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from feeds_model import *
from db import DBSession
import logging
import datetime as dtt


# start - logging configuration
# @TODO: Maybe logging in this way it is an error sinse 
# https://docs.python.org/3.5/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter('[%(asctime)s] %(message)s')
logger_handler = logging.FileHandler("logs/{0}_feeds_input.log".format(dtt.datetime.today().strftime("%Y-%m-%d")))
logger_handler.setFormatter(log_formatter)
logger.addHandler(logger_handler)
# end - logging configuration

def process_feed(_url, download_folder):

    logger = logging.getLogger(__name__)

    feed_in = DBSession().query(FeedIn).filter_by(url = _url ).one()

    url = feed_in.url

    try:
        file_name = download_file(url, download_folder)
        result = preprocess(file_name, feed_in.bulk_insert, ())
        for res in result:
            logger.info("{0} {1} {2} {3} {4}".format(url, file_name, res['status'], res['inserted'], res['e_msg']))
    
    except Exception as e:
        file_name = ""
        logger.info("{0} {1} {2} {3} {4}".format(url, file_name, type(e).__name__, 0, str(e)))




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
        ,'http://inmueblesenweb.com/xml-trovit/'
    ]

    #urls = ['http://allhouses.com.br/feeds/trovit']
    #urls = ['http://www.hispacasas.com/views/nl/admin/xml/immo_xml/trovit.xml']
    run("./feeds", num_workers = num_workers, urls = urls)

