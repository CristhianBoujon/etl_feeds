from feed_process import LOG_FOLDER, DONWLOAD_FOLDER
from feed_process.main_input.downloader import download_file
from feed_process.main_input.preprocessor import preprocess
from multiprocessing import Pool, cpu_count
from feed_process.tools import cleaner
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from feed_process.models import *
from feed_process.models.db import DBSession
import logging
import datetime as dtt



def process_feed(_url, download_folder):

    logger = logging.getLogger(__name__)

    feed_in = DBSession().query(FeedIn).filter_by(url = _url ).one()

    url = feed_in.url
    file_name = ""    
    try:
        file_name = download_file(url, download_folder)
        result = preprocess(file_name, feed_in.bulk_insert, ())
        for res in result:
            logger.info("{0} {1} {2} {3} {4} {5} {6}"
                .format(
                    url, 
                    file_name, 
                    res['status'], 
                    res['inserted'], 
                    res['old_ads'],
                    res['repeated_ads'],
                    res['e_msg']))
    
    except Exception as e:
        logger.info("{0} {1} {2} {3} {4} {5} {6}"
            .format(
                url, 
                file_name, 
                type(e).__name__, 
                0, 0, 0,
                str(e)))



def run(urls, num_workers = None):
    log_file_name = os.path.join(
        LOG_FOLDER, 
        "{0}_feeds_input.log".format(dtt.datetime.today().strftime("%Y-%m-%d")))

    # start - logging configuration
    # @TODO: Maybe logging in this way it is an error sinse 
    # https://docs.python.org/3.5/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_formatter = logging.Formatter('[%(asctime)s] %(message)s')
    logger_handler = logging.FileHandler(log_file_name)
    logger_handler.setFormatter(log_formatter)
    logger.addHandler(logger_handler)
    # end - logging configuration


    with Pool(processes = num_workers) as pool:
        #results = pool.map_async(process_feed, range(5)).get()
        responses = [pool.apply_async(process_feed, (url, DONWLOAD_FOLDER)) for url in urls]

        for response in responses:
            response.get()

if __name__ == '__main__':

    # Number of process to be create
    num_workers = cpu_count() * 2

    # Folder to save the feeds
    feeds_folder = "./feeds"

    urls = [
        'http://dubai.classonet.com/classonet-jobs-trovit.xml'
        ,'http://www.avisos-chile.com/feeds/trovit/jobs/'
        ,'http://www.avisos-colombia.com/feeds/trovit/jobs/'
        ,'http://www.bachecalavoro.com/export/Trovit_anunico.xml'
        ,'http://www.indads.in/feeds/trovit/jobs/'
        ,'http://www.jobsxl.com/xml/trovit.php'
        ,'http://www.reclutamos.com/trovit_chile-1.xml'
        ,'http://www.tablerotrabajo.com.co/export/Trovit_anunico.xml'
        ,'http://www.tablerotrabajo.com.mx/export/Trovit_anunico.xml'
        ,'http://www.tablerotrabajo.com/export/Trovit_anunico.xml'
        ,'http://www.toditolaboral.com/trovit_empleos_argentina.xml'
        ,'http://www.toditolaboral.com/trovit_empleos_chile.xml'
        ,'http://www.toditolaboral.com/trovit_empleos_colombia.xml'
        ,'http://www.toditolaboral.com/trovit_empleos_ecuador.xml'
        ,'http://www.toditolaboral.com/trovit_empleos_espana.xml'
        ,'http://www.toditolaboral.com/trovit_empleos_mexico.xml'
        ,'http://www.toditolaboral.com/trovit_empleos_peru.xml'
        ,'http://www.toditolaboral.com/trovit_empleos_venezuela.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_ARG.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_AUS.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_AUT.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_BLZ.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_BRA.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_CAN.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_CHL.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_COL.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_CUB.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_ECU.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_ESP.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_HND.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_HTI.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_IND.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_MEX.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_NIC.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_PER.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_PRT.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_TTO.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_UGY.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_USA.xml'
        ,'https://www.tiptopjob.com/joblist/TTJTrovit_VEN.xml'

    ]
    import random
    random.shuffle(urls)
    #urls = ['http://allhouses.com.br/feeds/trovit']
    #urls = ['http://www.hispacasas.com/views/nl/admin/xml/immo_xml/trovit.xml']
    run("./feeds", num_workers = num_workers, urls = urls)

