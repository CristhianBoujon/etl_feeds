from feed_process import LOG_FOLDER, DOWNLOAD_FOLDER
from feed_process.tools.downloader import download_file
from feed_process.main_input.preprocessor import preprocess
from multiprocessing import Pool, cpu_count, current_process
from feed_process.tools import cleaner
import os
from feed_process.models import FeedIn
from feed_process.models.db import DBSession
import logging
import datetime as dtt


def process_feed(feed_id, download_folder):

    logger = logging.getLogger(__name__)
    session = DBSession()
    
    feed_in = session.query(FeedIn).get(feed_id)

    url = feed_in.url
    file_name = ""    
    try:
        file_name = download_file(url, download_folder, timeout = 10)
        result = preprocess(file_name, feed_in.bulk_insert, ())
        for res in result:
            logger.info("{0} {1} {2} {3} {4} {5} {6}"
                .format(
                    res['status'], 
                    url, 
                    file_name, 
                    res['inserted'], 
                    res['old_ads'],
                    res['repeated_ads'],
                    res['e_msg'] or ""))
    
    except Exception as e:
        logger.info("{0} {1} {2} {3} {4} {5} {6}"
            .format(
                type(e).__name__,
                url, 
                file_name,  
                0, 0, 0,
                str(e)))

def __process_feed(args):
    return process_feed(*args)

def run(urls = None, feed_ids = None, num_workers = None):
    """
    params:
        urls list of feed urls
        feed_ids list of feed ids
        num_workers: Number of workers process. If None It will be used 

    If urls and feed_ids are not provided the process will be run over all feeds enabled 
    where it was not processed today
    """

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

    pool = Pool(processes = num_workers)
    
    session = DBSession()
    
    if not urls and not feed_ids:
        result = session.query(FeedIn.id).\
            filter(
                FeedIn.last_processed_date < dtt.date.today(), 
                FeedIn.enabled == '1' ).all()
        feed_ids = [t_id[0] for t_id in result]
    
    elif urls:
        # It gets a list of feed_id from urls if it is passed
        result = session.query(FeedIn.id).\
            filter( FeedIn.url.in_(urls)).all()

        feed_ids = [t_id[0] for t_id in result]

    args_collection = [(feed_id, DOWNLOAD_FOLDER) for feed_id in feed_ids]
    results = pool.map_async(__process_feed, args_collection).get()



if __name__ == '__main__':

    # Number of process to be create
    num_workers = cpu_count() * 2

    # Folder to save the feeds
    feeds_folder = "./feeds"

    urls = [
        "http://www.ciudadanuncios.es/feeds/trovit/homes/"
    ]

    import random
    random.shuffle(urls)
    #urls = ['http://allhouses.com.br/feeds/trovit']
    #urls = ['http://www.hispacasas.com/views/nl/admin/xml/immo_xml/trovit.xml']
    run(urls, num_workers = num_workers)
