from feed_process.models import *
from feed_process.models.db import DBSession
from feed_process.tools import chunk_list, id_generator
from feed_process.tools.downloader import download_file
from feed_process import LOG_FOLDER, DOWNLOAD_FOLDER
from feed_process.tools.cleaner import REGEX_IMAGE_EXTENSION
import os
import logging
import datetime as dtt
import time
from multiprocessing import Pool, current_process

def create_temp_ads(raw_ad_ids):
    session = DBSession()
    raw_ad_list = session.query(RawAd).filter(RawAd.id.in_(raw_ad_ids)).all()
    
    bulk_temp_ads = []
    bulk_props = []
    bulk_imgs = []
    processed = 0
    for raw_ad in raw_ad_list:
        try:

            temp_ad = raw_ad.map()
            temp_ad.id = raw_ad.id
            
            # Set temp_ad_id to all TempAdProperty instances
            for prop_name, prop in temp_ad.properties.items():

                # Adding to bulk
                bulk_props.append({"temp_ad_id": temp_ad.id, "name": prop.name, "value": prop.value})

            # Download images
            for image in temp_ad.images:
                # Adding to bulk
                bulk_imgs.append({
                    "temp_ad_id": temp_ad.id, 
                    "external_path": image.external_path, 
                })


            # Adding to bulk
            bulk_temp_ads.append({
                "id": temp_ad.id, 
                "feed_in_location_id": temp_ad.feed_in_location_id, 
                "feed_in_subcat_id": temp_ad.feed_in_subcat_id,
                "feed_in_id": raw_ad.feed_in_id 
            })

            processed += 1

        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.info("RawId: {0} {1} {2}".format(raw_ad.id, type(e).__name__, str(e)))

    if bulk_temp_ads:
        session.execute(TempAd.__table__.insert(), bulk_temp_ads)
    
    if bulk_props:
        session.execute(TempAdProperty.__table__.insert(), bulk_props)

    if bulk_imgs:
        session.execute(TempAdImage.__table__.insert(), bulk_imgs)

    session.commit()

    return processed

def run(num_workers = None, max_size = 10000, chunk_size = 1000):
    
    log_file_name = os.path.join(
        LOG_FOLDER, 
        "{0}_feeds_transform.log".format(dtt.datetime.today().strftime("%Y-%m-%d")))

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
    raw_ads = []
    last_id = 0
    processed = 0
    total_time = 0

    session = DBSession()
    # process will scan all existing RawAd inserted
    while True:
        
        # We need to chunk result query sinse raw ads are so big
        raw_ads = session.query(RawAd.id).\
            filter(RawAd.id > last_id, RawAd.status == "P").\
            order_by(RawAd.id).\
            limit(max_size).\
            all()

        # If no more RawAds it breks loop
        if not raw_ads:
            logger.info("FINISHED. Total RawAds mapped: {0} in {1} secs".format(processed, round(total_time, 2)))
            break

        raw_ads = [raw_ad[0] for raw_ad in raw_ads]
        last_id = raw_ads[-1]

        chunked_raw_ads = chunk_list(raw_ads, chunk_size)
        
        start = time.time()
        results = pool.map_async(create_temp_ads, chunked_raw_ads).get()
        end = time.time() - start
        total_time += end
        processed += sum(results)
        logger.info("Processed: {0} in {1} secs".format(processed, round(end, 2)))



def _download(url):
    match_image_extension = REGEX_IMAGE_EXTENSION.search(url)
    extension = match_image_extension.group(0) if match_image_extension else ".jpg"

    try:        
        return download_file(url, path = DOWNLOAD_FOLDER, file_name = id_generator(20), ext = extension, timeout = 10)        
    except:
        return None

def run_img(tempids = None, num_workers = None, images_by_loop = 1000):

    if tempids:
        images_query = DBSession.query(TempAdImage).\
                    filter(TempAdImage.temp_ad_id.in_(tempids))

    else:
        images_query = DBSession.query(TempAdImage).\
                    filter( TempAdImage.internal_path == None)

    last_id = 0

    pool = Pool(num_workers)
    
    while True:
        images = images_query.\
                    filter(TempAdImage.id > last_id).\
                    order_by(TempAdImage.id).\
                    limit(images_by_loop).\
                    all()
        if not images:
            break
            
        last_id = images[-1].id
        
        arguments = [image.external_path for image in images]
        
        for image, path in zip(images, pool.map(_download, arguments)):
            image.internal_path = path

        DBSession.commit()


if __name__ == "__main__":

    run(chunk_size = 2)


        
        
