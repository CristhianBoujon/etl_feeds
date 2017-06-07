from multiprocessing import Pool
from feed_process.models import *
from feed_process.models.db import DBSession
from feed_process.tools import chunk_list
import os
import logging
import datetime as dtt
from feed_process import LOG_FOLDER

def create_temp_ad(raw_ad_ids):
    session = DBSession()
    raw_ad_list = DBSession.query(RawAd).filter(RawAd.id.in_(raw_ad_ids)).all()
    
    bulk_temp_ads = []
    bulk_props = []
    processed = 0
    for raw_ad in raw_ad_list:
        try:

            temp_ad = raw_ad.map()
            temp_ad.id = raw_ad.id
            
            # Set temp_ad_id to all TempAdProperty instances
            for prop_name, prop in temp_ad.properties.items():

                # Adding to bulk
                bulk_props.append({"temp_ad_id": temp_ad.id, "name": prop.name, "value": prop.value})

            # Adding to bulk
            bulk_temp_ads.append({
                "id": temp_ad.id, 
                "feed_in_location_id": temp_ad.feed_in_location_id, 
                "feed_in_subcat_id": temp_ad.feed_in_subcat_id,
                "feed_in_id": raw_ad.feed_in_id 

                })

            processed += 1

        except Exception as e:
            logger.info("RawId: {0} {1} {2}".format(raw_ad.id, type(e).__name__, str(e)))

    if bulk_temp_ads:
        session.execute(TempAd.__table__.insert(), bulk_temp_ads)
    
    if bulk_props:
        session.execute(TempAdProperty.__table__.insert(), bulk_props)

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

    total_to_be_process = DBSession.query(RawAd).filter(RawAd.status == "P").count()

    total_time = 0

    # process will scan all existing RawAd inserted
    while True:

        # We need to chunk result query sinse raw ad is so big
        raw_ads = DBSession.query(RawAd.id).\
            filter(RawAd.id > last_id, RawAd.status == "P").\
            order_by(RawAd.id).\
            limit(max_size).\
            all()

        # If no more RawAds it breks loop
        if not raw_ads:
            break

        raw_ads = [raw_ad[0] for raw_ad in raw_ads]
        last_id = raw_ads[-1]

        chunked_raw_ads = chunk_list(raw_ads, chunk_size)
        
        import time
        start = time.time()
        results = pool.map_async(create_temp_ad, chunked_raw_ads).get()
        end = time.time() - start
        total_time += end
        processed += sum(results)
        print("Processed: {0}/{1} in {2} secs".format(processed, total_to_be_process, end))

if __name__ == "__main__":

    run()


        
        
