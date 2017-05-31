import requests
from db import DBSession
from feeds_model import TempAd, TempAdProperty
from sqlalchemy.sql.expression import func
import time
import logging
import datetime as dtt

# start - logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_formatter = logging.Formatter('[%(asctime)s] %(message)s')
logger_handler = logging.FileHandler("logs/{0}_feeds_load.log".format(dtt.datetime.today().strftime("%Y-%m-%d")))
logger_handler.setFormatter(log_formatter)
logger.addHandler(logger_handler)
# end - logging configuration


sub_query = DBSession.query(TempAd.id).\
            join(TempAd.properties).\
            filter( TempAdProperty.name == "cityid", 
                    TempAdProperty.value != None,
                    TempAd.ad_id == None)

query = DBSession.query(TempAd).\
            join(TempAd.properties).\
            filter( TempAdProperty.name == "subcatid", 
                    TempAdProperty.value != None,
                    TempAdProperty.value != "0",
                    TempAd.ad_id == None).\
            filter(TempAd.id.in_(sub_query)).\
            order_by(func.rand()).\
            limit(100)

while True:

    temp_ads = query.all()
    temp_ads_count = len(temp_ads)
    
    if not temp_ads:
        break

    ads_data = []
    errors = 0

    for temp_ad in temp_ads:
        ad_data = {prop_name: prop.value for prop_name, prop in temp_ad.properties.items()} 
        ad_data["sitioId"] = temp_ad.id
        ad_data["sitio"] = temp_ad.feed_in.partner_code

        # If feed is reliable then moderated = 0 and enabled = 1 and verfied = 1
        # Else moderated = 1 and enabled = 0 and verified = 0
        ad_data["moderated"] = int(not temp_ad.feed_in.reliable)
        ad_data["enabled"] = int(temp_ad.feed_in.reliable)
        ad_data["verified"] = int(temp_ad.feed_in.reliable)        

        ads_data.append(ad_data)

    data = {"ads": ads_data}

    response = requests.post("", json = data)
    if(response.ok):
        response = response.json()

        for resp_ad in response:
            temp_ad = DBSession.query(TempAd).get(resp_ad["sitioId"])
            temp_ad.ad_id = resp_ad["adid"] or None
            temp_ad.error_message = resp_ad["errorMessage"] or None

            errors += 1 if temp_ad.error_message else 0
        
        logger.info("{0} {1} {2}".format(temp_ads_count, temp_ads_count - errors, errors))    
    
    else:
        error_message = "ERROR {0} {1}".format(str(response.status_code), str(response.content))
        for temp_ad in temp_ads:
            temp_ad.error_message = error_message 

        errors = temp_ads_count
        logger.info("{0} {1} {2} {3}".format(temp_ads_count, temp_ads_count - errors, errors, error_message))

    DBSession.commit()

    # Process sleeps in order to avoid overload API's server.
    time.sleep(5)