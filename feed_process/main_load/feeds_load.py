from feed_process.models.db import DBSession
from feed_process.models import TempAd, TempAdProperty
from sqlalchemy.sql.expression import func
from feed_process import LOG_FOLDER
from feed_process.main_load import get_loader_data_connection
from feed_process.translation import Translator
from feed_process.tools.cleaner import slugify
import importlib
import os
import time
import logging
import datetime as dtt

translator = Translator()

def create_loader(loader_name):
    loader_data_connection = get_loader_data_connection(loader_name)
    return getattr(
            importlib.__import__("feed_process.main_load.loader", fromlist = [loader_name]), 
            loader_name)(**loader_data_connection)

def prepare_ad_data(loader, temp_ad):
    ad_data = {prop_name: prop.value for prop_name, prop in temp_ad.properties.items()} 
    ad_data["sitioId"] = temp_ad.id
    ad_data["sitio"] = temp_ad.feed_in.partner_code
    ad_data["canonicalUrl"] = set_canonical_url(loader, temp_ad)

    # If feed is reliable then moderated = 0 and enabled = 1 and verfied = 1
    # Else moderated = 1 and enabled = 0 and verified = 0
    ad_data["moderated"] = int(not temp_ad.feed_in.reliable)
    ad_data["enabled"] = int(temp_ad.feed_in.reliable)
    ad_data["verified"] = int(temp_ad.feed_in.reliable)        

    return ad_data

def set_canonical_url(loader, temp_ad):

    # Getting location data
    location_id = temp_ad.properties["location_id"].value 
    location_slug = loader.get_location(location_id)["locationslug"]
    
    # Getting country data
    country_id = temp_ad.feed_in.country_id
    country = loader.get_country(country_id)
    domain = country["countrydomain"]
    country_slug = country["countryslug"]

    # Getting subcategory data
    subcategory_id = temp_ad.properties["subcatid"].value
    subcategory_slug = loader.get_subcategory(subcategory_id)["subcatslug"]


    subdomain = location_slug or 'www'

    if (subdomain == 'www.' and country_slug == 'cuba'):
        subdomain = '' # No www for cuba since cuba is cuba.anunico.com
    
    pub_url = 'http://{subdomain}.{domain}/{ad_url}/{subcat_slug}/{title_slug}-{ad_id}.html'.\
                format(
                    subdomain = subdomain,
                    domain = domain,
                    ad_url = translator.translate(temp_ad.feed_in.locale, 'AD_URL', "messages"),
                    subcat_slug = translator.translate(temp_ad.feed_in.locale, subcategory_slug, 'slugs'),
                    title_slug = slugify(temp_ad.properties["adtitle"].value, "_"),
                    ad_id = '%REPLACEID%'
                )

    return pub_url



def run(loader_name, sleep_time = 0):

    log_file_name = os.path.join(
    LOG_FOLDER, 
    "{0}_feeds_load.log".format(dtt.datetime.today().strftime("%Y-%m-%d")))

    # start - logging configuration
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_formatter = logging.Formatter('[%(asctime)s] %(message)s')
    logger_handler = logging.FileHandler(log_file_name)
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
                filter(TempAd.id.in_(sub_query))

    loader = create_loader(loader_name)
    
    processed_temp_ads = []
    
    total = query.order_by(func.rand()).count()
    limit = 1000
    temp_ads = query.order_by(func.rand()).limit(limit).all()

    while True:

        temp_ads_count = len(temp_ads)
        processed_temp_ads += [temp_ad.id for temp_ad in temp_ads]

        if not temp_ads:
            break

        ads_data = []
        errors = 0

        for temp_ad in temp_ads:
            ad_data = prepare_ad_data(loader, temp_ad)
            ads_data.append(ad_data)

        # It loads the ads 
        result = loader.load(ads_data)

        for res in result:
            temp_ad = DBSession.query(TempAd).get(res["id"])
            temp_ad.ad_id = res["ad_id"]
            temp_ad.error_message = res["error_message"]

            errors += 1 if temp_ad.error_message else 0

        logger.info("{0}/{3} {1} {2}".format(len(processed_temp_ads), temp_ads_count - errors, errors, total))
        DBSession.commit()

        temp_ads = query.filter(~ TempAd.id.in_(processed_temp_ads)).\
                        order_by(func.rand()).\
                        limit(limit).\
                        all()

        # Process sleeps in order to avoid overload API's server.
        time.sleep(sleep_time)


