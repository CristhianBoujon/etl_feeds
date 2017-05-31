from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, TIMESTAMP, Unicode, UnicodeText, ForeignKey, Boolean
from sqlalchemy.orm import sessionmaker, reconstructor, relationship
from sqlalchemy.orm.session import object_session
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm.collections import attribute_mapped_collection
from db import DBSession
from datetime import datetime as dtt

Base = declarative_base()

class FeedType(Base):
    __tablename__ = "fp_feed_types"

    id = Column(String, primary_key = True)
    ad_mapper_name = Column("feedmapper", String)

    mapping = relationship("FeedTypeMapping", lazy = 'dynamic')
    additional_params = relationship("Param", lazy = 'dynamic')

    @reconstructor
    def on_load(self):
        """ on_load is called when a objects is instantiated from database """
        self.__init__()

    def __init__(self):
        self.ad_mapper = getattr(__import__("feeds_mapper"), self.ad_mapper_name)(self)


    def bulk_insert(self, file, feed_in):
        return self.ad_mapper.bulk_insert(file, feed_in)

    def map_ad(self, raw_content):
        return self.ad_mapper.map(raw_content)

class FeedIn(Base):
    """ 
    Object that maps table xzclf_feeds_in 
    It represents some feed source
    """

    __tablename__ = "xzclf_feeds_in"

    id = Column("feedid", Integer, primary_key = True)
    url = Column("feedurl", String)
    name  = Column("feedname", String)
    country_id = Column("countryid", Integer)
    partner_code = Column("feedsiteid", String)
    reliable = Column(Boolean)


    
    feed_type_id = Column(String, ForeignKey("fp_feed_types.id"))
    feed_type = relationship("FeedType")

    def bulk_insert(self, file):
        """ Bulk insert from a file """
        
        self.feed_type.ad_mapper.iter_from_file(file)

        max_pending = 10000 # Max INSERTs pending to commit
        current_pending = 0   # count the number of ads processing from the xml
        inserted_ads = 0

        info = {'status': None, 'file': file, 'inserted': None, 'e_msg': None}
        pending_raw_ads = []
        record_ids = []
        old_ads = 0
        repeated_ads = 0
        while True:
            try:
                raw_content = self.feed_type.ad_mapper.get_raw_content()

                ######################## Begin - Filter section ################################
                # @TODO: Filters should be dinamic. E.g: implement some kind of observer pattern
                date_info = self.feed_type.ad_mapper.exec_method("FECHA", raw_content)
                days = (dtt.today() - dtt.strptime(date_info["date"], date_info["_format"])).days                
                ######################## End - Filter section ################################


                if days > 30:
                    old_ads += 1
                    continue # It skips the remaining code in the loop. 
                             # This way we don't call to database in each iteration 


                ######################## Begin - Filter section ################################
                # @TODO: Filters should be dinamic. E.g: implement some kind of observer pattern
                record_id = str(self.id) + self.feed_type.ad_mapper.exec_method("ID", raw_content)["_id_in_feed"]
                ad_exists = DBSession.execute("SELECT 1 FROM fp_feeds_in_records WHERE id = :id", {"id": record_id}).first()
                ######################## End - Filter section ################################
                if ad_exists:
                    repeated_ads += 1
                else:
                    pending_raw_ads.append(
                        {
                            "raw_ad": raw_content,
                            "feed_in_id": self.id
                        })

                    record_ids.append({"id": record_id})

                    current_pending += 1
                    
                    if( current_pending == max_pending):
                        self.__insert(pending_raw_ads, record_ids)

                        inserted_ads += current_pending
                        current_pending = 0

            except StopIteration:
                if(current_pending != 0):
                    self.__insert(pending_raw_ads, record_ids)
                    
                    inserted_ads += current_pending
                    current_pending = 0

                info['status'] = 'ok'
                info['inserted'] = inserted_ads
                info['repeated_ads'] = repeated_ads
                info['old_ads'] = old_ads

                return info

            except Exception as e:
                info['status'] = type(e).__name__
                info['inserted'] = inserted_ads
                info['e_msg'] = str(e)
                info['repeated_ads'] = repeated_ads
                info['old_ads'] = old_ads

                return info

    def __insert(self, pending_raw_ads, record_ids):
        DBSession.execute(RawAd.__table__.insert(), pending_raw_ads)
        DBSession.execute("INSERT INTO fp_feeds_in_records (id) VALUES (:id)", record_ids)        
        DBSession.commit()    
        del pending_raw_ads[:] 
        del record_ids[:] 


class FeedTypeMapping(Base):
    __tablename__ = "fp_feed_type_mappings"

    id = Column(Integer, primary_key = True) 
    field = Column(String)
    method = Column(String)
    param_order = Column(Integer)

    # Mapping relationship feed_type_mapping -> feed_type
    feed_type_id = Column(String, ForeignKey("fp_feed_types.id"))
    feed_type = relationship("FeedType", back_populates = "mapping")


class Param(Base):
    __tablename__ = "fp_params"
    id = Column(Integer, primary_key = True)
    
    name = Column(String)
    value = Column("val", String)
    method = Column(String)

    feed_type_id = Column(String, ForeignKey("fp_feed_types.id"))
    feed_type = relationship("FeedType", back_populates = "additional_params")

class FeedInLocation(Base):
    __tablename__ = "fp_feeds_in_location"

    id = Column(Integer, primary_key = True)
    slug_location_name = Column("locationnameinfeed", String)
    location_id = Column("locationid", Integer)
    state_id = Column("stateid", Integer)
    country_id = Column("countryid", Integer)
    location_name = Column("locationname", String) 

    temp_ads = relationship("TempAd")

    def waiting_moderation(self):
        """ Return True if feed_in_location was not supervised by moderator yet """
        return not self.location_id

class FeedInSubcategory(Base):
    __tablename__ = "fp_feeds_in_subcats"

    id = Column(Integer, primary_key = True)
    slug_subcat_name = Column(String)
    subcategory_id = Column("subcatid", Integer)

    def waiting_moderation(self):
        """ Return True if feed_in_subcategory was not supervised by moderator yet """
        return not self.subcategory_id


class TempAdProperty(Base):
    """ A fact about an ad. """

    __tablename__ = 'fp_temp_ad_properties'
    
    temp_ad_id = Column(ForeignKey('fp_temp_ads.id'), primary_key = True)
    name = Column(Unicode(64), primary_key = True)
    value = Column(UnicodeText)

    def __init__(self, name, value):
        self.name = name
        self.value = value

class TempAd(Base):
    """ Temporal ad """

    __tablename__ = 'fp_temp_ads'

    id = Column(Integer, primary_key = True)
    ad_id = Column(String)
    error_message = Column(String)

    # FeedIn reference
    feed_in_id = Column(Integer, ForeignKey("xzclf_feeds_in.feedid"))
    feed_in = relationship("FeedIn")


    # FeedInLocation reference
    feed_in_location_id = Column(Integer, ForeignKey("fp_feeds_in_location.id"))
    feed_in_location = relationship("FeedInLocation", back_populates = "temp_ads")

    # FeedInSubcategory reference
    feed_in_subcat_id = Column(Integer, ForeignKey("fp_feeds_in_subcats.id"))
    feed_in_subcat = relationship("FeedInSubcategory")
    
    # TempAdProperty represents a specifically data mapped from a RawAd. 
    # E.g: adtitle, addesc, price, etc 
    properties = relationship("TempAdProperty",
                collection_class = attribute_mapped_collection('name'))

    def set_properties(self, dict_properties):
        """ Set properties based on a dictionary """

        for name, value in dict_properties.items():

            if name.startswith("_") and hasattr(self, name[1:]):
                setattr(self, name[1:], value)
            elif name.startswith("_") and not hasattr(self, name[1:]):
                continue
            else:
                self.properties[name] = TempAdProperty(name, value)

    @classmethod
    def with_characteristic(self, name, value):
        return self.properties.any(name = name, value = value)


class RawAd(Base):
    __tablename__ = "fp_raw_ads"

    id = Column(Integer, primary_key = True)
    raw_content = Column("raw_ad", String)
    feed_in_id = Column(Integer, ForeignKey("xzclf_feeds_in.feedid"))

    status = Column(String)

    feed_in = relationship("FeedIn")

    def map(self):
        temp_ad = self.feed_in.feed_type.map_ad(self)
        self.status = "M" # Mapped

        return temp_ad