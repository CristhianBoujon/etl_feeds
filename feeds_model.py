from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, TIMESTAMP, Unicode, UnicodeText, ForeignKey
from sqlalchemy.orm import sessionmaker, reconstructor, relationship
from sqlalchemy.orm.session import object_session
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm.collections import attribute_mapped_collection

Base = declarative_base()

class FeedType(Base):
    __tablename__ = "fp_feed_types"

    id = Column(String, primary_key = True)
    feed_mapper_name = Column("feedmapper", String)

    mapping = relationship("FeedTypeMapping", lazy = 'dynamic')
    additional_params = relationship("Param", lazy = 'dynamic')

    feed_in_list = relationship("FeedIn", back_populates = "feed_type")

    @reconstructor
    def on_load(self):
        """ on_load is called when a objects is instantiated from database """
        self.__init__()

    def __init__(self):
        self.feed_mapper = getattr(__import__("feeds_mapper"), self.feed_mapper_name)(self)


    def bulk_insert(self, file, feed_in):
        return self.feed_mapper.bulk_insert(file, feed_in)

    def map_ad(self, raw_content):
        return self.feed_mapper.map(raw_content)

class FeedIn(Base):
    """ 
    Object that maps table xzclf_feeds_in 
    It represents some feed source
    """

    __tablename__ = "xzclf_feeds_in"

    id = Column("feedid", Integer, primary_key = True)
    url = Column("feedurl", String)
    name  = Column("feedname", String)
    countryid  = Column(Integer)
    feedlastid = Column(String)
    feed_type_id = Column(String, ForeignKey("fp_feed_types.id"))

    feed_type = relationship("FeedType", back_populates = "feed_in_list")

    def bulk_insert(self, file):
        # @TODO: Is it good aproach?
        return self.feed_type.bulk_insert(file, self)

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

    id = Column(Integer, primary_key=True)
    
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

        
        # If location can't be mapped because FeedInLocation does not exist
        # Or FeedInLocation.location_id, FeedInLocation.state_id and FeedInLocation.country_id
        # does not revised by moderator yet  
        self.feed_in_location = dict_properties.pop("feed_in_location", None)
        self.feed_in_location_id = dict_properties.pop("feed_in_location_id", None)
        
        # If subcat can't be mepped because FeedInSubcategory does not exist
        # Or it does not revised by moderator yet  
        self.feed_in_subcat_id = dict_properties.pop("feed_in_subcat_id", None)

        for name, value in dict_properties.items():
            self.properties[name] = TempAdProperty(name, value)

    @classmethod
    def with_characteristic(self, name, value):
        return self.properties.any(name = key, value = value)


class RawAd(Base):
    __tablename__ = "fp_raw_ads"

    id = Column(Integer, primary_key = True)
    raw_ad = Column(String)
    feed_in_id = Column(Integer, ForeignKey("xzclf_feeds_in.feedid"))
    status = Column(String)

    feed_in = relationship("FeedIn")

    def map(self):
        temp_ad = self.feed_in.feed_type.map_ad(self.raw_ad)
        self.status = "M"

        return temp_ad