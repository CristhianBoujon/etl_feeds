from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, reconstructor, relationship
from sqlalchemy.orm.session import object_session

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
#        try:
            # @TODO: Is it good aproach?
        return self.feed_type.bulk_insert(file, self)
#        except Exception as e:
#            raise Exception("No existe feed_type definido para el feed {0}".format(self.id))

class FeedTypeMapping(Base):
    __tablename__ = "fp_feed_type_mappings"

    id = Column(Integer, primary_key = True) 
    field = Column(String)
    method = Column(String)
    param_order = Column(Integer)

    # Mapping relationship feed_type_mapping -> feed_type
    feed_type_id = Column(String, ForeignKey("fp_feed_types.id"))
    feed_type = relationship("FeedType", back_populates = "mapping")

    def __str__(self):
        return "<{0} field = {1}," \
                    " feed_type = {4}" \
                    " method = {2} " \
                    " param_order = {3} >".format(
                                self.__class__.__name__,
                                self.field, 
                                self.method, 
                                self.param_order,
                                self.feed_type_id)

class Param(Base):
    __tablename__ = "fp_params"
    id = Column(Integer, primary_key = True)
    
    name = Column(String)
    value = Column("val", String)
    method = Column(String)

    feed_type_id = Column(String, ForeignKey("fp_feed_types.id"))
    feed_type = relationship("FeedType", back_populates = "additional_params")

class FeedInLocation(Base):
    __tablename__ = "xzclf_feeds_in_location"

    id = Column(Integer, primary_key = True)
    location_name = Column("locationnameinfeed", String)
    location_id = Column("locationid", Integer)
    state_id = Column("stateid", Integer)
    country_id = Column("countryid", Integer)


class RawAd(Base):
    __tablename__ = "fp_raw_ads"

    id = Column(Integer, primary_key = True)
    raw_ad = Column(String)
    feed_in_id = Column(Integer)

