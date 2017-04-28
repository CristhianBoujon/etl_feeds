from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, reconstructor, relationship
#from feeds_mapper import *

Base = declarative_base()


class FeedType(Base):
    __tablename__ = "fp_feed_types"

    id = Column(String, primary_key = True)
    feed_mapper_name = Column("feedmapper", String)

    mapping = relationship("FeedTypeMapping", lazy = 'dynamic')
    feed_in_list = relationship("FeedIn", back_populates = "feed_type")

    @reconstructor
    def on_load(self):
        """ on_load is called when a objects is instantiated from database """
        self.__init__()

    def __init__(self):
        self.feed_mapper = getattr(__import__("feeds_mapper"), self.feed_mapper_name)(self)


class FeedIn(Base):
    """ 
    Object that maps table xzclf_feeds_in 
    Represents feeds source
    """

    __tablename__ = "xzclf_feeds_in"

    feedid = Column(Integer, primary_key = True)
    feed_url = Column("feedurl", String)
    feed_name  = Column("feedname", String)
    countryid  = Column(Integer)
    feedlastid = Column(String)
    feed_type_id = Column(String, ForeignKey("fp_feed_types.id"))

    feed_type = relationship("FeedType", back_populates = "feed_in_list")

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
