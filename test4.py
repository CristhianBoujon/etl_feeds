from feeds_downloader import download_file
from feeds_preprocessor import preprocess
from multiprocessing import Pool, cpu_count
from tools import cleaner
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from feeds_model import *

engine = create_engine("mysql+pymysql://root:33422516@localhost/ads?charset=utf8mb4")
Session = scoped_session(sessionmaker(bind=engine))
Session.configure(autoflush = False, expire_on_commit = False)
session = Session()
feed_in = session.query(FeedIn).filter_by(id = 205 ).one()

url = feed_in.url


file_name = "./feeds/fotoautos_com20170503161857563415.xml"
result = preprocess(file_name, feed_in.bulk_insert, ())
for res in result:
    print( res)
