from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

engine = create_engine("mysql+pymysql://root:33422516@localhost/ads?charset=utf8mb4")
Session = sessionmaker(bind=engine)
DBSession = scoped_session(Session)

