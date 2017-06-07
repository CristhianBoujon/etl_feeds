from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from feed_process.translation import DATABASE_CONFIG

connection_string = "{0}+{1}://{2}:{3}@{4}/{5}?charset=utf8mb4".format(
    DATABASE_CONFIG["engine"],
    DATABASE_CONFIG["driver"],
    DATABASE_CONFIG["user"],
    DATABASE_CONFIG["password"],
    DATABASE_CONFIG["host"],
    DATABASE_CONFIG["db_name"]
)

engine = create_engine(connection_string)
Session = sessionmaker(bind=engine)
DBSession = scoped_session(Session)

