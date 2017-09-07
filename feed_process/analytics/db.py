from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from feed_process.analytics import DATABASE_CONFIG
from sqlalchemy import event
from sqlalchemy import exc
import os
import warnings

def add_engine_pidguard(engine):
    """Add multiprocessing guards.

    Forces a connection to be reconnected if it is detected
    as having been shared to a sub-process.

    """

    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            # substitute log.debug() or similar here as desired
            warnings.warn(
                "Parent process %(orig)s forked (%(newproc)s) with an open "
                "database connection, "
                "which is being discarded and recreated." %
                {"newproc": pid, "orig": connection_record.info['pid']})
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError(
                "Connection record belongs to pid %s, "
                "attempting to check out in pid %s" %
                (connection_record.info['pid'], pid)
            )

connection_string = "{0}+{1}://{2}:{3}@{4}/{5}?charset=utf8mb4".format(
    DATABASE_CONFIG["engine"],
    DATABASE_CONFIG["driver"],
    DATABASE_CONFIG["user"],
    DATABASE_CONFIG["password"],
    DATABASE_CONFIG["host"],
    DATABASE_CONFIG["db_name"]
)

engine = create_engine(connection_string)

add_engine_pidguard(engine)

Session = sessionmaker(bind=engine)
DBSession = scoped_session(Session)



