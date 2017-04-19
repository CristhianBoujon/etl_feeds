import feeds_preprocessor
import xml.etree.ElementTree as etree
import pymysql
import run2

    # Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='33422516',
                             db='ads',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

run2.insert_feed("feeds/www_hispacasas_com_20170412112633440048.xml", connection)
#run2.insert_feed("feeds/extract_casas.xml", connection)
