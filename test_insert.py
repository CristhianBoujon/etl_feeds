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

print(run2.insert_feed("feeds/feeds_doplim_com20170419214939764505.xml", connection))
#run2.insert_feed("feeds/extract_casas.xml", connection)
