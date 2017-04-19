from feeds_downloader import download_file
from feeds_preprocessor import preprocess
from multiprocessing import Pool, cpu_count
import pymysql
import xml.etree.ElementTree as etree
import os

def insert_feed(file, connection):
    sql = "INSERT INTO fp_splited_feeds_in VALUES (NULL, %s, NULL)"

    with connection.cursor() as cursor:
        for event, element in etree.iterparse(file):
            if element.tag == "ad":
                cursor.execute(sql, (etree.tostring(element)))



    connection.commit()

def get_urls(connection, order_by = None, limit = None):
    
    sql = "SELECT feedurl FROM xzclf_feeds_in WHERE enabled LIKE '1' "

    if order_by:
        sql += "ORDER BY %s " % order_by

    if limit:
        sql += "LIMIT %i" % limit
    
    with connection.cursor() as cursor:
        cursor.execute(sql)    

    return [feed_in['feedurl'] for feed_in in cursor.fetchall()]

def feed_process(url, folder_path):
#    url = args[0]
#    folder_path = args[1]
    print("---------------")
    print("Processing " + url)
    # Database connection data
    db_host = 'localhost'
    db_user = 'root'
    db_pass = '33422516'
    db_name = 'ads'
    
    # Connect to the database
    connection = pymysql.connect(host = db_host,
                                 user = db_user,
                                 password = db_pass,
                                 db = db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    result = preprocess(download_file(url, folder_path), insert_feed, ((connection)))
    print( result['status'], "\t", result['data'][0], "\t", result['data'][1])
    connection.close()
    
def sarasa():
    print ("sarasa")

def run():
    print(1)
    # Number of process to be create
    num_workers = cpu_count() * 2

    # Folder to save the feeds
    feeds_folder = "./feeds"

    # Database connection data
    db_host = 'localhost'
    db_user = 'root'
    db_pass = '33422516'
    db_name = 'ads'

    # Connect to the database
    connection = pymysql.connect(host = db_host,
                                 user = db_user,
                                 password = db_pass,
                                 db = db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    print(2)
    urls = get_urls(connection, order_by = 'RAND()', limit = 50)

    with Pool(processes = num_workers) as pool:
        print(3)
#        results = [pool.apply_async(sarasa, (url, feeds_folder)) for url in urls]
        results = [pool.apply_async(sarasa, ()) for i in range(5)]
        print(len(results))
        #pool.map_async(feed_process, params).get()
    for result in results:
        print("caca")
        result.get()
        print("caco")

if __name__ == '__main__':
    print(1)
    # Number of process to be create
    num_workers = cpu_count() * 2

    # Folder to save the feeds
    feeds_folder = "./feeds"

    # Database connection data
    db_host = 'localhost'
    db_user = 'root'
    db_pass = '33422516'
    db_name = 'ads'

    # Connect to the database
    connection = pymysql.connect(host = db_host,
                                 user = db_user,
                                 password = db_pass,
                                 db = db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    print(2)
    urls = get_urls(connection, order_by = 'RAND()', limit = 50)

    with Pool(processes = num_workers) as pool:
        print(3)
#        results = [pool.apply_async(sarasa, (url, feeds_folder)) for url in urls]
        results = [pool.apply_async(sarasa, ()) for i in range(5)]
        print(len(results))
        #pool.map_async(feed_process, params).get()
    for result in results:
        print("caca")
        result.get()
        print("caco")
