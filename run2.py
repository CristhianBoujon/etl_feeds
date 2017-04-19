from feeds_downloader import download_file
from feeds_preprocessor import preprocess
from multiprocessing import Pool, cpu_count
from tools import cleaner
import pymysql
import xml.etree.ElementTree as etree
import os
    
def insert_feed(file, db_connection):
    sql_ok = "INSERT INTO fp_splited_feeds_in VALUES (NULL, %s, NULL)"

    cursor = db_connection.cursor()
    xml_parser = etree.iterparse(file)

    max_pending = 10000 # Max INSERTs pending to commit
    current_pending = 0   # count the number of ads processing from the xml
    skip_ads = 0 # Ads to skip in case that we need to "restart" the iterator
    inserted_ads = 0

    while True:
        try:
            event, element = next(xml_parser)

            if element.tag == "ad" and skip_ads == 0:
                cursor.execute(sql_ok, (etree.tostring(element)))
                current_pending += 1
                inserted_ads += 1
                print(current_pending, "Insert", skip_ads, element[0].text)

                element.clear()
                if( current_pending == max_pending):
                    db_connection.commit()
                    current_pending = 0
            elif element.tag == "ad" and skip_ads != 0:
                skip_ads -= 1
                print(current_pending, "Skip", skip_ads, element[0].text)

        except StopIteration:
            break

        except etree.ParseError as e:
            # https://docs.python.org/3/library/xml.etree.elementtree.html#exceptions
            if e.code == 4 or e.code == 11: # Invalid Token or invalid entity
                skip_ads = inserted_ads
                current_pending = 0
                
                print("ERROR CODE: ", e.code, skip_ads)

                cleaner.clear_file(file) # Removes invalid characters from file
                xml_parser = etree.iterparse(file)
            else:
                raise e

    
    db_connection.commit()

    return {'ok': True, 'data': (file, 'Inserted')}

def get_urls(db_connection, order_by = None, limit = None):
    
    sql = "SELECT feedurl FROM xzclf_feeds_in WHERE enabled LIKE '1' "

    if order_by:
        sql += "ORDER BY %s " % order_by

    if limit:
        sql += "LIMIT %i" % limit
    
    with db_connection.cursor() as cursor:
        cursor.execute(sql)    

    return [feed_in['feedurl'] for feed_in in cursor.fetchall()]

def process_feed(url, download_folder):
    # Database connection data
    db_host = 'localhost'
    db_user = 'root'
    db_pass = '33422516'
    db_name = 'ads'

    # Connect to the database
    db_connection = pymysql.connect(host = db_host,
                                 user = db_user,
                                 password = db_pass,
                                 db = db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        result = preprocess(download_file(url, download_folder), insert_feed, (db_connection,))
        print( result['ok'], "\t", result['data'][0], "\t", result['data'][1])

    except Exception as e:
        sql = "INSERT INTO fp_exceptions_feeds_in (type, url, msg) VALUES ('%s', '%s', '%s')"
    
        with db_connection.cursor() as cursor:
            cursor.execute( sql % (type(e).__name__, 
                url.replace("'", "\"").replace('"','\\"'), 
                str(e).replace("'", "\"").replace('"','\\"')))

        db_connection.commit()    
    
    db_connection.close()


def run(download_folder, db_connection, num_workers = None, urls = None):

    if not urls:
        urls = get_urls(db_connection, order_by = "RAND()", limit = 5)

    with Pool(processes = num_workers) as pool:
        #results = pool.map_async(process_feed, range(5)).get()
        responses = [pool.apply_async(process_feed, (url, download_folder)) for url in urls]

        for res in responses:
            res.get()

if __name__ == '__main__':

    # Number of process to be create
    num_workers = cpu_cpu_count() * 2

    # Folder to save the feeds
    feeds_folder = "./feeds"

    # Database connection data
    db_host = 'localhost'
    db_user = 'root'
    db_pass = '33422516'
    db_name = 'ads'

    # Connect to the database
    conn = pymysql.connect(host = db_host,
                                 user = db_user,
                                 password = db_pass,
                                 db = db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)


    urls = [
        'http://fotoautos.com/xml/trovit/trovit_br.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_ec.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_bo.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_co.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_mx.xml'
        ,'http://fotoautos.com/xml/trovit/trovit_pe.xml'
    ]
    
    #urls = ['http://allhouses.com.br/feeds/trovit']
    #urls = ['http://www.hispacasas.com/views/nl/admin/xml/immo_xml/trovit.xml']
    run("./feeds", conn, num_workers = num_workers, urls = urls)

