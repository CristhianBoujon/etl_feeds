import pymysql
import feeds_preprocessor as p
import run2
import time

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


start = time.time()
#run2.run("./feeds", db_connection, urls = ['http://hotelde_std:f6e883b551@www.hotel.de/media/downloads/export/MultiLingualExport/hotelde_standard_export.zip', 'https://export.net.linio.com/ce08db27-6021-4de3-b592-9c30ac9d1f67/api/productdataexport_16?pid=29990&format=xml'])
run2.insert_feed("feeds/export_net_linio_com_20170418162655450995.xml", db_connection)
end = time.time()

print ("Parallel process: %.2f seconds" % (end - start))