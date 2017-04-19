from feeds_downloader import download, _download_file
import time

urls = [
'http://fotoautos.com/xml/trovit/trovit_br.xml'
,'http://fotoautos.com/xml/trovit/trovit_ec.xml'
,'http://fotoautos.com/xml/trovit/trovit_bo.xml'
,'http://fotoautos.com/xml/trovit/trovit_co.xml'
,'http://fotoautos.com/xml/trovit/trovit_mx.xml'
,'http://fotoautos.com/xml/trovit/trovit_pe.xml'

];


start = time.time()

download(urls, "feeds", num_workers = 4)
end = time.time()

print ("Parallel process: %.2f seconds" % (end - start))



start = time.time()

for url in urls:
    _download_file((url, "feeds"))

end = time.time()

print( "Sequential process: %.2f seconds" % (end - start))