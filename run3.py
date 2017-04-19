
file = "feeds/export_net_linio_com_20170418103506186869.xml"
file2 = open("feeds/extract_linio.xml", "w")

file2.write('<?xml version="1.0" encoding="UTF-8"?>')
file2.write("<trovit>")

with open(file) as fp:
    for i, line in enumerate(fp):
        if i >= 150516 and i <= 150520:
            file2.write(line)
#            print (line)
file2.write("</trovit>")
file2.close()