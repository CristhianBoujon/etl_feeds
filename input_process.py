from feed_process.main_input import run
from feed_process import FP_ENV
from multiprocessing import cpu_count
from send_email import send_email 
import sys, traceback

if __name__ == "__main__":

    try:
        # Number of process to be create
        num_workers = cpu_count() * 2
        urls = [
            'http://dubai.classonet.com/classonet-jobs-trovit.xml'
            ,'http://www.avisos-chile.com/feeds/trovit/jobs/'
            ,'http://www.avisos-colombia.com/feeds/trovit/jobs/'
            ,'http://www.bachecalavoro.com/export/Trovit_anunico.xml'
            ,'http://www.indads.in/feeds/trovit/jobs/'
            ,'http://www.jobsxl.com/xml/trovit.php'
            ,'http://www.reclutamos.com/trovit_chile-1.xml'
            ,'http://www.tablerotrabajo.com.co/export/Trovit_anunico.xml'
            ,'http://www.tablerotrabajo.com.mx/export/Trovit_anunico.xml'
            ,'http://www.tablerotrabajo.com/export/Trovit_anunico.xml'
            ,'http://www.toditolaboral.com/trovit_empleos_argentina.xml'
            ,'http://www.toditolaboral.com/trovit_empleos_chile.xml'
            ,'http://www.toditolaboral.com/trovit_empleos_colombia.xml'
            ,'http://www.toditolaboral.com/trovit_empleos_ecuador.xml'
            ,'http://www.toditolaboral.com/trovit_empleos_espana.xml'
            ,'http://www.toditolaboral.com/trovit_empleos_mexico.xml'
            ,'http://www.toditolaboral.com/trovit_empleos_peru.xml'
            ,'http://www.toditolaboral.com/trovit_empleos_venezuela.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_ARG.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_AUS.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_AUT.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_BLZ.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_BRA.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_CAN.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_CHL.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_COL.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_CUB.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_ECU.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_ESP.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_HND.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_HTI.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_IND.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_MEX.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_NIC.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_PER.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_PRT.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_TTO.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_UGY.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_USA.xml'
            ,'https://www.tiptopjob.com/joblist/TTJTrovit_VEN.xml'
        ]

        run(num_workers = num_workers, urls = urls)

    except Exception as e:
        import traceback

        tb = traceback.format_exc()
        email_body = "Exception type: {0}\nEnvironment: {3} \n\n\n {2} \n\n\n{1}".format(type(e).__name__, tb, "Input Process", FP_ENV)        
        send_email("Feed Process Error " + FP_ENV, email_body)