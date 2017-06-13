from feed_process.main_load import run
from feed_process import FP_ENV
from send_email import send_email 
import sys, traceback

if __name__ == "__main__":

    try:
        run("AnunicoApiLoader")

    except Exception as e:
        import traceback

        tb = traceback.format_exc()
        email_body = "Exception type: {0}\nEnvironment: {3} \n\n\n {2} \n\n\n{1}".format(type(e).__name__, tb, "Load Process", FP_ENV)
        
        send_email("Feed Process Error", email_body)