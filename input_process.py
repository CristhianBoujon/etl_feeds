from feed_process.main_input import run
from feed_process import FP_ENV
from multiprocessing import cpu_count
from send_email import send_email 
import sys, traceback
import argparse

if __name__ == "__main__":

    try:

        parser = argparse.ArgumentParser()
        parser.add_argument('--ids', help='feed ids separated by comma. E.g: --ids=1,2,3')
        parser.add_argument('--urls', help='feed urls separated by comma. E.g: --urls=www.foo.com,www.bar.com')
        args = parser.parse_args()

        # Number of process to be create
        num_workers = cpu_count() * 2

        if(args.ids):
            feed_ids = args.ids.split(',')
            run(feed_ids = feed_ids, num_workers = num_workers)

        elif(args.urls):
            urls = args.urls.split(',')
            run(urls = urls, num_workers = num_workers)
        
        else:
            run(num_workers = num_workers)

    except Exception as e:
        import traceback

        tb = traceback.format_exc()
        email_body = "Exception type: {0}\nEnvironment: {3} \n\n\n {2} \n\n\n{1}".format(type(e).__name__, tb, "Input Process", FP_ENV)        
        send_email("Feed Process Error " + FP_ENV, email_body)
