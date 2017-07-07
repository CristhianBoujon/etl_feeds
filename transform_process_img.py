import argparse
from multiprocessing import Pool, cpu_count
from feed_process.models.db import DBSession
from feed_process.models import TempAdImage
from feed_process.tools.downloader import download_file
from feed_process import LOG_FOLDER, DOWNLOAD_FOLDER, FP_ENV
from feed_process.main_transform import run_img
from send_email import send_email 
import traceback
from multiprocessing import cpu_count

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--tempids', help='TempAd ids separated by comma. E.g: --ids=1,2,3')
        args = parser.parse_args()

        ids = args.tempids.split(",") if args.tempids else None
        run_img(tempids = ids, num_workers = cpu_count() * 2)

    except Exception as e:

        tb = traceback.format_exc()
        email_body = "Exception type: {0}\nEnvironment: {3} \n\n\n {2} \n\n\n{1}".format(type(e).__name__, tb, "Transform Image Process", FP_ENV)
        send_email("Feed Process Error " + FP_ENV, email_body)