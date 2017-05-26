import requests
from db import DBSession
from feeds_model import TempAd


#temp_ad = DBSession.query(Temp.id).filter(Temp.id > last_id, status = "P").order_by(RawAd.id).limit(max_size).all()

temp_ad = DBSession.query(TempAd).get(1196412)

#print(temp_ad.properties)

properties = {prop_name: prop.value for prop_name, prop in temp_ad.properties.items()} 

#properties.pop("date")
properties.pop("url")
print(properties)

resp = requests.post("http://user:kljbkUO&%@178.32.159.68/ad-from-fp", data = properties)
print("-----------------------------------------------------------")
print(resp.status_code, resp.content)



#data = {
#    'username': "emaildecristhian@gmail.com",
#    'passwd': "unaco123",
#    'email': "emaildecristhian@gmail.com", 
#    "roles":['ROLE_LOGGED_USER']}

#resp = requests.post("http://user:kljbkUO&%@178.32.159.68/user", data = data)
#resp = requests.get("http://user:kljbkUO&%@178.32.159.68/user/tacgctx@gmail.com",)
#print(resp.content, resp.status_code)


"""
def run(num_workers = None):
    pool = Pool(processes = num_workers)
    raw_ads = []
    last_id = 0
    max_size = 10000
    chunk_size = 1000
    processed = 0

    total_to_be_process = DBSession.query(RawAd).count()

    total_time = 0

    # process will scan all existing RawAd inserted
    while True:

        # We need to chunk result query sinse raw ad is so big
        raw_ads = DBSession.query(Temp.id).filter(RawAd.id > last_id, status = "P").order_by(RawAd.id).limit(max_size).all()

        # If no more RawAds it breks loop
        if not raw_ads:
            break

        raw_ads = [raw_ad[0] for raw_ad in raw_ads]
        last_id = raw_ads[-1]

        chunked_raw_ads = chunk_list(raw_ads, chunk_size)
        
        import time
        start = time.time()
        results = pool.map_async(create_temp_ad, chunked_raw_ads).get()
        end = time.time() - start
        total_time += end
        processed += sum(results)
        print("Processed: {0}/{1} in {2} secs".format(processed, total_to_be_process, end))

if __name__ == "__main__":

    run()

"""
        
        

