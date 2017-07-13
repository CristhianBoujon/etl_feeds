import requests
import time
from multiprocessing import Pool, cpu_count

class Loader:
    def load(self, data):
        raise NotImplementedError("La función load() was not implemented yet for " + type(self).__name__)


class ApiLoader(Loader):
    def __init__(self, server, protocol = "http", user = None, password = None):
        
        if user and password:
            self.url = "{protocol}://{user}:{password}@{server}/".format(
                protocol = protocol,
                user = user, 
                password = password, 
                server = server)
        else:
            self.url = "{protocol}://{server}/".format(
                protocol = protocol, server = server)


class AnunicoApiLoader(ApiLoader):

    def __init__(self, server, protocol = "http", user = None, password = None):
        super().__init__(server, protocol, user, password)
        self.__location_cache = {}
        self.__country_cache = {}
        self.__subcategory_cache = {}

    def batch_load(self, ads_data):
        data = {"ads": ads_data}
        response = requests.post(self.url + "batch-ad-from-fp", json = data)

        if(response.ok):
            response = response.json()
            return [{
                "id": resp_ad["sitioId"], 
                "ad_id": (resp_ad["adid"] or None), 
                "error_message": (resp_ad["errorMessage"] or None) } for resp_ad in response]

        else:
            error_message = "ERROR {0} {1}".format(str(response.status_code), str(response.content))
            return [{
                "id": ad_data["sitioId"], 
                "ad_id": None, 
                "error_message": error_message } for ad_data in ads_data]


    def _load(self, serialized_ad):
        response = requests.post(self.url + "ad-from-fp", 
                data = serialized_ad["data"],
                files = [("img_" + str(i), open(serialized_ad["images"][i], "rb")) for i in range(len(serialized_ad["images"]))])

        if(response.ok):
            resp_ad = response.json()
            return {
                    "id": resp_ad["sitioId"], 
                    "ad_id": (resp_ad["adid"] or None), 
                    "error_message": (resp_ad["errorMessage"] or None) }
        else:
            error_message = "ERROR {0} {1}".format(str(response.status_code), str(response.content))
            return {
                    "id": serialized_ad["data"]["sitioId"], 
                    "ad_id": None, 
                    "error_message": error_message }

    def load(self, serialized_ads):
        with Pool(cpu_count() * 2) as pool: 
            return pool.map(self._load, serialized_ads)


    def get_location(self, location_id):
        
        if location_id not in self.__location_cache:
            response = requests.get(self.url + 'location', params = {"locationid": location_id})
            response.raise_for_status() # Raise an exception if status code is not 200
            self.__location_cache[location_id] = dict(response.json())

        return self.__location_cache[location_id]

    def get_country(self, country_id):

        if country_id not in self.__country_cache:
            response = requests.get(self.url + 'country/{0}'.format(country_id))
            response.raise_for_status() # Raise an exception if status code is not 200
            self.__country_cache[country_id] = dict(response.json()[0])

        return self.__country_cache[country_id]

    def get_subcategory(self, subcategory_id):

        if subcategory_id not in self.__subcategory_cache:
            response = requests.get(self.url + 'subcat/{0}'.format(subcategory_id))
            response.raise_for_status() # Raise an exception if status code is not 200
            #print(subcategory_id)
            self.__subcategory_cache[subcategory_id] = dict(response.json()[0])

        return self.__subcategory_cache[subcategory_id]
