import requests
import pandas as pd
import os
from zipfile import ZipFile

class ApiData:
    def get():
        raise NotImplementedError
	
class TorontoData(ApiData):
    base_url = "https://ckan0.cf.opendata.inter.prod-toronto.ca"
    url = base_url + "/api/3/action/package_show"
    
    def __init__(self, endpoint, export_path):
        self.endpoint = endpoint
        self.export_path = export_path

    def generate_link(self):
        package = requests.get(__class__.url, params = {"id": self.endpoint})
        if package.status_code != 200:
            package.raise_for_status()
        for idx, resource in enumerate(package.json()["result"]["resources"]):
            # To get metadata for non datastore_active resources:
            if not resource["datastore_active"]:
                joined_url = __class__.base_url + "/api/3/action/resource_show?id=" + resource["id"]
                resource_metadata = requests.get(joined_url).json()
                return resource_metadata["result"]['url']
    
    def download_zip(self, link, chunk_size=128):
        save_path = os.path.join(self.export_path, os.path.basename(link))
        r = requests.get(link, stream=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)
        with ZipFile(save_path) as zip:
            zip.extractall(self.export_path)

    def get(self):
        link = self.generate_link()
        self.download_zip(link)

class TorontoDataProxy(TorontoData):
    def __init__(self, endpoint, export_path):
        super().__init__(endpoint, export_path)
    
    def get_data(self):
        if not os.path.exists(self.export_path):
            raise FileNotFoundError(self.export_path)
        self.get()
        
TorontoDataProxy("x", "/home/nstuart/Documents/data_export").get_data()


