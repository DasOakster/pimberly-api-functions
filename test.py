import pimberlyFunctions as pim
import json


def read_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


crd = read_json('config.json')

df1 = pim.get_products(token=crd.get('token'), api=crd.get('api'), env=crd.get('env'), since_id='', date_updated='',
                       log=True)
