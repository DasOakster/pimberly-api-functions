import pimberlyFunctions as pim
import json


def read_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)


crd = read_json('config.json')

# df1 = pim.get_products(token=crd.get('token'), api=crd.get('api'), env=crd.get('env'), since_id=crd.get('since_id'),
#                        date_updated=crd.get('date_updated'), log=True)

items = ['894096938XLG', 813037900076]

df1 = pim.get_parent_products(token=crd.get('token'), env=crd.get('env'), child_id=items, id_only=True, log=True)
