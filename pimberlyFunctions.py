import requests
import re
import pandas as pd
from datetime import datetime

""" This module contains functions that use the Pimberly API  to download and upload product data
"""


def process_header(message_string):
    """Take a string and print a formatted message to the console to provide a log file in Cron"""
    print("-" * 125)
    print(message_string)
    print("-" * 125 + "\n")


def process_sub_header(message_string):
    """Take a string and print a formatted message to the console to provide a log file in Cron"""
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print("-" * 8 + ">  " + message_string + " " * (106 - len(message_string)) + current_time)


def process_message(message_string):
    """Take a string and print a formatted message to the console to provide a log file in Cron"""
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(" " * 8 + "   Message:" + message_string + " " * (98 - len(message_string)) + current_time)


""" Pimberly API Functions"""


def set_product_endpoint(page_count, since_id, api, env, date_updated):
    """Build the endpoint based on environment, page number and date filter ="""
    url = ''

    # Channel endpoints
    if env == "Sandbox" and api == "Channel":
        url = "https://sandbox.pimber.ly/api/v2.2/products" + since_id

    if env == "Production" and api == "Channel":
        url = "https://pimber.ly/api/v2.2/products" + since_id

    # Product endpoints
    if page_count == 1:
        if env == "Sandbox" and api == "Product":
            url = "https://sandbox.pimber.ly/api/v2.2/products" + "?extendResponse=1&attributes=*"
        if env == "Production" and api == "Product":
            url = "https://pimber.ly/api/v2.2/products" + "?extendResponse=1&attributes=*"
    else:
        if env == "Sandbox" and api == "Product":
            url = "https://sandbox.pimber.ly/api/v2.2/products" + since_id + "&extendResponse=1&attributes=*"
        if env == "Production" and api == "Product":
            url = "https://pimber.ly/api/v2.2/products" + since_id + "&extendResponse=1&attributes=*"

    # Date Updated endpoints
    if date_updated and page_count == 1:
        base_url = "https://pimber.ly/api/v2.2/products"
        date_filter = "?filters={\"dateUpdated\":{\"$gte\":\"date_updatedT00:00:0.000Z\"}}"
        date_filter = re.sub("date_updated", date_updated, date_filter)
        url = base_url + date_filter

    elif date_updated and page_count > 1:
        base_url = "https://pimber.ly/api/v2.2/products"
        date_filter = since_id + "&filters={\"dateUpdated\":{\"$gte\":\"date_updatedT00:00:0.000Z\"}}"
        date_filter = re.sub("date_updated", date_updated, date_filter)
        url = base_url + date_filter

    return url


def get_products(token='', api='', env='Production', since_id='', date_updated='', log=False):
    """Download product data from either the Channel or Product API"""

    # Initialise function variables
    page_count = 1
    header = {'Authorization': token}
    dfs = []

    # Download product data until call returns 404
    if date_updated:
        process_message("Downloading all Pimberly products updated since " + date_updated)
    elif api == 'Channel':
        process_message("Downloading Pimberly product data from the Channel")
    elif api == 'Product':
        process_message("Downloading all Pimberly product data")

    while True:
        if log:
            process_message("Downloading page " + str(page_count) + " | sinceId = " + since_id)

        endpoint = set_product_endpoint(page_count, since_id, api, env, date_updated)
        payload = requests.get(endpoint, headers=header)

        if payload.status_code == 200:
            since_id = '?sinceId=' + payload.json().get('maxId')
            df1 = payload.json().get('data')
            df2 = pd.json_normalize(df1)  # unpack nested dictionaries i.e. list attributes
            df3 = pd.melt(df2, id_vars='primaryId')
            df3['primaryId'] = df3.primaryId.astype(str)  # ensure primaryId is always a string
            dfs.append(df3)
        else:
            df4 = pd.concat(dfs)
            return df4

        page_count += 1


if __name__ == '__main__':
    process_header('Getting Products')
    df1 = get_products(token='', api='Product', env='Production', since_id='', date_updated='', log=True)
    # print(set_product_endpoint(1, "?sinceId=''", 'Channel', 'Sandbox', ''))
    # print(set_product_endpoint(1, "?sinceId=''", 'Channel', 'Production', ''))
    # print(set_product_endpoint(10, "?sinceId=123456789", 'Channel', 'Sandbox', ''))
    # print(set_product_endpoint(10, "?sinceId=987654321", 'Channel', 'Production', ''))
    # print(set_product_endpoint(1, "?sinceId=''", 'Product', 'Sandbox', ''))
    # print(set_product_endpoint(1, "?sinceId=''", 'Product', 'Production', ''))
    # print(set_product_endpoint(10, "?sinceId=123456789", 'Product', 'Sandbox', ''))
    # print(set_product_endpoint(10, "?sinceId=987654321", 'Product', 'Production', ''))
    # print(set_product_endpoint(1, "?sinceId=''", 'Product', 'Sandbox', '10/8/21'))
    # print(set_product_endpoint(1, "?sinceId=''", 'Product', 'Production', '12/8/21'))
    # print(set_product_endpoint(10, "?sinceId=123456789", 'Product', 'Sandbox', '10/8/21'))
    # print(set_product_endpoint(10, "?sinceId=987654321", 'Product', 'Production', '12/8/21'))
    # process_sub_header('This is a Process sub header')
    # process_sub_header('This is a different Process sub header')
    # process_message('And this is a message')
    # process_message('And this is another message')
