import requests
import re
import pandas as pd
import urllib.parse
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


def get_products(token, api, env, dfs=None, page_count=1, since_id='', date_updated='', log=False):
    """Download product data from either the Channel or Product API"""

    # Initialise function variables
    if dfs is None:
        dfs = []
    header = {'Authorization': token}

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
        elif payload.status_code == 404:  # api call returns 404 i.e. end of product list
            process_message('Creating dataframe')
            df4 = pd.concat(dfs)
            return df4
        else:  # if api call returns 503 or other error
            process_message('API error...retrying')
            get_products(token, api, env, dfs, page_count, since_id, date_updated, log)

        page_count += 1


def get_parent_products(token, env, child_id, dfs=None, id_only=True, log=False):
    """Take a list of child products and return their parent data into a dataframe"""

    # Initialise the function variables
    primary_id = "primaryId"
    prod_endpoint = "https://pimber.ly/api/v2.2/products/"
    sb_endpoint = "https://sandbox.pimber.ly/api/v2.2/products/"
    extend_response = "/parents?extendResponse=1&attributes=*"
    parent = "/parents"
    if dfs is None:
        dfs = []
    header = {'Authorization': token}

    # Create the endpoint
    if env == "Production" and not id_only:
        base_url = prod_endpoint + primary_id + extend_response
    elif env == "Sandbox" and not id_only:
        base_url = sb_endpoint + primary_id + extend_response
    elif env == "Production" and id_only:
        base_url = prod_endpoint + primary_id + parent
    elif env == "Sandbox" and id_only:
        base_url = sb_endpoint + primary_id + parent

    # Ensure all item ids are properly url encoded strings
    child_id = [str(i) for i in child_id]
    child_id = [urllib.parse.quote(i, safe='') for i in child_id]

    # Iterate through the items retaining the loop counter in case of error and need to restart
    for c, i in enumerate(child_id):

        url = re.sub("primaryId", i, base_url)
        payload = requests.get(url, headers=header)

        if payload.status_code == 200:
            df1 = payload.json().get('data')
            df2 = pd.json_normalize(df1)  # unpack nested dictionaries i.e. list attributes
            df3 = pd.melt(df2, id_vars='primaryId')
            df3['primaryId'] = df3.primaryId.astype(str)  # ensure primaryId is always a string
            df3['itemId'] = urllib.parse.unquote(i)
            dfs.append(df3)
        else:  # if api call returns 503 or other error
            process_message('API error...retrying')
            child_id = child_id[c:]
            child_id = [urllib.parse.unquote(i) for i in child_id]
            get_parent_products(token, env, child_id, dfs, id_only, log)

        # Output an optional status message to the console
        if log:
            process_message(i + " of " + str(len(child_id)) + " | "
                            + primary_id + " | Status: " + str(payload.status_code))

    process_message('Creating dataframe')
    df4 = pd.concat(dfs)
    return df4


if __name__ == '__main__':
    process_header('Getting Products')
    df = get_products(token='', api='Product', env='Production', since_id='', date_updated='', log=True)
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
