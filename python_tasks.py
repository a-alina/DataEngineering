import requests
import json
import random
import pandas as pd
import os
import numpy as np
import re



def convert():
    """Converting from json to pandas"""
    with open('/opt/airflow/dags/kym.json') as data:
        memes = pd.read_json(data)

    memes.to_csv('/opt/airflow/dags/kym.csv', index=False)


def non_memes():
    """Removing non meme instances"""
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    memes = memes[memes['category'] == 'Meme'] 
    memes.to_csv('/opt/airflow/dags/kym.csv', index=False)

def date_conv():
    """Converting string to datetime"""
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    memes = memes[memes['added'].notna()] # droping nan
    memes['added'] = pd.to_datetime(memes['added'], unit='s') # transforming to datatetime
    memes.to_csv('/opt/airflow/dags/kym.csv', index=False)

def dropping():
    """Droping features and duolicates (leaving only last added"""
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    memes.drop(['category','content', 'ld', 'additional_references', 'last_update_source'], inplace=True, axis=1)
    memes = memes[memes['added'].notna()]
    memes = memes.sort_values('added').drop_duplicates(subset=['title'], keep='last')
    memes.to_csv('/opt/airflow/dags/kym.csv', index=False)

def feature_extraction():
    """Feature extraction"""
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')

    memes['description'] = memes['meta'].apply(lambda x: extract_description(x))
    memes = memes[memes['description'].notna()]

    memes['height'] = memes['meta'].apply(lambda x: extract_height(x))

    memes['width'] = memes['meta'].apply(lambda x: extract_width(x))

    memes.drop('meta', inplace=True, axis=1)

    memes['origin'] = memes['details'].apply(lambda x: extract_origin(x))

    memes['status'] = memes['details'].apply(lambda x: extract_status(x))
    

    memes['year'] = memes['details'].apply(lambda x: extract_year(x))

    memes = memes[memes['year'].notna()]
    memes['year'] = memes['year'].astype(int)

    memes.drop('details', inplace=True, axis=1)
    memes.to_csv('/opt/airflow/dags/kym.csv', index=False)


def search_clean():
    """Cleaning search_words feature """
    memes = pd.read_csv('/opt/airflow/dags/kym.csv')
    clean = re.compile(r'[^\w,]+')
    memes['search_keywords'] = memes['search_keywords'].apply(lambda row: ','.join(row) if type(row) == list else row)
    memes['search_keywords'] = memes['search_keywords'].apply(lambda row: clean.sub(' ', row) if type(row) == str else row)
    memes.to_csv('/opt/airflow/dags/kym.csv', index=False)


def extract_description(row):
    row = eval(row)
    try:
        value = row['description']
    except (KeyError, ValueError):
        value = np.nan
    return value


def extract_height(row):
    row = eval(row)
    try:
        value = int(row['og:image:height'])
    except (KeyError, ValueError):
        value = np.nan
    return value


def extract_width(row):
    row = eval(row)
    try:
        value = int(row['og:image:width'])
    except (KeyError, ValueError):
        value = np.nan
    return value

def extract_origin(row):
    row = eval(row)
    try:
        value = row['origin']
    except (KeyError, ValueError):
        value = np.nan
    return value

def extract_status(row):
    row = eval(row)
    try:
        value = row['status']
    except (KeyError, ValueError):
        value = np.nan
    return value

def extract_year(row):
    row = eval(row)
    try:
        value = int(row['year'])
    except (KeyError, TypeError):
        value = np.nan
    return value

