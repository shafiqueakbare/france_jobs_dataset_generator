#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from random import randint
from datetime import datetime
from time import sleep
import concurrent.futures


# Setting User Agent Header
headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0'}


def get_list_of_jobs_titles_from_open_data():
    """
    Read jobs titles list from French Pole Emploi Public Insitutuion's Open Data file
    downloaded from https://www.pole-emploi.org/opendata/repertoire-operationnel-des-meti.html?type=article
    where the supposedly unique Identifier is the code OGR

    Arguments : None

    Return : Pandas dataframe of JOB_ID/JOB_NAME
    """

    df = pd.read_csv('ROME_ArboPrincipale.csv', sep=';', header=None)

    df = df.drop(df.columns[[0, 1, 2]], axis=1)  # dropping of first columns

    df = df.rename(columns={3: 'JOB_NAME', 4: 'JOB_ID'})

    df = df.drop(0, axis=0)  # drop of first empty row

    columns_titles = ['JOB_ID', 'JOB_NAME']
    df = df.reindex(columns=columns_titles)  # reindexing of columns

    df = df.set_index('JOB_ID')  # set index label

    df = df.drop(' ')  # dropping row with index labels equal to ' ' because it means that it is not a job name

    if df.index.is_unique:
        pass
    else:
        print("Necessary to choose which jobs titles to drop in open data")
        return None

    df = df.sort_index()  # sort index in ascending order

    df['JOB_NAME'] = df['JOB_NAME'].apply(lambda x: transformation(x))

    print(f'Number of job titles in France : {df.shape[0]}')

    return df


def transformation(job_title):
    """
    Transform a job title in a unisex job title
    Here are examples of main transformations :
    - Chauffeur / Chauffeuse de machines agricoles --->>> Chauffeur de machines agricoles
    - Débardeur / Débardeuse --->>> Débardeur
    - Arboriste grimpeur / grimpeuse --->>> Arboriste grimpeur
    - Élagueur-botteur / Élagueuse-botteuse --->>> Élagueur-botteur
    - Débardeur forestier / Débardeuse forestière --->>> Débardeur forestier
    - Peintre-fileur-décorateur / Peintre-fileuse-décoratrice en céramique --->>> Peintre-fileur-décorateur en céramique
    - Ingénieur / Ingénieure halieute --->>> Ingénieur halieute
    - Conducteur livreur installateur / Conductrice livreuse installatrice --->>> Conducteur livreur installateur
    - Accessoiriste
    - Accueillant familial / Accueillante familiale auprès d'adultes

    Arguments : job title

    Return : modified job title
    """

    try:
        left, right = map(str.strip, job_title.split('/'))
        start = left.count(' ')
        right = ' '.join(right.split()[start+1:])
        return left + (' ' + right if right else '')

    except ValueError:
        return job_title


def get_proxies():
    """
    Retrieve proxy list from https://free-proxy-list.net/
    Arguments : None

    Return : proxies list
    """
    r = requests.get('https://free-proxy-list.net/')
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find('tbody')
    proxies = []
    for row in table:
        if row.find_all('td')[4].text =='elite proxy':
            proxy = ':'.join([row.find_all('td')[0].text, row.find_all('td')[1].text])
            proxies.append(proxy)
        else:
            pass
    return proxies


def extract_proxy(proxy):
    """
    Change the url to https://httpbin.org/ip that doesnt block anything
    Arguments : proxy

    Return : proxy
    """
    try:

        r = requests.get('https://httpbin.org/ip', headers=headers, proxies={'http' : proxy,'https': proxy}, timeout=1)
        print(r.json(), r.status_code)
    except:
        print("Skipping. Connnection error")
    return proxy


def checking_scraping_allowance(robot_file, url_sub_category):
    """
    Check if it is allowed to scrap the subcategory of a url

    Arguments : url to robot.txt file
                url subcategory

    Return : Boolean True/False
    """
    robot_res = os.popen(f'curl {robot_file}').read()
    auth_res = {'Allow': [], 'Disallow': []}

    for line in robot_res.split("\n"):
        if line.startswith('Allow'):
            auth_res['Allow'].append(line.split(': ')[1])
        elif line.startswith('Disallow'):
            auth_res['Disallow'].append(line.split(': ')[1])

    if url_sub_category not in auth_res['Disallow']:
        return True
    else:
        return False


def get_indeed_url(position, location):
    """
    Gives the indeed url of a search query with a job position and location given as parameters

    Parameters : Job Position
                 Job Location

    Return : Appropriate Indeed URL
    """
    pattern = 'https://fr.indeed.com/jobs?q={}&l={}'

    url = pattern.format(position, location)

    return url


def format_indeed_post_date(post_date):
    """
    Format a Posted date to a numeric string of the recorded days

    Arguments : posted date string

    Return : Numeric string
    """
    if ("Publiée à l'instant" in post_date) or ("Aujourd'hui" in post_date):
        return str(0)
    else:
        temp = re.findall(r'\d+', post_date)
        res = tuple(map(int, temp))[0]
        return str(res)


def format_indeed_location(location):
    """
    Format a location to a numeric string representing the appropriate french department

    Arguments : location string

    Return : Numeric string
    """
    if re.findall(r'\d+', location):
        temp = re.findall(r'\d+', location)
        res = tuple(map(int, temp))[0]
        return str(res)
    else:
        return 'OTHERS'


def create_jobs_dataset_from_indeed(jobs_titles):
    """
    Generate a dataset.csv file (and a continuous residual_dataset.csv file) of all jobs titles with
    a record of the last 30 days of the number of jobs in all french departments

    Arguments : Pandas dataframe of JOB_ID/JOB_NAME

    Return : True/False if the dataset has been correctly generated
    """

    if checking_scraping_allowance('https://www.indeed.com/robots.txt', '/job?'):
        pass
    else:
        print("Not allowed to scrap this website")
        return False

    proxylist = get_proxies()
    print(proxylist)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(extract_proxy, proxylist)

    df = pd.DataFrame(jobs_titles)

    extract_date = datetime.today().strftime('%d-%m-%Y')

    df['NUMBER_OF_JOBS'] = 0
    df['EXTRACT_DATE'] = extract_date

    for x in range(0, 31):
        # counter of jobs for each date (0: today and minutes ago; 1 to 29, and 30/30+ together)
        df['POST_DATE_'+str(x)] = 0

    for y in range(1, 96):
        # counter of jobs for each metropolitan department (1 to 95)
        df['DEPT_'+str(y)] = 0

    df['DEPT_OTHERS'] = 0  # counter for departments outside of metropolitan France (971, 972, 973, 974, 976)

    # Residual dataset in case a need to stop the dataset generation and retrieve only the first lines of the dataset
    df_residual = pd.DataFrame(columns=df.columns)
    df_residual.to_csv('residual_dataset.csv', sep=';', index_label='JOB_ID')

    for index, rows in df.iterrows():

        url = get_indeed_url(rows[0], 'France')  # create the url while passing in the position and location.

        while True:
            print(url)
            delay = randint(5, 10)
            sleep(delay)  # sleep to help avoid Indeed scraping blocking

            response = requests.get(url, headers=headers)
            # print(response.status_code)
            # print(response.reason)

            soup = BeautifulSoup(response.text, 'html.parser')

            cards = soup.find_all('div', 'jobsearch-SerpJobCard')

            for card in cards:
                post_date = format_indeed_post_date(card.find('span', 'date').text.strip())

                location = format_indeed_location(card.find('div', 'recJobLoc').get('data-rc-loc'))

                df.loc[index, 'POST_DATE_'+post_date] += 1

                df.loc[index, 'DEPT_'+location] += 1

            df.loc[index, "NUMBER_OF_JOBS"] += len(cards)

            try:
                url = 'https://fr.indeed.com' + soup.find('a', {'aria-label': 'Suivant'}).get('href')
                # detect the next page when no next page break the loop

            except AttributeError:
                print("No Next Page or Blocked or HTML tags have been updated by Indeed")
                break

        print(f"number of jobs of ({index}/{rows[0]}) is {df.loc[index, 'NUMBER_OF_JOBS']}")

        rows.to_frame().T.to_csv('residual_dataset.csv', mode='a', sep=';', header=False)

    df.to_csv('dataset.csv', sep=';')  # finally create the full dataset

    return True


