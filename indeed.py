
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from random import randint
from datetime import datetime
from time import sleep

# Setting User Agent Header as global variable
headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0'}

indeed_jobs = {}
indeed_dataset = []

def get_list_of_jobs_from_pole_emploi_open_data():
    """Read jobs list from French Pole Emploi public Open Data file"""
    # indexing will be with Code OGR column because it is the supposedly unique Identifier of each job
    # downloaded from official french Pole Emploi opendata at the following url :
    # https://www.pole-emploi.org/opendata/repertoire-operationnel-des-meti.html?type=article

    df = pd.read_csv('ROME_ArboPrincipale.csv', sep=';', header=None)

    df = df.drop(df.columns[[0, 1, 2]], axis=1)  # dropping of first columns

    df = df.rename(columns={3: 'JOB_NAME', 4: 'JOB_ID'})

    df = df.drop(0, axis=0)  # drop of first empty row

    columns_titles = ['JOB_ID', 'JOB_NAME']
    df = df.reindex(columns=columns_titles)  # reindexing of columns

    df = df.set_index('JOB_ID')  # set index label

    df = df.drop(' ')  # dropping row with index labels equal to ' ' because it means that it is not a job name

    if df.index.is_unique:
        print("Each job has a unique ID")
    else:
        print("Necessary to drop duplicates")

    # Comment : job_id is unique

    # Cleaning the jobs names in order to help the seach in Indeed, transforming the job title in unisex because
    # Indeed use the "H/F" format for french market job for male/female
    # following lines are examples of the main transformations done by the followed regex transformation
    # Chauffeur / Chauffeuse de machines agricoles --->>> Chauffeur de machines agricoles
    # Débardeur / Débardeuse --->>> Débardeur
    # Arboriste grimpeur / grimpeuse --->>> Arboriste grimpeur
    # Élagueur-botteur / Élagueuse-botteuse --->>> Élagueur-botteur
    # Débardeur forestier / Débardeuse forestière --->>> Débardeur forestier
    # Peintre-fileur-décorateur / Peintre-fileuse-décoratrice en céramique --->>> Peintre-fileur-décorateur en céramique
    # Ingénieur / Ingénieure halieute --->>> Ingénieur halieute

    regex = r'(^\w+|\w+\s\w+|\w+-\w+|\w+-\w+-\w+)(\s/\s)(\w+|\w+\s\w+|\w+-\w+|\w+-\w+-\w+)(\s|$)'

    df['JOB_NAME'] = df['JOB_NAME'].apply(lambda x: re.sub(regex, r'\1 ', x))

    print(f'Number of job titles in France : {df.shape[0]}')

    # jobs list registering in global dictionary
    global indeed_jobs
    indeed_jobs = df.to_dict()  # dicts are unordered/arbitrary but pandas does ascending key automatic indexing


def checking_indeed_scraping_allowance():
    """Check the the indeed scraping page possibility"""
    # Check the robot.txt file of Indeed website
    robot_res = os.popen('curl https://www.indeed.com/robots.txt').read()
    indeed_auth_res = {'Allow': [], 'Disallow': []}

    for line in robot_res.split("\n"):
        if line.startswith('Allow'):
            indeed_auth_res['Allow'].append(line.split(': ')[1])
        elif line.startswith('Disallow'):
            indeed_auth_res['Disallow'].append(line.split(': ')[1])

    if '/job?' not in indeed_auth_res['Disallow']:
        print('Scraping is allowed for the following url link : https://fr.indeed.com/jobs?')


def get_indeed_url(position, location):
    """return the indeed url of a search query with a job position and location given as parameters"""
    pattern = 'https://fr.indeed.com/jobs?q={}&l={}'
    url = pattern.format(position, location)
    return url


def format_post_date(test):
    if ("Publiée à l'instant" in test) or ("Aujourd'hui" in test):
        return str(0)
    elif re.findall(r'\d+', test):
        temp = re.findall(r'\d+', test)
        res = tuple(map(int, temp))[0]
        return str(res)
    else:
        return None


def format_location(test):
    if re.findall(r'\d+', test):
        temp = re.findall(r'\d+', test)
        res = tuple(map(int, temp))[0]
        return str(res)
    else:
        return 'OTHERS'


def get_jobs_from_indeed():
    """Get jobs in France from indeed and save it in a .csv file"""
    df = pd.DataFrame(indeed_jobs)
    df.index.name = 'JOB_ID'

    extract_date = datetime.today().strftime('%d-%m-%Y')

    df['NUMBER_OF_JOBS'] = 0
    df['EXTRACT_DATE'] = extract_date

    loc_debug = False

    for x in range(0, 31):
        # counter of jobs for each date (0: today and minutes ago; 1 to 29, and 30/30+ together)
        df['POST_DATE_'+str(x)] = 0

    for y in range(1, 96):
        # counter of jobs for each metropolitan department (1 to 95)
        df['DEPT_'+str(y)] = 0

    df['DEPT_OTHERS'] = 0  # counter for departments outside of metropolitan France (971, 972, 973, 974, 976)

    print(df.head(5))

    for index, rows in df.iterrows():

        url = get_indeed_url(rows[0], 'France')  # create the url while passing in the position and location.

        cpt = 0  # counter of number of jobs

        while True:
            print(url)
            delay = randint(5, 20)
            sleep(delay)  # sleep to help avoid Indeed scrapping blocking

            response = requests.get(url, headers=headers)
            print(response.status_code)
            print(response.reason)

            soup = BeautifulSoup(response.text, 'html.parser')
            cards = soup.find_all('div', 'jobsearch-SerpJobCard')

            for card in cards:
                post_date = format_post_date(card.find('span', 'date').text.strip())

                location = format_location(card.find('div', 'recJobLoc').get('data-rc-loc'))

                df.loc[index, 'POST_DATE_'+post_date] += 1

                df.loc[index, 'DEPT_'+location] += 1

            cpt += len(cards)

            try:
                url = 'https://fr.indeed.com' + soup.find('a', {'aria-label': 'Suivant'}).get('href')
                # detect the next page when no next page break the loop

            except AttributeError:
                print("No Next Page or Blocked or HTML tags have been updated")
                break

        df.loc[index, "NUMBER_OF_JOBS"] = cpt

        print(f"number of jobs of ({index}/{rows[0]}) is {cpt}")

        if loc_debug:
            print("Leave the loop for debug purposes")
            break

    df.to_csv('dataset.csv', sep=';')  # finally create the dataset


