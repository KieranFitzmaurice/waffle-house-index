import numpy as np
import scipy.stats as stats
import pandas as pd
import requests
import os
import json
from datetime import datetime
import time

# *** Proxy pool class *** #

class ProxyPool:

    def __init__(self,proxy_list_path):
        """
        param: proxy_list_path: path to list of proxies downloaded form webshare.io
        """
        proxy_list = list(np.loadtxt(proxy_list_path,dtype=str))
        proxy_list = ['http://' + ':'.join(x.split(':')[2:]) + '@' + ':'.join(x.split(':')[:2]) for x in proxy_list]
        proxy_list = [{'http':x,'https':x} for x in proxy_list]

        self.proxy_list = proxy_list
        self.num_proxies = len(self.proxy_list)
        self.random_index = stats.randint(0,self.num_proxies).rvs

    def verify_ip_addresses(self,sleep_seconds=0.0,nmax=10):

        """
        Function to verify that IP address appears as those of proxies
        """
        url = 'https://api.ipify.org/'
        n = np.min([nmax,len(self.proxy_list)])

        for i in range(n):

            res = requests.get(url,proxies=self.proxy_list[i])
            print(res.text,flush=True)
            time.sleep(sleep_seconds)

        return(None)

    def random_proxy(self):
        """
        Function to return a randomly-selected proxy from self.proxy_list
        """

        proxy = self.proxy_list[self.random_index()]

        return(proxy)


# *** Initial setup *** #

def create_folders(companies=['bojangles','dunkin_donuts','wendys']):
    """
    Function to create directory structure for data scraped from company websites

    param: companies: company names
    """

    pwd = os.getcwd()

    folders_to_create = []

    for company in companies:
        folders_to_create.append(f'data/raw/{company}')
        folders_to_create.append(f'data/clean/{company}')

    for folder in folders_to_create:
        folderpath = os.path.join(pwd,folder)
        if not os.path.exists(folderpath):
            os.makedirs(folderpath,exist_ok=True)

    return(None)

# *** Bojangles *** #

def get_bojangles_data(proxypool,sleep_seconds=0.2,random_pause=0.1,increment=50,extra_limit=5,failure_limit=10):

    """
    Scraper to pull data on bojangles restaurants and locations

    param: proxypool: pool of proxies to route requests through
    param: sleep_seconds: number of seconds to wait in between api queries
    param: random_pause: total seconds between queries = sleep_seconds + uniform[0,random_pause]
    param: increment: number of results to get in each api query
    param: extra_limit: number of additional times to query api after reaching the "final" location
    param: failure_limit: number of additional times to query api if initial attempt fails
    """

    results_dict = {}

    offset = 0
    num_extra = 0
    num_failures = 0

    dist = stats.uniform(0,random_pause)

    keepgoing = True
    scraper_issues = False

    while keepgoing:

        url = 'https://liveapi.yext.com/v2/accounts/me/answers/vertical/query'

        params = {'experienceKey':'bojangles-answers',
                  'api_key':'8e126c39edbada8bfd7cf991a1932848',
                  'v': '20190101',
                  'version': 'PRODUCTION',
                  'locale': 'en',
                  'input': '',
                  'verticalKey': 'locations',
                  'limit': increment,
                  'offset': offset,
                  'retrieveFacets': 'true',
                  'facetFilters': '{"address.region":[],"address.city":[]}',
                  'session_id': '0340c0e9-4857-4045-bf9a-386a183d96d1',
                  'sessionTrackingEnabled': 'true',
                  'sortBys': '[]',
                  'referrerPageUrl': 'https://locations.bojangles.com/',
                  'source': 'STANDARD',
                  'jsLibVersion': 'v1.12.4'}

        res = requests.get(url,params=params,proxies=proxypool.random_proxy())

        if res.ok:

            res_dict = res.json()

            num_results = len(res_dict['response']['results'])

            if num_results > 0:

                num_failures = 0

                keystr = f'{offset+1}-{offset + increment}'
                results_dict[keystr] = res_dict.copy()

            else:

                num_extra += 1

            offset += increment

        else:

            num_failures += 1
            scraper_issues = True

        time.sleep(sleep_seconds + dist.rvs())

        if (num_extra > extra_limit) or (num_failures > failure_limit):

            keepgoing = False

    date_str = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    raw_filepath = os.path.join(os.getcwd(),f'data/raw/bojangles/{date_str}_bojangles.json')

    with open(raw_filepath,'w') as f:
        json.dump(results_dict,f,indent=4)
        f.close()

    return(raw_filepath,scraper_issues)

def clean_bojangles_data(raw_filepath,scraper_issues):

    """
    Function to clean data scraped from bojangles web api

    param: raw_filepath: filepath of raw data in .json format
    param: scraper_issues: False if data was scraped without errors; True otherwise
    """


    with open(raw_filepath,'r') as f:
        results_dict = json.load(f)
        f.close()

    fileparts = raw_filepath.split('/')[-1].split('_')
    observation_time = pd.Timestamp(fileparts[0] + ' ' + fileparts[1].replace('-',':'))

    internal_id_list = []
    address_list = []
    state_list = []
    status_list = []
    website_list = []
    phone_list = []
    extra_list = []

    for key in results_dict.keys():
        for entry in results_dict[key]['response']['results']:

            fields = entry['data']

            internal_id = fields['id']

            address = ''

            address += fields['address']['line1']

            if 'line2' in fields['address'].keys():
                address += fields['address']['line2'] + ', '
            else:
                address += ', '

            address += fields['address']['city'] + ', ' + fields['address']['region'] + ' ' + fields['address']['postalCode']

            state = fields['address']['region']

            try:
                if fields['closed'] == True:
                    status = 'closed'
                else:
                    status = 'open'
            except:
                status = 'inconclusive'

            try:
                website = fields['websiteUrl']['url']
            except:
                website = np.nan

            try:
                phone = fields['mainPhone']
            except:
                phone = np.nan

            try:
                extra = 'googlePlaceId : ' + fields['googlePlaceId']
            except:
                extra = np.nan

            address_list.append(address)
            state_list.append(state)
            status_list.append(status)
            internal_id_list.append(internal_id)
            website_list.append(website)
            phone_list.append(phone)
            extra_list.append(extra)

    n_obs = len(address_list)
    observation_time_list = [observation_time]*n_obs
    scraper_issues_list = [scraper_issues]*n_obs
    company_list = ['bojangles']*n_obs

    d = {'observation_time':observation_time_list,
         'scraper_issues': scraper_issues_list,
         'company':company_list,
         'address':address_list,
         'state':state_list,
         'status':status_list,
         'internal_id':internal_id_list,
         'website':website_list,
         'phone':phone_list,
         'extra':extra_list}

    df = pd.DataFrame(data=d)

    fname = fileparts[0] + '_bojangles.csv'
    outname = os.path.join(os.getcwd(),f'data/clean/bojangles/{fname}')
    df.to_csv(outname,index=False)

    return(df)

# *** Dunkin Donuts ***

def get_dunkin_data(proxypool,lat=38.6270,lon=-90.1994,radius=1000000,maxresults=1000000):

    """
    Scraper to pull data on dunkin donuts locations

    param: proxypool: pool of proxies to route requests through
    param: lat: latitude of origin point
    param: lon: longitude of origin point
    param: radius: search radius for dunkin locations (appears to be in miles). Make big to get all locations.
    param: maxresults: maximum number of results to return. Make big to get all locations
    """

    scraper_issues = False

    origin_str = f'{lat},{lon}'

    payload = {'service': 'DSL',
               'origin': origin_str,
               'radius': radius,
               'maxMatches': maxresults,
               'pageSize': 1,
               'units': 'm',
               'ambiguities': 'ignore'}

    url = 'https://www.dunkindonuts.com/bin/servlet/dsl'

    res = requests.post(url,data=payload,proxies=proxypool.random_proxy())

    if res.ok:

        try:

            results_dict = res.json()['data']['storeAttributes']

        except:

            results_dict = {}
            scraper_issues = True

    else:

        results_dict = {}
        scraper_issues = True

    date_str = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')

    raw_filepath = os.path.join(os.getcwd(),f'data/raw/dunkin_donuts/{date_str}_dunkin_donuts.json')

    with open(raw_filepath,'w') as f:
        json.dump(results_dict,f,indent=4)
        f.close()

    return(raw_filepath,scraper_issues)

def clean_dunkin_data(raw_filepath,scraper_issues):

    """
    Function to clean data scraped from dunkin donuts web api

    param: raw_filepath: filepath of raw data in .json format
    param: scraper_issues: False if data was scraped without errors; True otherwise
    """

    with open(raw_filepath,'r') as f:
        results_dict = json.load(f)
        f.close()

    fileparts = raw_filepath.split('/')[-1].split('_')
    observation_time = pd.Timestamp(fileparts[0] + ' ' + fileparts[1].replace('-',':'))

    internal_id_list = []
    address_list = []
    state_list = []
    status_list = []
    website_list = []
    phone_list = []
    extra_list = []

    extra_keys = ['operation_status_cd','close_reason_cd']

    for entry in results_dict:

        address = entry['address']

        if entry['address2'].strip(' ') != '':
            address += ' ' + entry['address2'].strip(' ')

        address += ', ' + entry['city'] + ', ' + entry['state'] + ' ' + entry['postal']

        state = entry['state']

        if entry['operation_status_cd']=='2':
            status = 'open'
        elif (entry['operation_status_cd']=='7') or (entry['operation_status_cd']=='3'):
            status = 'closed'
        else:
            status = 'inconclusive'

        internal_id = entry['recordId']
        website = entry['website']
        phone = entry['phonenumber']

        extra = ''

        for extra_key in extra_keys:
            extra += extra_key + ' : ' + entry[extra_key] + ', '

        extra = extra.strip(', ')

        address_list.append(address)
        state_list.append(state)
        status_list.append(status)
        internal_id_list.append(internal_id)
        website_list.append(website)
        phone_list.append(phone)
        extra_list.append(extra)

    n_obs = len(address_list)
    observation_time_list = [observation_time]*n_obs
    scraper_issues_list = [scraper_issues]*n_obs
    company_list = ['dunkin donuts']*n_obs

    d = {'observation_time':observation_time_list,
         'scraper_issues': scraper_issues_list,
         'company':company_list,
         'address':address_list,
         'state':state_list,
         'status':status_list,
         'internal_id':internal_id_list,
         'website':website_list,
         'phone':phone_list,
         'extra':extra_list}

    df = pd.DataFrame(data=d)

    fname = fileparts[0] + '_dunkin_donuts.csv'
    outname = os.path.join(os.getcwd(),f'data/clean/dunkin_donuts/{fname}')
    df.to_csv(outname,index=False)

    return(df)


# *** Wendys ***

def get_wendys_data(grid,proxypool,sleep_seconds=2.0,random_pause=2.0,maxresults=10000,radius_multiplier=1.0,failure_limit=5,backoff_seconds=10):

    """
    Scraper to pull data on wendys restaurants and locations

    param: grid: dataframe of lat/lon coordinates and radii to use in search
    param: proxypool: pool of proxies to route requests through
    param: sleep_seconds: number of seconds to wait in between api queries
    param: random_pause: total seconds between queries = sleep_seconds + uniform[0,random_pause]
    param: maxresults: maximum results to return within each search bubble (make big)
    param: radius_multiplier: multiplier applied to search radius
    param: failure_limit: number of times to re-attempt an api query if initial attempt fails
    param: backoff_seconds: number of seconds to wait before re-attempting api query
    """

    dist = stats.uniform(0,random_pause)
    scraper_issues = False
    result_list = []

    n = len(grid)


    for i,point in enumerate(grid.to_dict(orient='records')):

        print(f'{i} / {n} ({np.round(i/n*100,1)}%)',flush=True)

        result_dict = {}
        result_dict['point'] = point.copy()

        params = {'lang': 'en',
                  'cntry': 'US',
                  'sourceCode': 'ORDER.WENDYS',
                  'version': '20.5.0',
                  'lat': point['lat'],
                  'long': point['lon'],
                  'limit': maxresults,
                  'filterSearch': 'false',
                  'radius': point['radius']*radius_multiplier}

        headers={'Accept':'application/json'}

        url = 'https://digitalservices.prod.ext-aws.wendys.com/LocationServices/rest/nearbyLocations'

        num_failures = 0

        while num_failures < failure_limit:

            res = requests.get(url,params=params,headers=headers,proxies=proxypool.random_proxy())
            time.sleep(sleep_seconds + dist.rvs())

            if res.ok:
                result_dict['data'] = res.json()['data']
                break
            else:
                num_failures +=1
                time.sleep(backoff_seconds)

        if not res.ok:
            result_dict['data'] = res.status_code
            scraper_issues = True

        result_dict['time'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        result_list.append(result_dict.copy())

    date_str = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    raw_filepath = os.path.join(os.getcwd(),f'data/raw/wendys/{date_str}_wendys.json')

    with open(raw_filepath,'w') as f:
        json.dump(result_list,f,indent=4)
        f.close()

    return(raw_filepath,scraper_issues)

def clean_wendys_data(raw_filepath,scraper_issues):

    """
    Function to clean data scraped from wendys web api

    param: raw_filepath: filepath of raw data in .json format
    param: scraper_issues: False if data was scraped without errors; True otherwise
    """

    with open(raw_filepath,'r') as f:
        results_dict = json.load(f)
        f.close()

    fileparts = raw_filepath.split('/')[-1].split('_')

    observation_time_list = []
    address_list = []
    state_list = []
    status_list = []
    internal_id_list = []
    website_list = []
    phone_list = []
    extra_list = []

    extra_keys = ['distance','lat','lng']

    for result in results_dict:

        observation_time = result['time']

        if type(result['data'])==list:

            for entry in result['data']:

                if entry['country'] == 'US':

                    address = entry['address1']
                    address += ', '  + entry['address2']

                    state = entry['state']

                    # IMPORTANT: Wendy's API appears to only return open stores
                    # This means we will need to infer which stores are closed
                    # based on those that are missing

                    if entry['storeStatusCode']=='OP':
                        status = 'open'
                    else:
                        status = 'inconclusive'

                    internal_id = entry['id']
                    website = np.nan
                    phone = entry['phone']

                    extra = ''

                    for extra_key in extra_keys:
                        extra += extra_key + ' : ' + entry[extra_key] + ', '

                    extra = extra.strip(', ')

                    observation_time_list.append(observation_time)
                    address_list.append(address)
                    state_list.append(state)
                    status_list.append(status)
                    internal_id_list.append(internal_id)
                    website_list.append(website)
                    phone_list.append(phone)
                    extra_list.append(extra)

        else:
            print(result['data'])

    n_obs = len(address_list)
    scraper_issues_list = [scraper_issues]*n_obs
    company_list = ['wendys']*n_obs

    d = {'observation_time':observation_time_list,
         'scraper_issues': scraper_issues_list,
         'company':company_list,
         'address':address_list,
         'state':state_list,
         'status':status_list,
         'internal_id':internal_id_list,
         'website':website_list,
         'phone':phone_list,
         'extra':extra_list}

    df = pd.DataFrame(data=d)

    df = df[~df[['address','internal_id']].duplicated(keep='first')].reset_index(drop=True)

    fname = fileparts[0] + '_wendys.csv'
    outname = os.path.join(os.getcwd(),f'data/clean/wendys/{fname}')
    df.to_csv(outname,index=False)

    return(df)

# *** McDonalds ***

def update_mcdonalds_grid(grid,proxypool,sleep_seconds=0.1,random_pause=0.1,maxresults=174,radius_multiplier=1.0,failure_limit=5,backoff_seconds=3):

    """
    Scraper to check whether a given grid point contains a McDonalds restaurant.
    By running this function once per month, we can speed up our daily web scraping by
    only focusing on points with results.

    param: grid: dataframe of lat/lon coordinates and radii to use in search
    param: proxypool: pool of proxies to route requests through
    param: sleep_seconds: number of seconds to wait in between api queries
    param: random_pause: total seconds between queries = sleep_seconds + uniform[0,random_pause]
    param: maxresults: maximum results to return within each search bubble (make big)
    param: radius_multiplier: multiplier applied to search radius
    param: failure_limit: number of times to re-attempt an api query if initial attempt fails
    param: backoff_seconds: number of seconds to wait before re-attempting api query
    """

    dist = stats.uniform(0,random_pause)

    n = len(grid)
    count_idx = list(grid.columns).index('num_results')
    checked_idx = list(grid.columns).index('checked_this_month')

    for i,point in enumerate(grid.to_dict(orient='records')):

        print(f'{i} / {n} ({np.round(i/n*100,1)}%)',flush=True)

        params = {'latitude': point['lat'],
                  'longitude': point['lon'],
                  'radius': point['radius']*radius_multiplier,
                  'maxResults': maxresults,
                  'country': 'us',
                  'language': 'en-us'}

        headers={'Accept':'*/*',
                 'Accept-Encoding':'gzip, deflate, br',
                 'Accept-Language':'en-US,en;q=0.9',
                 'Referer':'https://www.mcdonalds.com/us/en-us/restaurant-locator.html',
                 'Sec-Ch-Ua':'"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
                 'Sec-Ch-Ua-Mobile':'?0',
                 'Sec-Ch-Ua-Platform':"Windows",
                 'Sec-Fetch-Dest':'empty',
                 'Sec-Fetch-Mode':'cors',
                 'Sec-Fetch-Site':'same-origin',
                 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

        url = 'https://www.mcdonalds.com/googleappsv2/geolocation'

        num_failures = 0

        while num_failures < failure_limit:

            res = requests.get(url,params=params,headers=headers,proxies=proxypool.random_proxy())
            time.sleep(sleep_seconds + dist.rvs())

            if res.ok:
                try:
                    grid.iloc[i,count_idx] = len(res.json()['features'])
                    grid.iloc[i,checked_idx] = True
                except:
                    grid.iloc[i,count_idx] = -1
                    grid.iloc[i,checked_idx] = True
                break

            else:
                num_failures +=1
                time.sleep(backoff_seconds)

        if not res.ok:
            grid.iloc[i,count_idx] = -1
            grid.iloc[i,checked_idx] = True

    return(grid)

def get_mcdonalds_data(grid,proxypool,sleep_seconds=0.1,random_pause=0.1,maxresults=174,radius_multiplier=1.0,failure_limit=5,backoff_seconds=3):

    """
    Scraper to pull data on wendys restaurants and locations

    param: grid: dataframe of lat/lon coordinates and radii to use in search
    param: proxypool: pool of proxies to route requests through
    param: sleep_seconds: number of seconds to wait in between api queries
    param: random_pause: total seconds between queries = sleep_seconds + uniform[0,random_pause]
    param: maxresults: maximum results to return within each search bubble (make big)
    param: radius_multiplier: multiplier applied to search radius
    param: failure_limit: number of times to re-attempt an api query if initial attempt fails
    param: backoff_seconds: number of seconds to wait before re-attempting api query
    """

    dist = stats.uniform(0,random_pause)
    scraper_issues = False
    result_list = []

    n = len(grid)


    for i,point in enumerate(grid.to_dict(orient='records')):

        print(f'{i} / {n} ({np.round(i/n*100,1)}%)',flush=True)

        result_dict = {}
        result_dict['point'] = point.copy()

        params = {'latitude': point['lat'],
                  'longitude': point['lon'],
                  'radius': point['radius']*radius_multiplier,
                  'maxResults': maxresults,
                  'country': 'us',
                  'language': 'en-us'}

        headers={'Accept':'*/*',
                 'Accept-Encoding':'gzip, deflate, br',
                 'Accept-Language':'en-US,en;q=0.9',
                 'Referer':'https://www.mcdonalds.com/us/en-us/restaurant-locator.html',
                 'Sec-Ch-Ua':'"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
                 'Sec-Ch-Ua-Mobile':'?0',
                 'Sec-Ch-Ua-Platform':"Windows",
                 'Sec-Fetch-Dest':'empty',
                 'Sec-Fetch-Mode':'cors',
                 'Sec-Fetch-Site':'same-origin',
                 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

        url = 'https://www.mcdonalds.com/googleappsv2/geolocation'

        num_failures = 0

        while num_failures < failure_limit:

            res = requests.get(url,params=params,headers=headers,proxies=proxypool.random_proxy())
            time.sleep(sleep_seconds + dist.rvs())

            if res.ok:
                result_dict['data'] = res.json()['features']
                break
            else:
                num_failures +=1
                time.sleep(backoff_seconds)

        if not res.ok:
            result_dict['data'] = res.status_code
            scraper_issues = True

        result_dict['time'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

        result_list.append(result_dict.copy())

    date_str = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    raw_filepath = os.path.join(os.getcwd(),f'data/raw/mcdonalds/{date_str}_mcdonalds.json')

    with open(raw_filepath,'w') as f:
        json.dump(result_list,f,indent=4)
        f.close()

    return(raw_filepath,scraper_issues)
