import numpy as np
import scipy.stats as stats
import pandas as pd
import requests
import os
import json
from datetime import datetime
import time
import asyncio
import aiohttp as aiohttp

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

# *** Classes and functions to facilitate asynchronous web scraping *** #

class TokenBucket:

    """
    Rate limiter class that uses token-bucket algorithm to control rate of asynchronous
    requests to api. Based on method described by Quentin Pradet in following blog post:
    https://quentin.pradet.me/blog/how-do-you-rate-limit-calls-with-aiohttp.html
    """

    def __init__(self,client,max_tokens,rate,backoff):
        """
        param: client: Instance of aiohttp ClientSession
        param: max_tokens: maximum number of tokens in bucket
        param: rate: average rate of token generation [tokens/second]
        param: backoff: time to wait if no tokens available before re-checking [seconds]
        """
        self.client = client
        self.tokens = max_tokens
        self.max_tokens = max_tokens
        self.rate = rate
        self.last_update = time.monotonic()
        self.backoff = backoff

        return None

    async def get(self,*args,**kwargs):
        """
        Submit a GET request when token comes available
        """
        await self.wait_for_token()
        return self.client.get(*args,**kwargs)

    async def post(self,*args,**kwargs):
        """
        Submit a POST request when token comes available
        """
        await self.wait_for_token()
        return self.client.post(*args,**kwargs)

    async def wait_for_token(self):
        """
        Check to see if tokens are available; if none are left,
        wait for a bit and check to see if any new ones were generated.
        """
        while self.tokens < 1:
            self.mint_tokens()
            await asyncio.sleep(self.backoff)
        self.tokens -= 1

        return None

    def mint_tokens(self):
        """
        Create new tokens. Number of tokens generated in time since
        last update is drawn randomly from a poisson distribution.
        """
        now = time.monotonic()
        time_elapsed = now - self.last_update
        mu = time_elapsed*self.rate
        new_tokens = stats.poisson(mu).rvs()
        self.tokens = np.min([self.tokens + new_tokens,self.max_tokens])
        self.last_update = now

        return None

async def json_get_request(client,req):
    """
    Helper function for implementing asynchronous GET requests returning a json
    """
    async with await client.get(url=req['url'],params=req['params'],headers=req['headers'],proxy=req['proxies']['http']) as res:
        return await res.json()

async def json_post_request(client,req):
    """
    Helper function for implementing asynchronous POST requests returning a json
    """
    async with await client.post(url=req['url'],data=req['payload'],headers=req['headers'],proxy=req['proxies']['http']) as res:
        return await res.json()

class AsynchronousScraper:

    def __init__(self,request_func,method='GET',num_retry=1,max_tokens=10,rate=10,backoff=0.2):
        """
        Class to implement asyncronous web scraping.

        param: request_func: function to generate list of dictonaries containing relevant arguments for request
        param: method: GET or POST
        param: num_retry: number of times to reattempt a failed web scrape
        param: max_tokens: maximum number of tokens in bucket
        param: rate: average rate of token generation [tokens/second]
        param: backoff: time to wait if no tokens available before re-checking [seconds]
        """
        self.request_func = request_func
        request_list = request_func()

        self.pending_requests = np.array(request_list)
        self.pending_indices = np.arange(len(request_list))
        self.results = np.array([])
        self.completed_requests = np.array([])
        self.completed_indices = np.array([])
        self.num_retry = num_retry
        self.max_tokens=max_tokens
        self.rate=rate
        self.backoff=backoff

        if method == 'GET':
            self.submit_request = json_get_request
        else:
            self.submit_request = json_post_request

        return None

    async def scraping_pass(self):
        """
        Perform one pass over list of pending pages to scrape.
        """

        async with aiohttp.ClientSession() as client:
            client = TokenBucket(client,self.max_tokens,self.rate,self.backoff)
            tasks = [asyncio.ensure_future(self.submit_request(client, req)) for req in self.pending_requests]
            return await asyncio.gather(*tasks,return_exceptions=True)

    async def scrape(self):
        """
        Main function to perform web scraping
        """

        num_passes = 0

        while (len(self.pending_requests) > 0) and (num_passes <= self.num_retry):

            # Perform web scraping pass
            new_results = await self.scraping_pass()
            new_results = np.array(new_results)

            # Inspect results
            is_exception = np.array([isinstance(x,Exception) for x in new_results])
            self.results = np.append(self.results,new_results[~is_exception])
            self.completed_requests = np.append(self.completed_requests,self.pending_requests[~is_exception])
            self.completed_indices = np.append(self.completed_indices,self.pending_indices[~is_exception])
            self.pending_indices = self.pending_indices[is_exception]

            # Generate new version of request list with shuffled proxies
            request_list = np.array(self.request_func())

            # Create list of requests that failed initially that we'll retry
            self.pending_requests = request_list[self.pending_indices]

            num_passes += 1

        num_fail = len(self.pending_indices)
        self.results = np.append(self.results,np.array([None]*num_fail))
        self.completed_indices = np.append(self.completed_indices,self.pending_indices)

        req_order = self.completed_indices.argsort()

        return(self.results[req_order].tolist())

# *** Initial setup *** #

def create_folders(companies=['bojangles','dunkin_donuts','wendys','mcdonalds','waffle_house']):
    """
    Function to create directory structure for data scraped from company websites

    param: companies: list of company names
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

def scrape_bojangles_data(proxypool,sleep_seconds=0.2,random_pause=0.1,increment=50,extra_limit=5,failure_limit=10):

    """
    Scraper to pull data on bojangles restaurants and locations

    param: proxypool: pool of proxies to route requests through
    param: sleep_seconds: number of seconds to wait in between api queries
    param: random_pause: total seconds between queries = sleep_seconds + uniform[0,random_pause]
    param: increment: number of results to get in each api query
    param: extra_limit: number of additional times to query api after reaching the "final" location
    param: failure_limit: number of additional times to query api if initial attempt fails
    """

    result_dict = {}

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
                result_dict[keystr] = res_dict.copy()

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
        json.dump(result_dict,f,indent=4)
        f.close()

    return(raw_filepath,scraper_issues)

def clean_bojangles_data(raw_filepath,scraper_issues):

    """
    Function to clean data scraped from bojangles web api

    param: raw_filepath: filepath of raw data in .json format
    param: scraper_issues: False if data was scraped without errors; True otherwise
    """


    with open(raw_filepath,'r') as f:
        result_dict = json.load(f)
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

    for key in result_dict.keys():
        for entry in result_dict[key]['response']['results']:

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

def scrape_dunkin_data(grid,proxypool,sleep_seconds=0.2,random_pause=0.1,maxresults=10000,radius_multiplier=1.0,failure_limit=5,backoff_seconds=3):

    """
    Scraper to pull data on dunkin donuts locations

    param: proxypool: pool of proxies to route requests through
    param: grid: dataframe of lat/lon coordinates and radii to use in search
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

        lat = point['lat']
        lon = point['lon']
        origin_str = f'{lat},{lon}'

        payload = {'service': 'DSL',
                   'origin': origin_str,
                   'radius': int(point['radius']*radius_multiplier),
                   'maxMatches': maxresults,
                   'pageSize': 1,
                   'units': 'm',
                   'ambiguities': 'ignore'}

        url = 'https://www.dunkindonuts.com/bin/servlet/dsl'

        num_failures = 0

        while num_failures < failure_limit:

            res = requests.post(url,data=payload,proxies=proxypool.random_proxy())
            time.sleep(sleep_seconds + dist.rvs())

            if res.ok:
                try:
                    result_dict['data'] = res.json()['data']['storeAttributes']
                except:
                    result_dict['data'] = -1
                    scraper_issues = True
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
    raw_filepath = os.path.join(os.getcwd(),f'data/raw/dunkin_donuts/{date_str}_dunkin_donuts.json')

    with open(raw_filepath,'w') as f:
        json.dump(result_list,f,indent=4)
        f.close()

    return(raw_filepath,scraper_issues)

def clean_dunkin_data(raw_filepath,scraper_issues):

    """
    Function to clean data scraped from dunkin donuts web api

    param: raw_filepath: filepath of raw data in .json format
    param: scraper_issues: False if data was scraped without errors; True otherwise
    """

    with open(raw_filepath,'r') as f:
        result_dict = json.load(f)
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

    extra_keys = ['operation_status_cd','close_reason_cd']

    for result in result_dict:

        observation_time = result['time']

        if type(result['data'])==list:

            for entry in result['data']:

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

                observation_time_list.append(observation_time)
                address_list.append(address)
                state_list.append(state)
                status_list.append(status)
                internal_id_list.append(internal_id)
                website_list.append(website)
                phone_list.append(phone)
                extra_list.append(extra)

    n_obs = len(address_list)
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

    df = df[~df[['address','internal_id']].duplicated(keep='first')].reset_index(drop=True)

    fname = fileparts[0] + '_dunkin_donuts.csv'
    outname = os.path.join(os.getcwd(),f'data/clean/dunkin_donuts/{fname}')
    df.to_csv(outname,index=False)

    return(df)


# *** Wendys ***

def scrape_wendys_data(grid,proxypool,sleep_seconds=0.2,random_pause=0.1,maxresults=10000,radius_multiplier=1.0,failure_limit=5,backoff_seconds=3):

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
        result_dict = json.load(f)
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

    for result in result_dict:

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

def scrape_mcdonalds_data(grid,proxypool,sleep_seconds=0.1,random_pause=0.1,maxresults=174,radius_multiplier=1.0,failure_limit=5,backoff_seconds=3):

    """
    Scraper to pull data on mcdonalds restaurants and locations

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
                    result_dict['data'] = res.json()['features']
                except:
                    result_dict['data'] = -1
                    scraper_issues = True
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

def configure_mcdonalds_requests(grid,proxypool,maxresults=174,radius_multiplier=1.0):

    """
    Function to configure mcdonalds request parameters

    param: grid: dataframe of lat/lon coordinates and radii to use in search
    param: proxypool: pool of proxies to route requests through
    param: maxresults: maximum results to return within each search bubble (make big)
    param: radius_multiplier: multiplier applied to search radius
    """

    n = len(grid)

    request_list = []

    for point in grid.to_dict(orient='records'):

        request_dict = {}

        params = {'latitude': point['lat'],
                  'longitude': point['lon'],
                  'radius': point['radius']*radius_multiplier,
                  'maxResults': maxresults,
                  'country': 'us',
                  'language': 'en-us'}

        headers={'Accept':'*/*',
                 'Accept-Language':'en-US,en;q=0.9',
                 'Referer':'https://www.mcdonalds.com/us/en-us/restaurant-locator.html',
                 'Sec-Ch-Ua':'"Not.A/Brand";v="8", "Chromium";v="114", "Google Chrome";v="114"',
                 'Sec-Ch-Ua-Mobile':'?0',
                 'Sec-Ch-Ua-Platform':"Windows",
                 'Sec-Fetch-Dest':'empty',
                 'Sec-Fetch-Mode':'cors',
                 'Sec-Fetch-Site':'same-origin',
                 'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

        # When using the asynchronous scraper we need to have 'http' insteald of 'https'
        request_dict['url'] = 'http://www.mcdonalds.com/googleappsv2/geolocation'
        request_dict['params'] = params
        request_dict['headers'] = headers
        request_dict['proxies'] = proxypool.random_proxy()

        request_list.append(request_dict.copy())

    return(request_list)

def async_scrape_mcdonalds_data(grid,proxypool,maxresults=174,radius_multiplier=1.0,max_tokens=15,rate=10):
    """
    Function to asynchronously scrape data from mcdonalds web api.

    param: grid: dataframe of lat/lon coordinates and radii to use in search
    param: proxypool: pool of proxies to route requests through
    param: maxresults: maximum results to return within each search bubble (make big)
    param: radius_multiplier: multiplier applied to search radius
    param: max_tokens: maximum number of tokens in bucket
    param: rate: average rate of token generation [tokens/second]
    """

    request_func = lambda: configure_mcdonalds_requests(grid,proxypool,maxresults,radius_multiplier)
    scraper = AsynchronousScraper(request_func,method='GET',max_tokens=max_tokens,rate=rate)
    result_list = asyncio.run(scraper.scrape())

    scraper_issues = False

    for i,point in enumerate(grid.to_dict(orient='records')):

        result_dict = {}
        result_dict['point'] = point.copy()

        if isinstance(result_list[i],dict):
            try:
                result_dict['data'] = result_list[i]['features']
            except:
                result_dict['data'] = -1
                scraper_issues = True
        else:
            result_dict['data'] = -1
            scraper_issues = True

        result_dict['time'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        result_list[i] = result_dict.copy()

    date_str = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    raw_filepath = os.path.join(os.getcwd(),f'data/raw/mcdonalds/{date_str}_mcdonalds.json')

    with open(raw_filepath,'w') as f:
       json.dump(result_list,f,indent=4)
       f.close()

    return(raw_filepath,scraper_issues)

def clean_mcdonalds_data(raw_filepath,scraper_issues):

    """
    Function to clean data scraped from mcdonalds web api.

    param: raw_filepath: filepath of raw data in .json format
    param: scraper_issues: False if data was scraped without errors; True otherwise
    """

    with open(raw_filepath,'r') as f:
        result_dict = json.load(f)
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

    for result in result_dict:

        observation_time = result['time']

        if type(result['data'])==list:

            for entry in result['data']:

                extra = str(entry['geometry']).strip('{}')
                entry = entry['properties']

                address = entry['addressLine1']

                if 'addressLine2' in entry.keys():
                    address += ', ' + entry['addressLine2']

                address += ', '  + entry['customAddress']

                state = entry['subDivision']

                # IMPORTANT: McDonald's API appears to only return open stores
                # This means we will need to infer which stores are closed
                # based on those that are missing

                if entry['openstatus']=='OPEN':
                    status = 'open'
                else:
                    status = 'inconclusive'

                internal_id = entry['identifierValue']

                if 'restaurantUrl' in entry.keys():
                    website = entry['restaurantUrl']
                else:
                    website = ''

                if 'telephone' in entry.keys():
                    phone = entry['telephone']
                else:
                    phone = ''

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
    company_list = ['mcdonalds']*n_obs

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

    fname = fileparts[0] + '_mcdonalds.csv'
    outname = os.path.join(os.getcwd(),f'data/clean/mcdonalds/{fname}')
    df.to_csv(outname,index=False)

    return(df)

# *** Waffle House *** #
def scrape_wafflehouse_data(restaurant_numbers,proxypool,sleep_seconds=0.1,random_pause=0.1,failure_limit=5,backoff_seconds=1):

    """
    Scraper to pull data on mcdonalds restaurants and locations

    param: restaurant_numbers: numpy array of restaurant numbers to use in search
    param: proxypool: pool of proxies to route requests through
    param: sleep_seconds: number of seconds to wait in between api queries
    param: random_pause: total seconds between queries = sleep_seconds + uniform[0,random_pause]
    param: radius_multiplier: multiplier applied to search radius
    param: failure_limit: number of times to re-attempt an api query if initial attempt fails
    param: backoff_seconds: number of seconds to wait before re-attempting api query
    """

    dist = stats.uniform(0,random_pause)
    scraper_issues = False
    result_list = []

    n = len(restaurant_numbers)

    for i,restaurant_number in enumerate(restaurant_numbers):

        result_dict = {}
        result_dict['restaurant_number'] = int(restaurant_number)

        print(f'{i} / {n} ({np.round(i/n*100,1)}%)',flush=True)

        params = None

        headers = {'Accept':'application/json, */*',
                   'Accept-Language':'en-US,en;q=0.9',
                   'Referer':f'https://order.wafflehouse.com/menu/waffle-house-{restaurant_number}',
                   'Sec-Ch-Ua':'\"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"',
                   'Sec-Ch-Ua-Mobile':'?0',
                   'Sec-Ch-Ua-Platform':'\"Windows\"',
                   'Sec-Fetch-Dest':'empty',
                   'Sec-Fetch-Mode':'cors',
                   'Sec-Fetch-Site':'same-origin',
                   'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                   'X-Olo-App-Platform':'web',
                   'X-Olo-Country':'us',
                   'X-Olo-Request':'1',
                   'X-Olo-Viewport':'Tablet',
                   'X-Requested-With':'XMLHttpRequest'}

        url = f'https://order.wafflehouse.com/api/vendors/waffle-house-{restaurant_number}'

        num_failures = 0

        while num_failures < failure_limit:

            res = requests.get(url,params=params,headers=headers,proxies=proxypool.random_proxy())
            time.sleep(sleep_seconds + dist.rvs())

            if res.ok:
                try:
                    result_dict['data'] = res.json()['vendor']
                except:
                    result_dict['data'] = -1
                    scraper_issues = True
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
    raw_filepath = os.path.join(os.getcwd(),f'data/raw/waffle_house/{date_str}_waffle_house.json')

    with open(raw_filepath,'w') as f:
        json.dump(result_list,f,indent=4)
        f.close()

    return(raw_filepath,scraper_issues)


def configure_wafflehouse_requests(restaurant_numbers,proxypool):

    """
    Function to configure waffle house request parameters

    param: restaurant_numbers: numpy array of restaurant numbers to use in search
    param: proxypool: pool of proxies to route requests through
    """

    request_list = []

    for restaurant_number in restaurant_numbers:

        request_dict = {}

        headers = {'Accept':'application/json, */*',
                   'Accept-Language':'en-US,en;q=0.9',
                   'Referer':f'https://order.wafflehouse.com/menu/waffle-house-{restaurant_number}',
                   'Sec-Ch-Ua':'\"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"',
                   'Sec-Ch-Ua-Mobile':'?0',
                   'Sec-Ch-Ua-Platform':'\"Windows\"',
                   'Sec-Fetch-Dest':'empty',
                   'Sec-Fetch-Mode':'cors',
                   'Sec-Fetch-Site':'same-origin',
                   'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                   'X-Olo-App-Platform':'web',
                   'X-Olo-Country':'us',
                   'X-Olo-Request':'1',
                   'X-Olo-Viewport':'Tablet',
                   'X-Requested-With':'XMLHttpRequest'}

        # Note use of http instead of https
        request_dict['url'] = f'http://order.wafflehouse.com/api/vendors/waffle-house-{restaurant_number}'
        request_dict['params'] = None
        request_dict['headers'] = headers
        request_dict['proxies'] = proxypool.random_proxy()

        request_list.append(request_dict.copy())

    return(request_list)

def async_scrape_wafflehouse_data(restaurant_numbers,proxypool,max_tokens=15,rate=10):
    """
    Function to asynchronously scrape data from waffle house web api.

    param: restaurant_numbers: numpy array of restaurant numbers to use in search
    param: proxypool: pool of proxies to route requests through
    param: max_tokens: maximum number of tokens in bucket
    param: rate: average rate of token generation [tokens/second]
    """

    request_func = lambda: configure_wafflehouse_requests(restaurant_numbers,proxypool)
    scraper = AsynchronousScraper(request_func,method='GET',num_retry=5,max_tokens=max_tokens,rate=rate)
    result_list = asyncio.run(scraper.scrape())

    scraper_issues = False

    for i,restaurant_number in enumerate(restaurant_numbers):

        result_dict = {}
        result_dict['restaurant_number'] = int(restaurant_number)

        if isinstance(result_list[i],dict):
            try:
                result_dict['data'] = result_list[i]['vendor']
            except:
                result_dict['data'] = -1
                scraper_issues = True
        else:
            result_dict['data'] = -1
            scraper_issues = True

        result_dict['time'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        result_list[i] = result_dict.copy()

    date_str = datetime.today().strftime('%Y-%m-%d_%H-%M-%S')
    raw_filepath = os.path.join(os.getcwd(),f'data/raw/waffle_house/{date_str}_waffle_house.json')

    with open(raw_filepath,'w') as f:
       json.dump(result_list,f,indent=4)
       f.close()

    return(raw_filepath,scraper_issues)
