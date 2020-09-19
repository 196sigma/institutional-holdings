import pandas as pd
import sys
import re
import urllib.request
import csv
import io
import lxml.etree as et
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
import requests

__FILER_INFO_FP__ = "filer-info-db.csv"
__XSL_FILE__ = "holdingsparser/xslfiles/13F-HR.xsl"
OPENFIGI_API_KEY = "db1b37e5-875e-4999-a014-243ebab89a16"

def ticker_from_cusip(cusip, load_from_cache=False, save_to_cache=True):
    if load_from_cache:
        # Not implemented yet!
        cusip_ticker_df = pd.read_csv("cusip-ticker-db.csv")
        pass
    
    # If not in cache, pull from OpenFIGI and save to cache
    headers = {
    'Content-Type': 'application/json',
    'X-OPENFIGI-APIKEY': OPENFIGI_API_KEY,}
    post_data = '[{"idType":"ID_CUSIP","idValue":"%s","exchCode":"US"}]' % cusip
    response = requests.post('https://api.openfigi.com/v2/mapping', headers=headers, data=post_data)
    if response.status_code == 200:
        try:
            name = response.json()[0]['data'][0]['name']
            ticker_symbol = response.json()[0]['data'][0]['ticker']
            exchange_code = response.json()[0]['data'][0]['exchCode']
            return (name, ticker_symbol, exchange_code)
        except:
            return None
    else:
        print(response.status_code)
    return None

def get_filer_info(filer_info_fp: str = "filer-info-db.csv") -> pd.DataFrame:
    filer_info_db = pd.read_csv(filer_info_fp)
    filer_info_db['cik'] = filer_info_db['cik'].apply(lambda s: str(s).zfill(10))
    filer_info_db = filer_info_db.sort_values(['filer', 'quarter'])
    return filer_info_db

def get_holdings_document_url(cik, quarter):
    filer_info_db = get_filer_info()
    query_string = f"cik == '{cik}' & quarter == '{quarter}'"
    res = filer_info_db.query(query_string)
    res = list(res['holdings_xml_url'])
    res = res[0]
    return res

def parse_holdings_document(holdings_document_url, xsl_file_location):
    # download the information table
    with urllib.request.urlopen(holdings_document_url) as response:
        info_table = response.read()

    # parse the information table
    soup = BeautifulSoup(info_table, 'lxml')

    # remove xmlns attribute
    informationtable_re = re.compile('informationtable', re.I)
    soup.find(informationtable_re).attrs = {}

    # remove namespace from all elements
    namespace_re = re.compile('^.+\:.+$', re.I)
    tag_name_re = re.compile('^.+\:(.+)$', re.I)

    for tag in soup.find_all(namespace_re):
        tag.name = tag_name_re.match(tag.name).group(1)

    soup = soup.find('informationtable')

    with open(xsl_file_location, 'r') as xsl_file:
        xslt = et.parse(xsl_file)

    dom = et.parse(io.StringIO(soup.prettify()))
    transform = et.XSLT(xslt)
    transformed_output = transform(dom)

    return transformed_output

def get_holdings_by_cik_by_quarter(cik, quarter=""):
    cik = str(cik).zfill(10)
    filer_info_db = get_filer_info(__FILER_INFO_FP__)
    
    query_string = f"cik == '{cik}' & quarter == '{quarter}'"
    res = filer_info_db.query(query_string)
    date_filed = list(res['date_filed'])[0]
    holdings_document_url = list(res['holdings_xml_url'])[0]
    
    data = parse_holdings_document(holdings_document_url, __XSL_FILE__)
    data = str(data)
    data = data.split("\n")
    data = [s.split("\t") for s in data]
    
    # remove rows with incorrect number of columns (e.g. empty rows)
    data = [d for d in data if len(d) == 10]
    
    # convert to pandas dataframe
    data = pd.DataFrame.from_records(data[1:], columns=data[0])
    
    # add column with date
    data['quarter'] = quarter
    data['date_filed'] = date_filed

    numeric_cols = ['Value', 
                    'SshPrnAmt',
                    'VotingAuthoritySole', 
                    'VotingAuthorityShared', 
                    'VotingAuthorityNone']
    for v in numeric_cols:
        data[v] = data[v].astype(int)

    # convert 'value' to dollars from thousands of dollars
    data['Value'] = data['Value']*1000
    
    # Add central index key column
    data['cik'] = cik
    
    data['date_filed'] = date_filed
    
    return data

def get_holdings_by_cik(cik, quarter) -> pd.DataFrame:
    filer_info_db = get_filer_info(__FILER_INFO_FP__)
    
    df = pd.DataFrame()
    
    _holdings = filer_info_db.query(f"cik == '{cik}' & quarter == '{quarter}'")

    holdings_document_url = get_holdings_document_url(cik=cik, quarter=quarter)
    
    df = pd.concat([df, get_holdings_by_cik(cik=cik, 
                                       holdings_document_url=holdings_document_url, 
                                       quarter=quarter)], axis=0)

    return df

def plot_13f(data, top_n=10, title=''):
    total_value = data['Value'].sum()
    data['pct_value'] = data['Value']/total_value
    data = data.sort_values('pct_value', ascending=False)

    y = list(data['pct_value'][:top_n])
    x = list(data['Name'][:top_n])

    plt.figure(figsize=(16,8))
    plt.xticks(rotation=45, horizontalalignment='right')
    plt.title(title)
    for i, v in enumerate(y):
        plt.text(i, y[i], round(100*y[i], 2), ha='center', fontweight='bold')
    holdings_plot = plt.bar(height=y, x=x)
    return None