import pandas as pd
from datetime  import date
from sqlalchemy import create_engine
import requests
import json
import time
import art
###   Function ###
def report(phras): # This function send requests, get report and delete him from server Yandex
    token = 'token'
    link = 'https://api.direct.yandex.ru/v4/json/'
    params_cr = {"method": "CreateNewWordstatReport", 
                "param": {'Phrases': phras}, "format": 'json',
                'token': token}
    
    r = requests.post(link, json.dumps(params_cr, ensure_ascii = False).encode('utf8')) # Requests on Create report
    report_num = r.json()

    key = 'data'
    if key in report_num:
        if report_num[key]:
            report_num = report_num[key]

    print(art.text2art('Send Requests', chr_ignore = True))

    print('Rep num:', report_num)

    time.sleep(60) # Time needed for creating report

    params_list = {'method': 'GetWordstatReportList',
        'param': report_num, "format": 'json',
        'token': token}

    list = requests.post(link, json.dumps(params_list, ensure_ascii = False).encode('utf8')) # Requests get report list, server has limit on 5 report in one time
    report_list = list.json()

    print(report_list)

    params_get = {'method': 'GetWordstatReport',
            'param': report_num, "format": 'json',
            'token': token}
    
    get = requests.post(link, json.dumps(params_get, ensure_ascii = False).encode('utf8')) # Get report data
    report_data = get.json()

    data = report_data

    print(art.text2art('Data Get!', chr_ignore = True))

    params_del = {'method': 'DeleteWordstatReport',
                'param': report_num, 'format': 'json',
                'token': token}
    
    deleted = requests.post(link, json.dumps(params_del, ensure_ascii = False).encode('utf8')) # Del report from server

    respons = deleted.json()

    key = 'data' 
    if key in respons: # Check error
        if respons[key] == 1:
                print(art.text2art('Report Deleted', chr_ignore = True))
    else:
        print(art.text2art('Error Del', chr_ignore = True))

    return data

def parsing(data,phras,claster): # Function parsing data with phras and claster and make DF
    x = 0
    CURRENT_DATE = date.today()
    shows = []
    for i in data['data']:
        if i['SearchedWith']:
            for j in i['SearchedWith']:
                try:
                    if j['Phrase'] == phras[x]:
                        shows.append([j['Phrase'],j['Shows'],claster[x],CURRENT_DATE])
                        x += 1
                except IndexError:
                    continue
    
    print(art.text2art('DataFrame Success', chr_ignore = True))
    shows = pd.DataFrame(shows,columns = ['Phrase','Shows','Cluster','Date'])
    return shows
    
### Main part ###
data = pd.read_excel('запросички.xlsx')

i = 0
PHRASES = []
CLASTERS = []

while i <=699: #699, Create 2 []  with clasters and phrases
    PHRAS = data.iloc[i][0]
    CLASTER = data.iloc[i][1]
    PHRASES.append(PHRAS)
    CLASTERS.append(CLASTER)
    i += 1

print(PHRASES[:10])

diap = []
for value in range(10, 710, 10): # Create [] with num from 10 to 710
    diap.append(value)

df = pd.DataFrame(columns = ['Phrase','Shows','Cluster','Date']) # Create DataFrame which will updated parsing datas

z = 0
for i in diap: # Distribution 700 phrases on 70 part, get report data and parsing data
    if z == 0:
        phras = (PHRASES[:diap[z]])
        claster = (CLASTERS[:diap[z]])
        data = report(phras)
        print(data)
        rep = parsing(data,phras,claster)
        df = df.append(rep, ignore_index = True)
        df.to_excel('Shows_info.xlsx')
        print(art.text2art('XLSX Update', chr_ignore = True))
        z += 1
    else:
        phras = (PHRASES[diap[z-1]:diap[z]])
        claster = (CLASTERS[diap[z-1]:diap[z]])
        data = report(phras)
        rep = parsing(data,phras,claster)
        df = df.append(rep, ignore_index = True)
        df.to_excel('Shows_info.xlsx')
        print(art.text2art('XLSX Update', chr_ignore = True))
        z += 1
        if z == 70:
            print('Конец')
            break

df = pd.read_excel('Shows_info.xlsx')
df = df.drop(df.columns[0], axis = 1)

engine = create_engine('...') 

df.to_sql('bi_WordStat_Shows', con = engine, if_exists = 'append', index = False)# Update DataBase Table.
    
print(art.text2art('DB Update', chr_ignore = True))


