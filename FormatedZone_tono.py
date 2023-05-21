import pymongo
from hdfs import InsecureClient
import datetime
from fastavro import reader
import json
import csv
import time
import enlighten

persistent_client = pymongo.MongoClient("10.4.41.47", 27017)
persistent_zone = persistent_client["PersistentLandingZone"]
formated_zone = persistent_client['FormattedZone']
lookup = persistent_zone['lookup'].find({})
idealista = persistent_zone['idealista'].find({})


def get_lookup(lookup):
    for doc in lookup:
        if doc['filename'] == 'lookup_idealista_extended_2023-05-11-11:31:49.csv':
            lookupIdealista = doc['data']
        else:
            lookupOpenBCN = doc['data']
    return lookupIdealista, lookupOpenBCN

def formatted_idealista(lookup):
    data=[]
    duplicates=[]
    dup=0
    for i in persistent_zone['idealista'].find():
            for j in i['data']:
                if j['propertyCode'] not in duplicates:

                    j['id']=j['propertyCode']
                    j['source_id']=i['_id']
                    j['timestamp']=i['timestamp']

                    del j['propertyCode']

                    for k in lookup:
                        try:
                            if j['neighborhood'] in list(k.values()) or str.lower(j['neighborhood']) in list(k.values()):
                                j['district'] = k['district']
                                j['neighborhood'] = k['neighborhood']
                                j['district_id'] = k['district_id']
                                j['neighborhood_id'] = k['district_id']

                                data.append(j)
                                duplicates.append(j['id'])
                                break
                        except KeyError:
                            pass
                else:
                    dup+=1
    print(f'Removed {dup} duplicates')
    return data

def format_bcn_markets(lookup):
    data=[]
    duplicates=[]
    dup=0
    for i in persistent_zone['bcn-markets'].find():
        for j in i['data']:
            if j['register_id'] not in duplicates:

                if len(j['addresses'])>0:
                    for k in j['addresses'][0].keys():
                        j[k]=j['addresses'][0][k]

                if len(j['classifications_data'])>0:
                    for k in j['classifications_data'][0].keys():
                        j[k]=j['classifications_data'][0][k]

                j['x_coord'] = j['geo_epgs_25831']['x']
                j['y_coord'] = j['geo_epgs_25831']['y']
                j['source_id']=i['_id']
                j['timestamp']=i['timestamp']
                del j['geo_epgs_25831']
                del j['geo_epgs_23031']
                del j['geo_epgs_4326']
                del j['addresses']
                del j['classifications_data']
                del j['secondary_filters_data']
                del j['attribute_categories']
                del j['entity_types_data']
                del j['values']

                duplicates.append(j['register_id'])
                data.append(j)
            else:
                dup += 1

    print(f'Removed {dup} duplicates')
    rem=[]
    for i in data[0].keys():
        if all(d[i] is None for d in data):
            rem.append(i)
    [x.pop(i) for x in data for i in rem]


    for i in data:
        appended=True
        for j in lookup:
            if i['neighborhood_name'] in list(j.values()) or str.lower(i['neighborhood_name']) in list(j.values()) :
                del i['district_name']
                del i['neighborhood_name']

                i['district']=j['district']
                i['neighborhood']=j['neighborhood']
                i['district_id']=j['district_id']
                i['neighborhood_id']=j['district_id']

                appended=False
                break

        if appended:
            lookup.append(
                {
                    'district': i['district_name'],
                    'neighborhood': i['neighborhood_name'],
                    'district_n_reconciled': i['district_name'],
                    'district_n': str.lower(i['district_name']),
                    'district_id': 'Q'+i['district_id'] ,
                    'neighborhood_n_reconciled': i['neighborhood_name'],
                    'neighborhood_n':str.lower(i['neighborhood_name']),
                    'neighborhood_id': 'Q'+i['district_id']+i['neighborhood_id']
                }
            )
    return data

def format_bcn_income(lookup):
    data=[]
    duplicates=[]
    dup=0
    for i in persistent_zone['bcn-income'].find():
            for j in i['data']:
                id=(j['Any'] + j['Codi_Districte'] + j['Codi_Barri']).__str__()
                if id not in duplicates:
                    j['id']=id
                    j['source_id']=i['_id']
                    j['timestamp']=i['timestamp']

                    for k in lookup:
                        if j['Nom_Barri'] in list(k.values()) or str.lower(j['Nom_Barri']) in list(k.values()):
                            del j['Nom_Districte']
                            del j['Nom_Barri']
                            del j['Codi_Districte']
                            del j['Codi_Barri']

                            j['district'] = k['district']
                            j['neighborhood'] = k['neighborhood']
                            j['district_id'] = k['district_id']
                            j['neighborhood_id'] = k['district_id']


                            data.append(j)
                            duplicates.append(id)
                            break
                else:
                    dup+=1
    print(f'Removed {dup} duplicates')
    return data


lookupIdealista, lookupOpenBCN = get_lookup(lookup)
table_markets=format_bcn_markets(lookupOpenBCN)
table_income=format_bcn_income(lookupOpenBCN)
table_idealista=formatted_idealista(lookupIdealista)
#formatted_idealista(idealista, formated_zone, lookupIdealista)