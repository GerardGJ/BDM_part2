import pymongo
from hdfs import InsecureClient
import datetime
from fastavro import reader
import json
import csv
import time
import enlighten

import sqlite3
from sqlite3 import Error

persistent_client = pymongo.MongoClient("10.4.41.47", 27017)
persistent_zone = persistent_client["PersistentLandingZone"]
formated_zone = persistent_client['FormattedZone']
lookup = persistent_zone['lookup'].find({})
idealista = persistent_zone['idealista'].find({})


def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


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
    for i in persistent_zone['idealista'].find({}):
        for j in i['data']:
            for k in lookup:
                val=j.get('neighborhood',None)
                if val is not None:
                    if (val in list(k.values()) or str.lower(val) in list(k.values())):
                        if j['propertyCode'] not in duplicates:
                            j['id'] = j['propertyCode']
                            del j['propertyCode']

                            j['source_id'] = i['_id'].__str__()
                            j['timestamp'] = i['timestamp']
                            j['district'] = k['district']
                            j['neighborhood'] = k['neighborhood']
                            j['district_id'] = k['district_id']
                            j['neighborhood_id'] = k['district_id']

                            try:
                                for kk in j['parkingSpace'].keys():
                                    j[kk]=j['parkingSpace'][kk]
                                for kk in j['detailedType'].keys():
                                    j[kk]=j['detailedType'][kk]
                                for kk in j['suggestedTexts'].keys():
                                    j[kk]=j['suggestedTexts'][kk]
                                del j['parkingSpace']
                                del j['detailedType']
                                del j['suggestedTexts']
                            except KeyError:
                                pass

                            data.append(j)
                            duplicates.append(j['id'])
                            break
                        else:
                            dup += 1
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


                j['n_warnings']=len(j['warnings'])
                j['x_coord'] = j['geo_epgs_25831']['x']
                j['y_coord'] = j['geo_epgs_25831']['y']
                j['source_id']=i['_id'].__str__()
                j['timestamp']=i['timestamp']
                j['asia_id']=int(j['asia_id'])
                del j['id']
                del j['geo_epgs_25831']
                del j['geo_epgs_23031']
                del j['geo_epgs_4326']
                del j['addresses']
                del j['classifications_data']
                del j['secondary_filters_data']
                del j['attribute_categories']
                del j['entity_types_data']
                del j['values']
                del j['image_data']
                del j['status_name']
                del j['location']
                del j['gallery_data']
                del j['warnings']
                del j['sections_data']
                del j['tickets_data']
                del j['from_relationships']
                del j['to_relationships']
                del j['timetable']
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
    for i in persistent_zone['bcn-income'].find({}):
            for j in i['data']:
                id=(j['Any'] + j['Codi_Districte'] + j['Codi_Barri']).__str__()
                if id not in duplicates:
                    j['id']=id
                    j['source_id']=i['_id'].__str__()
                    j['timestamp']=i['timestamp']

                    for k in lookup:
                        if j['Nom_Barri'] in list(k.values()) or str.lower(j['Nom_Barri']) in list(k.values()):
                            del j['Nom_Districte']
                            del j['Nom_Barri']
                            del j['Codi_Districte']
                            del j['Codi_Barri']

                            j['population']=j['Població']
                            j['year']=j['Any']
                            j['index_rfd']=j['Índex RFD Barcelona = 100']
                            j['district'] = k['district']
                            j['neighborhood'] = k['neighborhood']
                            j['district_id'] = k['district_id']
                            j['neighborhood_id'] = k['district_id']

                            del j['Població']
                            del j['Índex RFD Barcelona = 100']
                            del j['Any']

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

formatted_path='./FormattedZone'
connection = create_connection(formatted_path)

create_idealista_table = """
CREATE TABLE IF NOT EXISTS idealista (
    id INTEGER PRIMARY KEY, 
    thumbnail TEXT,
    externalReference TEXT,
    numPhotos INTEGER, 
    floor INTEGER, 
    price REAL,
    propertyType TEXT, 
    operation TEXT, 
    size REAL,
    exterior TEXT, 
    rooms INTEGER,
    bathrooms INTEGER, 
    address TEXT, 
    province TEXT, 
    municipality TEXT, 
    district TEXT,
    country TEXT, 
    neighborhood TEXT, 
    latitude REAL, 
    longitude REAL, 
    showAddress TEXT, 
    url TEXT, 
    distance INTEGER, 
    hasVideo TEXT, 
    status TEXT, 
    newDevelopment TEXT,
    newDevelopmentFinished TEXT, 
    hasLift TEXT, 
    parkingSpace TEXT, 
    priceByArea REAL, 
    hasPlan TEXT, 
    has3DTour TEXT, 
    has360 TEXT, 
    hasStaging TEXT, 
    topNewDevelopment TEXT, 
    source_id TEXT, 
    timestamp TEXT, 
    district_id TEXT, 
    neighborhood_id TEXT,
    hasParkingSpace TEXT, 
    isParkingSpaceIncludedInPrice TEXT, 
    parkingSpacePrice REAL, 
    typology TEXT, 
    subTypology TEXT, 
    subtitle TEXT, 
    title TEXT
);
"""
create_income_table = """
CREATE TABLE IF NOT EXISTS income (
    id INTEGER PRIMARY KEY, 
    year INTEGER, 
    population TEXT, 
    index_rfd REAL, 
    source_id TEXT, 
    timestamp TEXT, 
    district TEXT,
    neighborhood TEXT, 
    district_id TEXT, 
    neighborhood_id TEXT
);
"""
create_market_table = """
CREATE TABLE IF NOT EXISTS markets (
    register_id INTEGER PRIMARY KEY,
    name TEXT, 
    created TEXT, 
    modified TEXT, 
    status TEXT, 
    core_type TEXT, 
    core_type_name TEXT, 
    body TEXT, 
    ical TEXT, 
    place TEXT, 
    district_id TEXT, 
    neighborhood_id TEXT, 
    address_name TEXT, 
    address_id INTEGER, 
    start_street_number INTEGER, 
    end_street_number INTEGER, 
    street_number_1 TEXT, 
    street_number_2 TEXT, 
    level INTEGER, 
    zip_code INTEGER, 
    province TEXT, 
    town TEXT, 
    country TEXT, 
    position INTEGER, 
    position_relative TEXT, 
    main_address TEXT, 
    roadtype_name TEXT, 
    roadtype_id TEXT, 
    hide_address TEXT, 
    full_path TEXT, 
    dependency_group INTEGER, 
    parent_id INTEGER, 
    tree_id INTEGER, 
    asia_id INTEGER, 
    x_coord REAL, 
    y_coord REAL, 
    source_id TEXT, 
    timestamp TEXT, 
    district TEXT, 
    neighborhood TEXT,
    n_warnings INTEGER
);
"""

execute_query(connection,create_idealista_table)
execute_query(connection,create_income_table)
execute_query(connection,create_market_table)

err=0
for x in table_idealista:
    columns = ', '.join(x.keys())
    placeholders = ':'+', :'.join(x.keys())
    query = 'INSERT OR REPLACE INTO idealista (%s) VALUES (%s)' % (columns, placeholders)
    try:
        connection.execute(query,x)
        connection.commit()
    except Error as e:
        err+=1
print(f'Number of failed inserts {err}')

err=0
for x in table_income:
    columns = ', '.join(x.keys())
    placeholders = ':'+', :'.join(x.keys())
    query = 'INSERT OR REPLACE INTO income (%s) VALUES (%s)' % (columns, placeholders)
    try:
        connection.execute(query,x)
        connection.commit()

    except Error as e:
        err += 1
print(f'Number of failed inserts {err}')

err=0
for x in table_markets:
    columns = ', '.join(x.keys())
    placeholders = ':'+', :'.join(x.keys())
    query = 'INSERT OR REPLACE INTO markets (%s) VALUES (%s)' % (columns, placeholders)
    connection.execute(query,x)
    connection.commit()
    try:
        connection.execute(query, x)
        connection.commit()

    except Error as e:
        err += 1
print(f'Number of failed inserts {err}')
