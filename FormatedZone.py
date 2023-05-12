import pymongo
import json


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
    return lookupIdealista,lookupOpenBCN

def formatted_idealista(idealista, Formated_zone, lookup ):
    propertyCodes = []
    duplicates = 0
    for document in idealista:
        data = document['data'][0]
        if data['propertyCode'] not in propertyCodes:
            propertyCodes.append(data['propertyCode'])
        else:
            duplicates +=1
            continue
        
        if 'district' in data.keys():
            district = data['district']
            if 'neighborhood' in data.keys():
                neighborhood = data['neighborhood']
                for neighborhoodLookup in lookup:
                    if neighborhoodLookup['neighborhood'] == neighborhood:

                        idN = neighborhoodLookup['neighborhood_id']
                        idD = neighborhoodLookup['district_id']

                        document['data'][0]['neighborhood_id'] = idN
                        document['data'][0]['district_id'] = idD
                        break
                continue
            
            for districtLookup in lookup:
                if districtLookup['district'] == district:

                    idD = neighborhoodLookup['district_id']

                    document['data'][0]['district_id'] = idD
                    break

    print(f'Removed {duplicates} duplicates')
lookupIdealista,lookupOpenBCN =  get_lookup(lookup)  
formatted_idealista(idealista, formated_zone, lookupIdealista)