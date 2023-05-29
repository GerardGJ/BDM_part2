import pandas as pd
import pymongo
from hdfs import InsecureClient
import pyarrow as pa
import pyarrow.parquet as pq

import sqlite3
from sqlite3 import Error

import os
import pyspark
import sys
import findspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer,VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import mean, sum, count, col,substring
import matplotlib.pyplot as plt
import seaborn as sns

hdfs_host = 'http://10.4.41.47:9870'
hdfs_client = InsecureClient(hdfs_host, user='bdm')

persistent_client = pymongo.MongoClient("10.4.41.47", 27017)
persistent_zone = persistent_client["PersistentLandingZone"]
formated_zone = persistent_client['FormattedZone']
Lookup = persistent_zone['lookup'].find({})
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
    lookupIdealista=None
    lookupOpenBCN=None
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
                        if (j['propertyCode'].__str__()+i['filename'][(i['filename'].find('_')+1):(i['filename'].find('_')+11)]) not in duplicates:
                            j['id'] = j['propertyCode'].__str__()
                            del j['propertyCode']

                            j['source_id'] = i['_id'].__str__()
                            j['timestamp'] = i['timestamp']
                            j['district'] = k['district']
                            j['neighborhood'] = k['neighborhood']
                            j['district_id'] = k['district_id']
                            j['neighborhood_id'] = k['neighborhood_id']
                            j['timestamp']=i['filename'][(i['filename'].find('_')+1):(i['filename'].find('_')+11)]
                            try:
                                for kk in j['parkingSpace'].keys():
                                    j[kk]=j['parkingSpace'][kk]
                                del j['parkingSpace']
                            except KeyError:
                                pass
                            try:
                                for kk in j['detailedType'].keys():
                                    j[kk]=j['detailedType'][kk]
                                del j['detailedType']
                            except KeyError:
                                pass
                            try:
                                for kk in j['suggestedTexts'].keys():
                                    j[kk]=j['suggestedTexts'][kk]
                                del j['suggestedTexts']
                            except KeyError:
                                pass

                            data.append(j)
                            duplicates.append(j['id'].__str__()+i['filename'][(i['filename'].find('_')+1):(i['filename'].find('_')+11)])
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
            if j['register_id'].__str__()+j['created'].__str__()[0:4] not in duplicates:

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
                duplicates.append(j['register_id'].__str__()+j['created'].__str__()[0:4])
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
                i['neighborhood_id']=j['neighborhood_id']

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
                    j['id']=id.__str__()
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
                            j['neighborhood_id'] = k['neighborhood_id']

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


def spark_sql_init():
    config = {
        "spark.cores.max": "5",
        "spark.master": "local[*]",
        "spark.python.profile": "false",
        "spark.ui.enabled": "false",
        "spark.executor.extraClassPath": "./sqlite-jdbc-3.34.0.jar",
        "spark.driver.extraClassPath": "./sqlite-jdbc-3.34.0.jar",
        "spark.jars": "./sqlite-jdbc-3.34.0.jar"
    }
    conf = SparkConf()
    for key, value in config.items():
        conf = conf.set(key, value)
    sc = SparkContext(appName="test", conf=conf)

    sqlcontext = SQLContext(sc)
    return sqlcontext,sc

def spark_mongo_init():
    os.environ["SPARK_HOME"] = "C:/Users/Keyser/PycharmProjects/pythonProject/venv/Lib/site-packages/pyspark"
    findspark.init()

    config = {
        "spark.master": "local[*]",
        "spark.python.profile": "false",
        "spark.ui.enabled": "false",
        "spark.executor.extraClassPath": "./mongo-spark-connector_2.12-3.0.1.jar",
        "spark.driver.extraClassPath": "./mongo-spark-connector_2.12-3.0.1.jar",
        'spark.jars': "./mongo-spark-connector_2.12-3.0.1.jar"
    }
    conf = SparkConf()
    for key, value in config.items():
        conf = conf.set(key, value)
    sc = SparkContext(appName="test", conf=conf)
    spark=SparkSession(sc)
    return spark
def spark_local_init():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("SparkByExamples.com") \
        .getOrCreate()
    return spark
def spark_sql(sqlcontext,query):
    url = "jdbc:sqlite:./ExploitationZone"
    return sqlcontext.read.format('jdbc').options(url=url, dbtable=query, driver='org.sqlite.JDBC').load()

def spark_data_model(table_markets, table_income,table_idealista):

    dm = sqlcontext.read.parquet(f'hdfs://pidgey.fib.upc.es:27000/user/bdm/FormattedZone/{table_markets}.parquet').rdd \
        .map(lambda x: (x['neighborhood_id'], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    project_income = ['index_rfd', 'population']
    di = sqlcontext.read.parquet(f'hdfs://pidgey.fib.upc.es:27000/user/bdm/FormattedZone/{table_income}.parquet').rdd \
        .map(lambda x: (x['neighborhood_id'], [float(x[k]) for k in project_income] + [1])) \
        .reduceByKey(lambda x, y: list(map(sum, zip(x, y)))) \
        .mapValues(lambda x: [y / x[-1] for y in x[:-1]]) \
        .sortBy(lambda x: x[1][0])\
        .zipWithIndex()
    size=di.count()
    di1= di.map(lambda x: (x[0][0],['Low',x[0][1][1]]) if (x[1]<size/3) else ((['High',x[0][1][1]]) if (x[1]>size*2/3) else (['Medium',x[0][1][1]])))\
        .persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    project_idealista = ['price', 'size', 'rooms', 'bathrooms', 'priceByArea']
    did = sqlcontext.read.parquet(f'hdfs://pidgey.fib.upc.es:27000/user/bdm/FormattedZone/{table_idealista}.parquet').rdd \
        .map(lambda x: (x['neighborhood_id'], [float(x[k]) for k in project_idealista] + [1])) \
        .reduceByKey(lambda x, y: list(map(sum, zip(x, y)))) \
        .mapValues(lambda x: [y / x[-1] for y in x[:-1]]) \
        .persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    schema = ['neighborhood_id', 'n_markets']  + project_idealista+project_income
    res = dm.join(did) \
        .mapValues(lambda x: [x[0]] + x[1])
    result = res.join(di1) \
        .mapValues(lambda x: x[0] + x[1])\
        .map(lambda x: [x[0]] + x[1])
    dm.unpersist()
    di1.unpersist()
    did.unpersist()
    data = result.collect()

    output=[]
    for x in data:
        output.append(dict(zip(schema,x)))

    return output
def spark_train_model():

    query = "(select * from model_data) as t"
    odata = spark_sql(sqlcontext, query)

    data=odata.drop('neighborhood_id')
    assembler= VectorAssembler().setInputCols(data.schema.names[:-2]+[data.schema.names[-1]]).setOutputCol('features')
    data=assembler.transform(data)
    labelIndexer = StringIndexer(inputCol="index_rfd", outputCol="indexedLabel").fit(data)
    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=3).fit(data)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])
    rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)
    labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",labels=labelIndexer.labels)
    pipeline = Pipeline(stages=[labelIndexer,featureIndexer, rf, labelConverter])
    model = pipeline.fit(trainingData)
    predictions = model.transform(testData)
    predictions.select("predictedLabel", "index_rfd", "features").show(5)
    evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g" % (1.0 - accuracy))
    rfModel = model.stages[2]
    print(rfModel)

    return model

def UploadFormattedZone():
    [lookupIdealista, lookupOpenBCN] = get_lookup(Lookup)
    table_markets = format_bcn_markets(lookupOpenBCN)
    table_income = format_bcn_income(lookupOpenBCN)
    table_idealista = formatted_idealista(lookupIdealista)

    # Subir a MONGO
    #
    # formated_zone['table_idealista'].insert_many(table_idealista)
    # formated_zone['table_income'].insert_many(table_income)
    # formated_zone['table_markets'].insert_many(table_markets)

    # Subir a HDFS en Parquet
    table_markets = pa.Table.from_pandas(pd.DataFrame.from_dict(table_markets))
    table_income = pa.Table.from_pandas(pd.DataFrame.from_dict(table_income))
    table_idealista = pa.Table.from_pandas(pd.DataFrame.from_dict(table_idealista))

    pq.write_table(table_markets, './FormattedZone/table_markets.parquet')
    pq.write_table(table_income, './FormattedZone/table_income.parquet')
    pq.write_table(table_idealista, './FormattedZone/table_idealista.parquet')

    hdfs_client.upload('./FormattedZone/table_markets.parquet', os.path.join('./FormattedZone/table_markets.parquet'), overwrite=True)
    hdfs_client.upload('./FormattedZone/table_income.parquet', os.path.join('./FormattedZone/table_income.parquet'), overwrite=True)
    hdfs_client.upload('./FormattedZone/table_idealista.parquet', os.path.join('./FormattedZone/table_idealista.parquet'), overwrite=True)
    
    os.remove('./FormattedZone/table_markets.parquet')
    os.remove('./FormattedZone/table_income.parquet')
    os.remove('./FormattedZone/table_idealista.parquet')

def spark_data_dashboard(table_markets,table_income,table_idealista):
    
    MV1 = sqlcontext.read.parquet(f'hdfs://pidgey.fib.upc.es:27000/user/bdm/FormattedZone/{table_idealista}.parquet')
    price_column = 'price'
    rooms_column = 'rooms'
    bath_column = 'bathrooms'
    pa_column = 'priceByArea'
    group_column1 = 'neighborhood_id'
    substring_col = 'timestamp'
    district = 'district'
    size_col = 'size'
    result1 = MV1.groupBy(group_column1,col(substring_col).substr(1, 4).alias('Year'),district).agg(mean(price_column).alias('price'),
                    mean(rooms_column).alias('rooms'),
                    mean(bath_column).alias('bathrooms'),
                    count(rooms_column).alias('count'),
                    mean(pa_column).alias('pba'),
                    sum(rooms_column).alias('totalrooms'),
                    sum(bath_column).alias('totalbathrooms'),
                    mean(size_col).alias('mean_size'))
    
    MV2 = sqlcontext.read.parquet(f'hdfs://pidgey.fib.upc.es:27000/user/bdm/FormattedZone/{table_markets}.parquet')
    count_column = 'register_id'
    group_column1 = 'neighborhood_id'
    district_column = 'district'
    result2 = MV2.groupBy(group_column1,district_column).agg(count(count_column).alias('numMarkets'))

    MV3 = sqlcontext.read.parquet(f'hdfs://pidgey.fib.upc.es:27000/user/bdm/FormattedZone/{table_income}.parquet')
    avg_column = 'index_rfd'
    group_column1 = 'neighborhood_id'
    substring_col = 'timestamp'
    result3 = MV3.groupBy(group_column1,col(substring_col).substr(1, 4).alias('Year')).agg(mean(avg_column).alias('average'))

    return result1,result2,result3

def UploadExploitationZone():
    # table_idealista = list(formated_zone['table_idealista'].find({}))
    # table_income = list(formated_zone['table_income'].find({}))
    # table_markets = list(formated_zone['table_markets'].find({}))
    print('start')
    data_model = spark_data_model('table_markets', 'table_income', 'table_idealista')
    print('model')
    data_dashboard = spark_data_dashboard('table_markets', 'table_income', 'table_idealista')
    ## CREAR Y SUBIR TABLAS

    formatted_path = './ExploitationZone'
    connection = create_connection(formatted_path)

    create_data_model = """
    CREATE TABLE IF NOT EXISTS model_data (
        neighborhood_id TEXT PRIMARY KEY NOT NULL,
        n_markets INTEGER, 
        price REAL, 
        size REAL, 
        rooms REAL, 
        bathrooms REAL,
        priceByArea REAL, 
        index_rfd TEXT, 
        population REAL
    );
    """
    execute_query(connection, create_data_model)

    err = 0
    for x in data_model:
        columns = ', '.join(x.keys())
        placeholders = ':' + ', :'.join(x.keys())
        query = 'INSERT OR REPLACE INTO model_data (%s) VALUES (%s)' % (columns, placeholders)
        connection.execute(query, x)
        connection.commit()
        try:
            connection.execute(query, x)
            connection.commit()

        except Error as e:
            err += 1
    print(f'Number of failed inserts {err}')

    data_dashboard[0].toPandas().to_sql(con=connection,name='mvidealista',if_exists='append',index=['neighborhood_id,year'])
    data_dashboard[1].toPandas().to_sql(con=connection,name='mvmarkets',if_exists='append',index=['neighborhood_id_ind'])
    data_dashboard[2].toPandas().to_sql(con=connection,name='mvincome',if_exists='append',index=['neighborhood_id_ind'])

#os.environ["SPARK_HOME"] = "C:/Users/Keyser/PycharmProjects/pythonProject/venv/Lib/site-packages/pyspark"
findspark.init()
[sqlcontext, sc] = spark_sql_init()

def dashboard():

    exploitation_path = './ExploitationZone'
    connection = create_connection(exploitation_path)

    query = 'SELECT m.district, m.numMarkets,  avg(i.average) FROM mvmarkets m inner join mvincome i on i.neighborhood_id = m.neighborhood_id GROUP BY m.neighborhood_id'
    res1 = connection.execute(query)
    dataPlot1 = res1.fetchall()

    query = 'SELECT (m.rooms+m.bathrooms) , m.pba as rph, m.district FROM mvidealista m'
    res2 = connection.execute(query)
    dataPlot2 = res2.fetchall()

    query = 'SELECT AVG(m.price/(m.totalrooms+m.totalbathrooms)) as PricePerRoom, avg(m.pba), m.district FROM mvidealista m  GROUP BY m.district'
    res3 = connection.execute(query)
    dataPlot3 = res3.fetchall()

    query = 'SELECT avg(m.mean_size),m.district FROM mvidealista m  GROUP BY m.district'
    res4 = connection.execute(query)
    dataPlot4 = res4.fetchall()

    fig, axs = plt.subplots(2, 2, figsize=(20,15))

    plt1 = sns.barplot(x = [x[0]for x in dataPlot1],y=[x[1]for x in dataPlot1],ax=axs[0,0], palette="viridis",errorbar=None)
    plt1.tick_params(axis ='x',rotation=30)
    plt1.set(title="Number markets per Distirct")

    plt2 = sns.scatterplot(x = [x[0]for x in dataPlot2],y=[x[1]for x in dataPlot2], hue=[x[2]for x in dataPlot2],ax=axs[0,1])
    plt2.set(xlabel = "Average number of rooms", ylabel = "Average price by area", title= "Number of rooms by price by area")

    plt3 = sns.scatterplot(x = [x[0]for x in dataPlot3],y=[x[1]for x in dataPlot3],hue=[x[2]for x in dataPlot3],s=100 ,ax=axs[1,0])
    plt3.tick_params(axis ='x',rotation=30,size = 1)
    plt3.set(xlabel='Price per Room', ylabel='Price by Area',xlim=(0,100000),title='Price per Room vs Price by Area')

    plt4 = sns.barplot(x = [x[0]for x in dataPlot4],y=[x[1]for x in dataPlot4],ax=axs[1,1],errorbar=None)
    plt4.tick_params(axis ='x',rotation=30,size = 1)
    plt4.set(xlabel='Size(m^2)', ylabel='District',title='Size per district')

    plt.savefig('dashboard.png')
    plt.show()

if __name__ == '__main__':
    #UploadFormattedZone()
    UploadExploitationZone()
    model=spark_train_model()
    dashboard()
    sc.stop()

# create_idealista_table = """
# CREATE TABLE IF NOT EXISTS idealista (
#     id INTEGER PRIMARY KEY,
#     thumbnail TEXT,
#     externalReference TEXT,
#     numPhotos INTEGER,
#     floor INTEGER,
#     price REAL,
#     timestamp TEXT
#     propertyType TEXT,
#     operation TEXT,
#     size REAL,
#     exterior TEXT,
#     rooms INTEGER,
#     bathrooms INTEGER,
#     address TEXT,
#     province TEXT,
#     municipality TEXT,
#     district TEXT,
#     country TEXT,
#     neighborhood TEXT,
#     latitude REAL,
#     longitude REAL,
#     showAddress TEXT,
#     url TEXT,
#     distance INTEGER,
#     hasVideo TEXT,
#     status TEXT,
#     newDevelopment TEXT,
#     newDevelopmentFinished TEXT,
#     hasLift TEXT,
#     parkingSpace TEXT,
#     priceByArea REAL,
#     hasPlan TEXT,
#     has3DTour TEXT,
#     has360 TEXT,
#     hasStaging TEXT,
#     topNewDevelopment TEXT,
#     source_id TEXT,
#     timestamp TEXT,
#     district_id TEXT,
#     neighborhood_id TEXT,
#     hasParkingSpace TEXT,
#     isParkingSpaceIncludedInPrice TEXT,
#     parkingSpacePrice REAL,
#     typology TEXT,
#     subTypology TEXT,
#     subtitle TEXT,
#     title TEXT
# );
# """
# create_income_table = """
# CREATE TABLE IF NOT EXISTS income (
#     id INTEGER PRIMARY KEY,
#     year INTEGER,
#     population TEXT,
#     index_rfd REAL,
#     source_id TEXT,
#     timestamp TEXT,
#     district TEXT,
#     neighborhood TEXT,
#     district_id TEXT,
#     neighborhood_id TEXT
# );
# """
# create_market_table = """
# CREATE TABLE IF NOT EXISTS markets (
#     register_id INTEGER PRIMARY KEY,
#     name TEXT,
#     created TEXT,
#     modified TEXT,
#     status TEXT,
#     core_type TEXT,
#     core_type_name TEXT,
#     body TEXT,
#     ical TEXT,
#     place TEXT,
#     district_id TEXT,
#     neighborhood_id TEXT,
#     address_name TEXT,
#     address_id INTEGER,
#     start_street_number INTEGER,
#     end_street_number INTEGER,
#     street_number_1 TEXT,
#     street_number_2 TEXT,
#     level INTEGER,
#     zip_code INTEGER,
#     province TEXT,
#     town TEXT,
#     country TEXT,
#     position INTEGER,
#     position_relative TEXT,
#     main_address TEXT,
#     roadtype_name TEXT,
#     roadtype_id TEXT,
#     hide_address TEXT,
#     full_path TEXT,
#     dependency_group INTEGER,
#     parent_id INTEGER,
#     tree_id INTEGER,
#     asia_id INTEGER,
#     x_coord REAL,
#     y_coord REAL,
#     source_id TEXT,
#     timestamp TEXT,
#     district TEXT,
#     neighborhood TEXT,
#     n_warnings INTEGER
# );
# """


