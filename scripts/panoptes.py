import argparse
import bson
from bson import ObjectId
from datetime import datetime
from datetime import date
import json
from jsonschema import validate, Draft202012Validator
from jsonschema.exceptions import best_match
import locale
locale.setlocale(locale.LC_ALL, 'nl_NL') 
import pprint as pp
from pymongo import MongoClient, ReplaceOne, InsertOne, DeleteOne
import re
import sys


def export(connection,schema,filename_out):
    for coll_name in connection['main'].list_collection_names():
        for items in connection['main'][coll_name].find(projection=proj):
            tenant = {}
            for item in items:
                tenant[item] = items[item]
            stderr(tenant)
            try:
                name = tenant['name']
            except:
                #stderr(f'no name in tenant: {tenant}')
                continue
            datasets = []
            dataset = {}
            for item in connection[name]['datasets'].find(): #projection=proj):
                dataset = item
                item['_id'] = str(item['_id'])
#                stderr(f'dataset(1): {dataset}')
                break
            for item_2 in connection[name].list_collection_names():
                if item_2=='datasets':
                    continue
                ds_parts = []
                for doc in connection[name][item_2].find(): #projection=proj):
                    doc['_id'] = str(doc['_id'])
                    ds_parts.append(doc)
                dataset[f'{item_2}'] = ds_parts
#                stderr(f'dataset(2): {dataset}')
#            for item in dataset:
#                print(dataset)
            datasets.append(dataset)
            tenant['datasets'] = datasets
            res.append(tenant)
    with open(filename_out,'w') as uitvoer:
        json.dump(res,uitvoer,indent=2)


def import_file(connection,schema,invoer):
    with open(invoer) as inv:
        json_data = json.load(inv)
    try:
        validate(json_data,schema)
    except Exception as err:
        stderr(f'{invoer} does not validate against schema:')
        stderr(err)
        return
    stderr('import file; to be developped')

    pp.pprint(json_data,indent=2)

    for item in json_data:
        # name en domain naar "main"
        name =  item['name']
        orig = {'name': name }
        update = { 'name': item['name'],
                   'domain': item['domain'] }
        try:
            result = connection.main.tenants.bulk_write([ReplaceOne(orig,update, upsert=True)])
            stderr(f'update tenant succeeded:\n{result.modified_count}\n')
        except Exception as err:
            stderr('update tenant failed:')
            stderr(err)

        handle_datasets(name,json_data[0]['datasets'],connection)


def handle_datasets(name,datasets,connection):
    # updates
    stderr('datasets')
    requests = []
    updated_datasets = []
    for dataset in datasets:
        collections = {}
        update = dict(dataset)
        update.pop('facets')
        update.pop('result_properties')
        update.pop('detail_properties')
        updated_datasets.append(update)
        filter = { "name" : update['name'],
                   "tenant_name": update['tenant_name'] }
        requests.append(ReplaceOne(filter,update, upsert=True))
    try:
        result = connection[name]['datasets'].bulk_write(requests)
        stderr(f'update datasets succeeded\n{result.modified_count}\n')
    except Exception as err:
                    stderr('update datasets failed:')
                    stderr(err)
                    stderr(f'filter: {filter}')
                    stderr(f'orig:\n{orig}')
                    stderr(f'update:\n{update}')

#    verwijder datasets

    requests = []
    for item in connection[name]['datasets'].find(projection=proj):
        print(item)
        if not item in updated_datasets:
            requests.append(DeleteOne(item))
    stderr(requests)
    try:
            result = connection.main[name]['datasets'].bulk_write(requests)
            stderr(f'''delete datasets succeeded:
deleted:  {result.deleted_count}

''')
#            stderr(f'orig:\n{orig}')
#            stderr(f'update:\n{update}\n')
    except Exception as err:
            if f'{err}'=='No operations to execute':
                stderr(err)
            else:
                stderr('delete datasets failed:')
                stderr(err)
                stderr(f'orig:\n{orig}')
                stderr(f'update:\n{update}')
                exit(1)

    for coll_name in ['facets','result_properties','detail_properties']:
        handle_mutations(coll_name,datasets,name,connection)


def handle_mutations(coll_name,datasets,name,connection):
    stderr(coll_name)
    for dataset in datasets:
        requests = []
        for update in dataset[coll_name]:
            filter = { "name" : update['name'],
                       "dataset_name": dataset['name'] }
            requests.append(ReplaceOne(filter,update, upsert=True))
        try:
            result = connection[name][coll_name].bulk_write(requests)
            stderr(message_succeed(coll_name,dataset["name"],result,'update'))
        except Exception as err:
            stderr(f'''update {coll_name} failed:
{err}
filter: {filter}
update: {update}
''')

    stderr(f'delete {coll_name} from: {str(connection[name]["facets"].find(projection=proj))}')
    requests = []
    for facet in connection[name][coll_name].find(projection=proj):
        if not facet in dataset[coll_name]:
            requests.append(DeleteOne(facet))
    stderr(f'requests: {requests}')
    try:
        result = connection.main[name][coll_name].bulk_write(requests)
        stderr(message_succeed(coll_name,dataset["name"],result,'delete'))
    except Exception as err:
            if f'{err}'=='No operations to execute':
                stderr(err)
            else:
                stderr('delete {coll_name} failed:')
                stderr(err)
                stderr(f'orig:\n{orig}')
                stderr(f'update:\n{update}')
                exit(1)

def message_succeed(coll_name,dataset_name,result,type):
     return f'''{type} {coll_name} for {dataset_name} succeeded
modified: {result.modified_count}
inserted: {result.inserted_count}
deleted:  {result.deleted_count}
'''

def message_failed():
    return f'''
'''



def stderr(text,nl="\n"):
    sys.stderr.write(f"{text}{nl}")

def end_prog(code=0):
    stderr(datetime.today().strftime("einde: %H:%M:%S"))
    if code!=0:
        stderr(f'program ended with code {code}')
    exit(code)

def arguments():
    ap = argparse.ArgumentParser(description='Export a json file from mongoDB or import a json file into mongoDB"')
    ap.add_argument('-f', '--file',
                    type=str,
                    default='sync.json',
                    help="import/export file")
    ap.add_argument('-c', '--choice',
                    type=str,
                    help="Choose import or export",
                    choices=['import', 'export'],
                    default = 'export')
    ap.add_argument("-d", "--debug", action="store_true")
    args = ap.parse_args()
    return args

if __name__ == "__main__":
    stderr("start: {}".format(datetime.today().strftime("%H:%M:%S")))
    args = arguments()
    filename = args.file
    choice = args.choice
    debug = args.debug
    if debug:
        stderr(f'debug: {debug}')
    config = {
            "user": "nraboy",
            "password": "password1234",
            "host": "localhost:27017"
            }
    proj = {'_id':False}
    
    connection = MongoClient("mongodb://localhost:27017")
    #connection = MongoClient(f"mongodb://{config['user']}:{config['password']}@{config['host']}")

    #with open('sync.json') as invoer:
    with open('schema.json') as invoer:
        schema = json.load(invoer)
    
    res = []
    dbs = connection.list_database_names()

    if choice=='export':
        if debug:
            uitvoer = 'export_res.json'
        else:
            uitvoer = filename
        export(connection,schema,uitvoer)
    elif choice=='import':
        if debug:
            invoer = 'import.json'
        else:
            invoer = filename
        import_file(connection,schema,invoer)

    end_prog(0)

