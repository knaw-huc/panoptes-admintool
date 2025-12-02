import argparse
import bson
from datetime import datetime
from datetime import date
import json
from jsonschema import validate, Draft202012Validator
from jsonschema.exceptions import best_match
import locale
locale.setlocale(locale.LC_ALL, 'nl_NL') 
import pandas as pd
import pprint as pp
from pymongo import MongoClient, ReplaceOne, InsertOne
import re
import sys


def export(connection,schema,filename_out):
    for coll_name in connection['main'].list_collection_names():
        for items in connection['main'][coll_name].find(projection=proj):
            tenant = {}
            for item in items:
                tenant[item] = items[item]
            name = tenant['name']
            datasets = []
            dataset = {}
            for item in connection[name]['datasets'].find(projection=proj):
                dataset = item
                break
            for item_2 in connection[name].list_collection_names():
                if item_2=='datasets':
                    continue
                ds_parts = []
                for doc in connection[name][item_2].find(projection=proj):
                    ds_parts.append(doc)
                dataset[f'{item_2}'] = ds_parts
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
    # name en domain naar "main"
    name =  json_data[0]['name']
    orig = {'name': name }
    update = { 'name': json_data[0]['name'],
                'domain': json_data[0]['domain'] }
    try:
        result = connection.main.tenants.bulk_write([ReplaceOne(orig,update, upsert=True)])
        stderr(f'update tenant succeeded:\n{result.modified_count}\n')
    except Exception as err:
        stderr('update tenant failed:')
        stderr(err)
        exit(1)

    datasets = json_data[0]['datasets']

    # datasets splitsen:
    facets = datasets[0].pop('facets')
    result_properties = datasets[0].pop('result_properties')
    detail_properties = datasets[0].pop('detail_properties')

    # updates
    stderr('datasets')
    for item in connection[name]['datasets'].find(projection=proj):
        if not 'datasets' in item:
            continue
        orig =  { 'datasets': item['datasets'] }
#        stderr(f'orig:\n{orig}')
#        stderr(f'item: {item["datasets"]}')
        for old in item['datasets']:
            filter = { "name" : old['name'],
                   "tenant_name": old['tenant_name'] }

            for nieuw in datasets:
                update = nieuw
                try:
                    result = connection[name]['datasets'].bulk_write(
                            [ReplaceOne(filter,nieuw, upsert=True)])
                    stderr(f'update datasets succeeded\n{result.modified_count}\n')
                except Exception as err:
                    stderr('update datasets failed:')
                    stderr(err)
                    stderr(f'filter: {filter}')
                    stderr(f'orig:\n{orig}')
                    stderr(f'update:\n{update}')
                    exit(1)

    # facets
    stderr('facets')
    old_facets = []
    for item in connection[name].list_collection_names():
        if item=='facets':
            stderr(f'item: {item}')
            for doc in connection[name][item].find(projection=proj):
                old_facets.append(doc)
            break
    orig =  { 'facets': old_facets }
    update = { 'facets' : facets }
    try:
        result = connection.main[name].bulk_write([ReplaceOne(filter,update, upsert=True)])
        stderr(f'update facets succeeded:\n{result.modified_count}\n')
    except Exception as err:
        stderr('update facets failed:')
        stderr(err)
        stderr(f'orig:\n{orig}')
        stderr(f'update:\n{update}')
        exit(1)

#    result_properties
    stderr('result_properties')
    orig =  []
    for item in connection[name].list_collection_names():
        if item=='result_properties':
            for doc in connection[name][item].find(projection=proj):
                orig.append(doc)
            break
 
    update = { 'result_properties': result_properties }
    try:
        result = connection.main[name].bulk_write([ReplaceOne(filter,update, upsert=True)])
        stderr(f'update result_properties succeeded\n{result.modified_count}\n')
    except Exception as err:
        stderr('update result_properties failed:')
        stderr(err)
        stderr(f'orig:\n{orig}')
        stderr(f'update:\n{update}')
        exit(1)

    #    detail_properties
    stderr('detail_properties')
    orig =  []
    for item in connection[name].list_collection_names():
        if item=='detail_properties':
            for doc in connection[name][item].find(projection=proj):
                orig.append(doc)
            break
 
    update = { 'detail_properties': detail_properties }
    try:
        result = connection.main[name].bulk_write([ReplaceOne(filter,update, upsert=True)])
        stderr(f'update detail_properties succeeded\n{result.modified_count}\n')
    except Exception as err:
        stderr('update detail_properties failed:')
        stderr(err)
        stderr(f'orig:\n{orig}')
        stderr(f'update:\n{update}')
        exit(1)


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

