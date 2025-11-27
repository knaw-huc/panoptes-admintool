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
from pymongo import MongoClient
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
                datasets.append(item)
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
    try:
        with open(filename_out,'w') as uitvoer:
            json.dump(res,uitvoer,indent=2)
    except Exception as err:
        stderr('result does not validate against schema')
        stderr(err)
        stderr(best_match(Draft202012Validator(schema).iter_errors(res)).message)


def import_file(connection,schema,invoer):
    with open(invoer) as inv:
        json_data = json.load(inv)
    try:
        validate(json_data,schema)
    except Exception as err:
        stderr('result does not validate against schema')
        stderr(err)
        return

    stderr('import file to be developped')


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

