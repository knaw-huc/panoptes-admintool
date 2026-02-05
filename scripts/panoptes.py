import argparse
from datetime import datetime
from datetime import date
import json
from jsonschema import validate, Draft202012Validator
from jsonschema.exceptions import best_match
import locale
locale.setlocale(locale.LC_ALL, 'nl_NL') 
from pymongo import MongoClient, ReplaceOne, InsertOne, DeleteOne
import re
import sys


def export(connection,schema,filename_out,select_tenant):
    for coll_name in connection['main'].list_collection_names():
        for items in connection['main'][coll_name].find(projection=proj):
            tenant = {}
            for item in items:
                tenant[item] = items[item]
            try:
                name = tenant['name']
                if select_tenant!='all':
                    if name!=select_tenant:
                        continue
            except:
                #stderr(f'no name in tenant: {tenant}')
                continue
            datasets = []
            dataset = {}
            for item in connection[name]['datasets'].find(projection=proj):
                dataset = item
                try:
                    item['_id'] = str(item['_id'])
                except:
                    pass
                break
            for item_2 in connection[name].list_collection_names():
                if item_2=='datasets':
                    continue
                ds_parts = []
                for doc in connection[name][item_2].find(projection=proj):
                    try:
                        doc['_id'] = str(doc['_id'])
                    except:
                        pass
                    ds_parts.append(doc)
                dataset[f'{item_2}'] = ds_parts
            datasets.append(dataset)
            tenant['datasets'] = datasets
            res.append(tenant)
    with open(filename_out,'w') as uitvoer:
        json.dump(res,uitvoer,indent=2)


def import_file(connection,schema,invoer,select_tenant):
    with open(invoer) as inv:
        json_data = json.load(inv)
    try:
        validate(json_data,schema)
    except Exception as err:
        stderr(f'{invoer} does not validate against schema:')
        stderr(err)
        return

    stderr('tenants')
    updates_tenants = []
    for item in json_data:
        name =  item['name']
        orig = {'name': name }
        if select_tenant!='all' and select_tenant!=name:
            continue
        update = { 'name': item['name'],
                   'domain': item['domain'] }
        updates_tenants.append(update)
        try:
            result = connection.main.tenants.bulk_write([ReplaceOne(orig,update, upsert=True)])
            stderr(f'update tenant succeeded: {result.modified_count}')
        except Exception as err:
            stderr('update tenant failed:')
            stderr(err)
        handle_datasets(name,item['datasets'],connection)

        #
        # delete tenants
        #
    for item in connection.main.tenants.find(projection=proj):
        tenant_name = item['name']
        if select_tenant!='all' and select_tenant!=tenant_name:
            continue
        tenant = { 'name': tenant_name,
                   'domain': item['domain'] }
        if not tenant in updates_tenants:
            # drop de collections in tenant
            for coll_name in connection[tenant_name].list_collection_names():
                connection[tenant_name].drop_collection(coll_name)
            try:
                result = connection.main.drop_collection(tenant_name)
                filter = { 'name': tenant_name }
                connection.main.tenants.bulk_write([DeleteOne(filter)])
                #stderr(f'OK?: {result}')
            except Exception as err:
                if f'{err}'=='No operations to execute':
                    stderr(err)
                else:
                    stderr(f'delete tenant {tenant_name} failed:')
                    stderr(err)


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
        stderr(f'update datasets succeeded: {result.modified_count}')
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
    try:
            result = connection.main[name]['datasets'].bulk_write(requests)
            stderr(f'delete datasets succeeded')
    except Exception as err:
            if f'{err}'=='No operations to execute':
                stderr(err)
            else:
                stderr('delete datasets failed:')
                stderr(err)
                stderr(f'orig:\n{orig}')
                stderr(f'update:\n{update}')
                exit(1)

    for dataset in datasets:
        for coll_name in ['facets','result_properties','detail_properties']:
            handle_mutations(coll_name,dataset,name,connection)


def handle_mutations(coll_name,dataset,tenant_name,connection):
    stderr(coll_name)
    requests = []
    list_updates = []
    for update in dataset[coll_name]:
            filter = { "name" : update['name'],
                       "dataset_name": dataset['name'] }
            list_updates.append(update['name'])
            requests.append(ReplaceOne(filter,update, upsert=True))
    try:
        if requests!=[]:
            result = connection[tenant_name][coll_name].bulk_write(requests)
            stderr(message_succeed(coll_name,dataset["name"],result,'update'))
    except Exception as err:
            stderr(f'''update {coll_name} failed:
{err}
filter: {filter}
update: {update}
''')

    requests = []
    if list_updates==[]:
        # delete collection binnen deze tenant
        res = connection[tenant_name].drop_collection(coll_name)
        if res:
            stderr(f'delete {coll_name} from {tenant_name} succeeded')
            return

    for item in connection[tenant_name][coll_name].find(projection=proj):
        filter = {
               "name" : item['name'],
               "dataset_name": dataset['name']   
                }
        if not item['name'] in list_updates:
            requests.append(DeleteOne(filter))
    try:
        result = connection.main[tenant_name].bulk_write(requests)
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
                    help="import/export file (default=sync.json)")
    ap.add_argument('-c', '--choice',
                    type=str,
                    help="Choose import or export (default=export)",
                    choices=['import', 'export'],
                    default = 'export')
    ap.add_argument('-t', '--tenant',
                    type=str,
                    default='all',
                    help="Give tenant name to import or export on (default=all)")
    ap.add_argument("-d", "--debug", action="store_true")
    args = ap.parse_args()
    return args

if __name__ == "__main__":
    stderr("start: {}".format(datetime.today().strftime("%H:%M:%S")))
    args = arguments()
    filename = args.file
    choice = args.choice
    tenant = args.tenant
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

    with open('schema.json') as invoer:
        schema = json.load(invoer)
    
    res = []
    dbs = connection.list_database_names()

    if choice=='export':
        if debug:
            uitvoer = 'export_res.json'
        else:
            uitvoer = filename
        export(connection,schema,uitvoer,tenant)
    elif choice=='import':
        if debug:
            invoer = 'import.json'
        else:
            invoer = filename
        import_file(connection,schema,invoer,tenant)

    end_prog(0)

