#Create MDF files from the JSON schema and decorate with CDEs from the csv file
import argparse
import pandas as pd
import json
import requests
import yaml
import pprint
import sys

def readConfigs(yamlfile):
    with open(yamlfile) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
    return configs

def readSchema(jsonld):
    with open(jsonld, 'r') as f:
        schema = json.loads(f.read())
    return schema

def buildModel(jsonthing):
    mdfjson = {}
    for entry in jsonthing:
        #pprint.pprint(entry)
        name = entry['sms:displayName']
        for subclass in entry['rdfs:subClassOf']:
            if subclass['@id'] in mdfjson.keys():
                mdfjson[subclass['@id']].append(name)
            else:
                temp = [name]
                mdfjson[subclass['@id']] = temp
        
        #req = entry['sms:required']
       # type = entry['@type']
       # definition = entry['rdfs:comment']
       # enums = []
        #print(entry)
        #print(name)
        #print(req)
        #print(type)
        #print(definition)
        #sys.exit(0)
        #if 'sms:rangeIncludes' in entry.keys():
        #    for enum in entry['sms:rangeIncludes']:
        #        enums.append(enum['@id'])
        #if len(enums) >= 1:
        #    mdfjson[name] = {"Desc":definition, "Props":enums}
        #else:
        #    mdfjson[name] = {"Desc":definition}
        #mdfjson[name] = {"Desc": definition}
    #pprint.pprint(mdfjson)
    return mdfjson

def annotateProps(jsonmodel, csv_df):
    for node, properties in jsonmodel.items():
        for property in properties:
            if (csv_df['Attribute'] == property).any():
                print(csv_df[csv_df['Attribute'] == property])

def writeYAML(filename, jsonthing):
    with open(filename, 'w') as f:
        yaml.dump(jsonthing, f)
    f.close()

def main(args):
    #Read the config file
    configs = readConfigs(args.configfile)
    #Read the JSONLD schema file
    htanschema = readSchema(configs['jsonschema'])
    #pprint.pprint(htanschema)
    graph = htanschema['@graph']
    modeljson = buildModel(graph)
    #pprint.pprint(modeljson)
    #Read the csv into a dataframe
    htan_df = pd.read_csv(configs['csvschema'])

    annotateProps(modeljson, htan_df)

    writeYAML(configs['mdf_model_file'], modeljson)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    args = parser.parse_args()

    main(args)