# For each line
# Node is in Parent (7)
# Attribute is in Attribute (1)
# Description in Description (2)
# ENUM in Valid Values (3)
# Required in Required (6)
# CDE in source (9)
# Type in Validation rules (10)

import pandas as pd
import argparse
import yaml
import requests
import json

def readConfigs(yamlfile):
    with open(yamlfile) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
    return configs

def parseModel(df, nodelist):
    finaljson = {"Handle":"HTAN"}
    workingjson = {}
    for node in nodelist:
        temp_df = df[df['Parent'] == node]
        attributes = temp_df.Attribute.to_list()
        #There appears to be errors in the csv, so eliminate any "node" that doesn't have at least one property
        if len(attributes) >= 1:
            workingjson[node] = {"Props" : attributes}
    finaljson['Nodes'] = workingjson
    return finaljson

def makeCleanList(string, delimiter):
    workinglist = string.split(delimiter)
    cleanlist = []
    for entry in workinglist:
        cleanlist.append(entry.strip())
    return cleanlist

def getCDEDef(cdeid, version):
    url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"+str(cdeid)+"?version="+str(version)
    headers = {'accept':'application/json'}
    try:
        results = requests.get(url, headers = headers)
    except requests.exceptions.HTTPError as e:
        pprint.pprint(e)
    if results.status_code == 200:
        results = json.loads(results.content.decode())
        #cdename = results['DataElement']['preferredName']
        definition = results['DataElement']['preferredName']
    else:
        #cdename = 'caDSR Name Error'
        definition = "caDSR Error"
    #return cdename, definition
    return definition

def parseProps(df):
    workingjson = {}
    for index, row in df.iterrows():
        prop = row['Attribute']
        desc = row['Description']
        req = row['Required']
        cdeline = row['Source']
        workingjson[prop] = {'Desc':desc, "Req":req}
        #print(type(row['Validation Rules']))
        if type(row['Valid Values']) is not float:
            workingjson[prop]['Enum']  = makeCleanList(row['Valid Values'], ',')
        if type(row['Validation Rules']) is not float:
            workingjson[prop]['Type'] = row['Validation Rules']
        if type(cdeline)is str:
            if "cadsr.cancer.gov" in cdeline:
                linestuff = cdeline.split("=")
                version = linestuff[3]
                cdeid = linestuff[2].split("&")[0]
                definition = getCDEDef(cdeid, version)
                workingjson[prop]['Term'] =  [{"Code":cdeid, "Origin":"caDSR", "Value": definition, "Version":version}]
    finaljson = {}
    finaljson['PropDefinitions']   = workingjson
    return finaljson          

def writeYAML(filename, jsonthing):
    with open(filename, 'w') as f:
        yaml.dump(jsonthing, f)
    f.close()
        

def main(args):
    configs = readConfigs(args.configfile)
    htan_csv_df = pd.read_csv(configs['htan_csv_file'])

    #Get the unique properties
    nodes = htan_csv_df.Parent.unique()

    #Create the model JSON object (list of properties per node)
    mdfjson = parseModel(htan_csv_df, nodes)
    writeYAML(configs['mdf_model_file'], mdfjson)

    #Now the fun stuff, make the properties file
    propjson = parseProps(htan_csv_df)
    writeYAML(configs['mdf_model_props_file'], propjson)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)