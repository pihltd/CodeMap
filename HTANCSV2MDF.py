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
from crdclib import crdclib

#def readConfigs(yamlfile):
#    with open(yamlfile) as f:
#        configs = yaml.load(f, Loader=yaml.FullLoader)
#    return configs

def parseModel(df, nodelist):
    # TODO: Figure out how to handle attributes with no parent.
    finaljson = {"Handle":"HTAN"}
    workingjson = {}
    for node in nodelist:
        temp_df = df[df['Parent'] == node]
        attributes = temp_df.Attribute.to_list()
        #There appears to be errors in the csv, so eliminate any "node" that doesn't have at least one property
        if len(attributes) >= 1:
            workingjson[node] = {"Props" : attributes}
    finaljson['Nodes'] = workingjson
    finaljson['Relationships'] = None
    return finaljson

def makeCleanList(string, delimiter):
    workinglist = string.split(delimiter)
    cleanlist = []
    for entry in workinglist:
        cleanlist.append(entry.strip())
    #Remove duplications
    cleanlist = list(set(cleanlist))
    return cleanlist

def getCDEDef(jsonthing):
    if jsonthing['status'] == "sucess":
        return jsonthing['DataElement']['longName']
    else:
        return f"No defintiion given with status {jsonthing['status']}"

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
                #definition = getCDEDef(cdeid, version)
                cadsrjson = crdclib.getCDERecord(cdeid,version)
                definition = getCDEDef(cadsrjson)
                workingjson[prop]['Term'] =  [{"Code":cdeid, "Origin":"caDSR", "Value": definition, "Version":version}]
            elif "gdc.cancer.gov" in cdeline:
                workingjson[prop]['Term'] = [{"Code":cdeline, "Origin": "GDC", "Value": "Non caDSR entry", "Version": "Unknown"}]
            elif "miti" in cdeline:
                workingjson[prop]['Term'] = [{"Code":cdeline, "Origin": "MITI", "Value": "Non caDSR entry", "Version": "Unknown"}]
            elif "purl.obolibrary.org" in cdeline:
                workingjson[prop]['Term'] = [{"Code":cdeline, "Origin": "OBO", "Value": "Non caDSR entry", "Version": "Unknown"}]
            elif "dataservice.datacommons.cancer.gov" in cdeline:
                workingjson[prop]['Term'] = [{"Code":cdeline, "Origin": "CDS", "Value": "Non caDSR entry", "Version": "Unknown"}]
            elif "google" in cdeline:
                workingjson[prop]['Term'] = [{"Code":cdeline, "Origin": "HTAN", "Value": "Non caDSR entry", "Version": "Unknown"}]
            elif "humancellatlas" in cdeline:
                workingjson[prop]['Term'] = [{"Code":cdeline, "Origin": "HTAN", "Value": "Non caDSR entry", "Version": "Unknown"}]
            else:
                workingjson[prop]['Term'] = [{"Code":cdeline, "Origin": "Somewhere", "Value": "Non caDSR entry", "Version": "Unknown"}]
                
    finaljson = {}
    finaljson['PropDefinitions']   = workingjson
    return finaljson          

#def writeYAML(filename, jsonthing):
#    with open(filename, 'w') as f:
#        yaml.dump(jsonthing, f)
#    f.close()
        

def main(args):
    configs = crdclib.readYAML(args.configfile)
    #configs = readConfigs(args.configfile)
    htan_csv_df = pd.read_csv(configs['csvschema'])

    #Get the unique properties
    nodes = htan_csv_df.Parent.unique()

    #Create the model JSON object (list of properties per node)
    mdfjson = parseModel(htan_csv_df, nodes)
    #writeYAML(configs['mdf_model_file'], mdfjson)
    crdclib.writeYAML(configs['mdf_model_file'], mdfjson)

    #Now the fun stuff, make the properties file
    propjson = parseProps(htan_csv_df)
    crdclib.writeYAML(configs['mdf_props_file'], propjson)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)