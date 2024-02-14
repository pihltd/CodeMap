import argparse
import yaml
import re
import pandas as pd
import pprint
import sys
#This version will attempt to future proof changes to the column names by using a dictionary where the key is the Excel column ID
#TODO:  Make sure nodes are referred to as Nodes and properties as Properties in the Element Type and Characteristic Type columns

def readConfigs(yamlfile):
    with open(yamlfile) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
    return configs

def buildConfigConstants(configjson, codemapdict):
    constants = {}
    for key, value in configjson['modelinfo'].items():
        constants[codemapdict[key]] = value
    return constants

def parseMDF(mdffile,type):
    with open(mdffile) as f:
        modeldict = yaml.load(f, Loader=yaml.FullLoader)

    if type == "props":
        modeldict = modeldict['PropDefinitions']
        return modeldict
    elif type == "model":
        modeldict = modeldict['Nodes']
        return modeldict


def cleanline(text):
    if isinstance(text, str):
        text = re.sub(r'\W+',' ',text)
    return text


def buildLine(linedict, fieldlist):
    line = []
    for field in fieldlist:
        if field in linedict:
            line.append(linedict[field])
        else:
            line.append("\t")
    return line


def lineSet(node, configs, codemapdict):
    line = {}
    line = buildConfigConstants(configs, codemapdict)
    #Element Long Name
    line[codemapdict['h']] = node
    #Element Physical Name
    line[codemapdict['i']] = node
    return line

#def nodeSet(node, configs, codemapdict):
#    line = {}
#    line = buildConfigConstants(configs, codemapdict)
#    #Element Long Name
#    line[codemapdict['h']] = node
#    #Element Physical Name
#    line[codemapdict['i']] = node
#    #Element Type
#    line[codemapdict['k']] = 'Node'
#    #Characteristic Type
#    line[codemapdict['p']] = 'Node'
#    return line


def main(args):
    codemapdict = {"a":"DO NOT USE","b":"Batch User","c":"Batch Name","d":"Seq ID","e":"*Model Name","f":"*Model ID","g":"*Model Version","h":"Element Long  Name","i":"*Element Physical Name","j":"Element Description",
                     "k":"Element Type","l":"Characteristic Long Name","m":"*Characteristic Physical Name","n":"Characteristic Order","o":"Characteristic Description","p":"Characteristic Type","q":"Characteristic Min Length",
                    "r":"Characteristic Max Length","s":"Characteristic Data Type", "t":"Characteristic UOM","u":"Characteristic Mandatory?","v":"Characteristic PK?","w":"Characteristic Default","x":"CDE ID",
                    "y":"CDE Version","z":"Element Mapping Group","aa":"Characteristic FK?","ab":"FK Element Name","ac":"FK Element Characteristic Name","ad":"Comments"}

    
    codemapfields = []
    for key, value in codemapdict.items():
        codemapfields.append(value)
    datalist = []

    configs = readConfigs(args.configfile)
    modeldict = parseMDF(configs['scriptinfo']['modelfile'],"model")
    propdict = parseMDF(configs['scriptinfo']['propsfile'], "props")

    index = 1
    for node, properties in modeldict.items():
        #Node is the graph node and properties is a dictionary associated with the property
       # nodeline = nodeSet(node, configs, codemapdict)
       # datalist.append(nodeline)
        line = lineSet(node, configs, codemapdict)
        #properties['Props'] can be null
        if properties['Props'] is not None:
            for propname in properties['Props']:
                if propname in propdict:
                    #Seq ID
                    line[codemapdict['d']] = index
                    index = index + 1
                    #Characteristic Long Name'
                    line[codemapdict['l']] = propname
                    #Characteristic Physical Name
                    line[codemapdict['m']] = propname
                    if "Desc" in propdict[propname]:
                        #Characteristic Description
                        line[codemapdict['o']] = cleanline(propdict[propname]['Desc'])
                    if "Type" in propdict[propname]:
                        #Type can have a "pattern" dict as a value, so only clean up if a string
                        if isinstance(propdict[propname]['Type'],str):
                            #Characteristic Type
                            line[codemapdict['s']] = cleanline(propdict[propname]['Type'])
                    if "Req" in propdict[propname]:
                        req = propdict[propname]['Req']
                        if isinstance(req, bool):
                            req = 'Yes'
                        if req == 'Preferred':
                            req = None
                            #Characteristic Mandatory
                        line[codemapdict['u']] = req
                    if "Term" in propdict[propname]:
                        #This gets a little funky as Term is a list of dictionary
                        for entry in propdict[propname]['Term']:
                            if entry['Origin'] == "caDSR":
                                cdeid = cleanline(entry['Code'])
                                if cdeid not in ['code ID']:
                                    #CDE ID
                                    line[codemapdict['x']] = cleanline(entry['Code'])
                                else:
                                    #Element Mapping Group (Optional if no CDE is associated)
                                    line[codemapdict['z']] = node
                            else:
                                line[codemapdict['z']] = node
                    else:
                        line[codemapdict['z']] = node
                    #At this point we've goten everything from the MDF file
                    datalist.append(line)
                    #Reset the line
                    line = lineSet(node, configs, codemapdict)
                else:
                    line = lineSet(node, configs, codemapdict)
    #Create the dataframe
    finaldf = pd.DataFrame(datalist, columns=codemapfields)
    finaldf.to_csv(configs['scriptinfo']['outputfile'], sep="\t", index=False)




        
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    #parser.add_argument("-p", "--props", help = "MDF props file" )
    #parser.add_argument("-m", "--model",  help="MDF model file")
    #parser.add_argument("-o", "--output",  help="Output csv file")
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)