import argparse
import yaml
import re
import requests
import pandas as pd
import pprint
import sys
#This version will attempt to future proof changes to the column names by using a dictionary where the key is the Excel column ID

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

def hasNubmers(text):
    return re.match("^[0-9]*$", text)


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

def getCDEInfo(publicID):
    apiurl = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api"
    endpoint = "/DataElement/"
    headers = {"accept":"application/json"}
    cderes = requests.get(apiurl+endpoint+publicID, headers=headers)
    cdeinfo = cderes.json()
    return cdeinfo

def versionSlap(version):
    #Do different things if version is a string or an int
    if type(version) == str:
        if '.' in version:
            versionlist = version.split(".")
            version = versionlist[0]
            version = float(version)
        elif ' ' in version:
            versionlist = version.split(" ")
            version = versionlist[0]
            version = float(version)
    else:
        version = float(version)
    version = str(round(version,2))
    return version


def main(args):
    #Read the configuration files
    configs = readConfigs(args.configfile)
    #Get the column names that are used by CodeMap in dictionary form
    codemapdict = configs['headers']
    
    #Create a list of the CodeMap headers.  Used later to create headers in the CodeMap file
    codemapfields = []
    for key, value in codemapdict.items():
        codemapfields.append(value)

    #Set up the list that will be used to store all of the reformatted data. 
    datalist = []

    #Read the MDF model file and MDF property file into dictionaries.  Locations for MDF are from the config file.
    modeldict = parseMDF(configs['scriptinfo']['modelfile'],"model")
    propdict = parseMDF(configs['scriptinfo']['propsfile'], "props")

    # Index is used in the Seq ID columns and starts with 1
    index = 1
    #Using the model dictionary, work through each property for each node and transform it into CodeMap format
    for node, properties in modeldict.items():
        #Create a line and poulate the constants
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
                    #Handling the mess around "Required" in MDF files. 
                    if "Req" in propdict[propname]:
                        req = propdict[propname]['Req']
                        if isinstance(req, bool):
                            req = 'Yes'
                        if req == 'Preferred':
                            req = None
                            #Characteristic Mandatory
                        line[codemapdict['u']] = req
                    # The Term section of an MDF property file is where thngs like the CDE ID and version live.
                    if "Term" in propdict[propname]:
                        #This gets a little funky as Term is a list of dictionary
                        for entry in propdict[propname]['Term']:
                            #This works if the CDE ID is in the props file.
                            if entry['Origin'] == "caDSR":
                                cdeid = cleanline(entry['Code'])
                                #Because people insiste on doing stupid stuff like putting "Pending" as a CDE ID, we have to check and see what the stinking thing is
                                cdeid = str(cdeid)
                                if hasNubmers(cdeid):
                                    version = cleanline(entry['Version'])
                                    #CodeMap is REALLY picky about represnting the version number, versionSlap formats it correctly regardless of how it starts
                                    version = versionSlap(version)
                                    line[codemapdict['x']] = cdeid
                                    line[codemapdict['y']] = version
                                else:
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
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)