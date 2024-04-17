#Convert the CRDC Search Excel file to an MDF format file.
import pandas as pd
import yaml
import argparse
import requests
import json
import pprint

def readConfigs(yamlfile):
    with open(yamlfile) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
    return configs

def readExcel(filename, sheetname):
    exeldf = pd.read_excel(filename, sheet_name=sheetname)
    return exeldf

def getCDEName(cdeid, version):
    url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"+str(cdeid)+"?version="+str(version)
    headers = {'accept':'application/json'}
    try:
        results = requests.get(url, headers = headers)
    except requests.exceptions.HTTPError as e:
        pprint.pprint(e)
    if results.status_code == 200:
        results = json.loads(results.content.decode())
        cdename = results['DataElement']['preferredName']
        definition = results['DataElement']['preferredDefinition']
    else:
        cdename = 'caDSR Name Error'
    return cdename, definition



def makeModelFile(xldf):
    modeljson = {}
    nodejson = {}
    for index, row in xldf.iterrows():
        if row['Object Type'] == 'Class':
            nodename = row['Class Name']
            nodejson[nodename] = {}
            nodejson[nodename]['Props'] = []
        elif row['Object Type'] == 'Attribute':
            nodejson[row['Class Name']]['Props'].append(row['Attribute Name'])
    #clean up duplications in the Props list
            for node, props in nodejson.items():
                deduplist = list(set(props['Props']))
                nodejson[node]['Props'] = deduplist
                nodejson[node]['Desc'] = 'text'
                nodejson[node]['Tags'] = {'Category':'value', 'Assignment':'value', 'Class':'value', 'Template':'Yes'}
    modeljson['Nodes'] = nodejson
    modeljson['Handle'] = 'CRDC Search'
    modeljson['Version'] = "1"
    return modeljson

def parseRow(row):
    #Common parsing
    rowjson = {}
    rowjson['Type'] = row['Data Type']
    rowjson['Desc'] = 'text'
    rowjson['Req'] = 'No'
    return rowjson

def makePropFile(xldf):
    propjson = {}
    propjson['PropDefinitions'] = {}
    
    for index, row in xldf.iterrows():
        if row['Object Type'] == 'Attribute':
            nodejson = {}
            nodejson = parseRow(row)
            propertyname = row['Attribute Name']
            cdedefinition = "Text"
            #Need to handle with and without CDE separately since with CDE has 3 lines
            if row['Tag Name'] in ('caDSR CDE ID', ' '):
                if row['Tag Name'] == 'caDSR CDE ID':
                    nodejson['Term'] = []
                    temp = {}
                    temp['Origin'] = 'caDSR'
                    temp['Code'] = int(row['Tag Value'])
                    cdename, cdedefinition = getCDEName(row['Tag Value'],1)
                    temp['Value'] = cdename
                    #This is where things get funky.  The version should be in the Tag Value column that is two rows later
                    newrowindex = index + 2
                    versionrow = xldf.iloc[newrowindex]
                    temp['Version'] = versionrow['Tag Value']
                    nodejson['Term'].append(temp)
                propjson['PropDefinitions'][propertyname] = nodejson
                propjson['PropDefinitions'][propertyname]['Desc'] = cdedefinition

    propjson['Handle'] = 'CRDC Search'
    propjson['Version'] = '1.10'
    return propjson

def updateVersion(jsonthing, xldf):
    #In the Excel file, the version is a string that can look like a float.  This turns it to int.
    for index, row in xldf.iterrows():
        if row['Tag Name'] == 'caDSR CDE Version':
            version = row['Tag Value']
            if isinstance(version, str):
                version = float(version)
                version = int(version)
            jsonthing['PropDefinitions'][row['Attribute Name']]['Term'][0]['Version'] = version
    return jsonthing
        


def writeYAML(filename, jsonthing):
    with open(filename, 'w') as f:
        yaml.dump(jsonthing, f)
    f.close()

def main(args):
    #Read the configs
    configs = readConfigs(args.config)
    #Read the Excel file into a dataframe
    #xldf = readExcel(args.excelfile, args.sheetname)
    xldf = readExcel(configs['excelfile'], configs['worksheet'])
    #Create the MDF Model file and write 
    modeljson = makeModelFile(xldf)
    #writeYAML(args.mdffile, modeljson)
    writeYAML(configs['mdffile'], modeljson)
    #Create the MDF Property file and write
    propjson = makePropFile(xldf)
    propjson = updateVersion(propjson, xldf)
    #writeYAML(args.propfile, propjson)
    writeYAML(configs['mdfprops'], propjson)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="Configuration file to convert CRDS Search XLS file to MDF")

    args = parser.parse_args()

    main(args)