#Convert the CRDC Search Excel file to an MDF format file.
import pandas as pd
import yaml
import argparse
import requests
import json
import pprint
import re

def readConfigs(yamlfile):
    with open(yamlfile) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
    return configs

def readExcel(filename, sheetname):
    exeldf = pd.read_excel(filename, sheet_name=sheetname)
    return exeldf

def cleanString(inputstring, description):
    if description:
        outputstring = re.sub(r'[\n\r\t?]+', '', inputstring)
        outputstring.rstrip()
    else:
        outputstring = re.sub(r'[\W]+', '', inputstring)
    
    return outputstring

def getCDEName(cdeid, version):
    if version is None:
        url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"+str(cdeid)
    elif 'http' in version:
        url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"+str(cdeid)
    else:
        url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"+str(cdeid)+"?version="+str(version)
    headers = {'accept':'application/json'}
    print(url)
    try:
        results = requests.get(url, headers = headers)
    except requests.exceptions.HTTPError as e:
        pprint.pprint(e)
    if results.status_code == 200:
        results = json.loads(results.content.decode())
        if 'preferredName' in results['DataElement']:
            cdename = results['DataElement']['preferredName']
        else:
            cdename = results['DataElement']['longName']
        if 'preferredDefinition' in results['DataElement']:
            definition = results['DataElement']['preferredDefinition']
        else:
            definition = results['DataElement']['definition']
    else:
        cdename = 'caDSR Name Error'
    definition = cleanString(definition, True)
    return cdename, definition

def getCDEInfoFromExcel(attclass, attribute, df):
    newdf = df.loc[(df['Class_Name'] == attclass) & (df['Attribute_Name'] == attribute)]
    cdeid = None
    version = None
    url = None
    for index, row in newdf.iterrows():
        if row['Tag_Name'] == 'caDSR CDE ID':
            cdeid = str(row['Tag_Value'])
        elif row['Tag_Name'] == 'caDSR CDE Version':
            version = str(row['Tag_Value'])
        elif row['Tag_Name'] == 'caDSR CDE URL':
            url = row['Tag_Value']
    return cdeid, version, url



def makeModelFile(xldf, handle, version):
    modeljson = {}
    nodejson = {}

    #Step 1 get the Classes 
    class_df = xldf.query('Object_Type == "Class"')
    for index, row in class_df.iterrows():
        nodename = row['Class_Name']
        nodejson[nodename] = {}
        nodejson[nodename]['Props'] = []
    
    #Step 2, get the Attributes
    attribute_df = xldf.query('Object_Type == "Attribute"')
    for index, row in attribute_df.iterrows():
        attributename = row['Attribute_Name']
        attributename = cleanString(attributename, False)
        nodejson[row['Class_Name']]['Props'].append(attributename)
        #Step 3, Clue up duplicaiotns in the Props list
        for node,props in nodejson.items():
            dedupelist = list(set(props['Props']))
            nodejson[node]['Props'] = dedupelist
            nodejson[node]['Text'] = 'text'
            nodejson[node]['Tags'] = {'Category':'value', 'Assignment':'value', 'Class':'value', 'Template':'Yes'}

    #Step 4, build the class/node relationships
    rel_df = xldf.query('Object_Type =="Association"')
    relterms = {"0..*":"many", "1..*":"one", "1":"one", "0..1":"one", 1:"one"}
    reljson = {}
    for index, row in rel_df.iterrows():
        if row['Source_Card'] not in ('', ' '):
            source = row['Class_Name']
            target = row['Target_Class']
            sourceCard = row['Source_Card']
            targetCard = row['Destination_Card']
            relstring = relterms[targetCard]+"_to_"+relterms[sourceCard]
            #There are sometimes relationship names in the Association name column.  Use if there
            if row['Association_Name'] not in ('',' '):
                labelstring = row['Association_Name']
                labelstring = labelstring.replace(" ","_")
            else:
                labelstring = "of_"+source
            if labelstring in reljson:
                reljson[labelstring]['Ends'].append({'Src':target, 'Dst':source})
            else:
                temp = {}
                temp['Mul'] = relstring
                temp['Ends'] = [{'Src':target, 'Dst':source}]
                temp['Props'] = None
                reljson[labelstring] = temp

    #Step 4, build the final json
    modeljson['Nodes'] = nodejson
    modeljson['Handle'] = handle
    modeljson['Version'] = version
    modeljson['Relationships'] = reljson
    return modeljson

def parseRow(row):
    #Common parsing
    rowjson = {}
    rowjson['Type'] = row['Data_Type']
    rowjson['Desc'] = 'text'
    rowjson['Req'] = 'No'
    return rowjson

def makePropFile(xldf, handle, version):
    propjson = {}
    propjson['PropDefinitions'] = {}
    
    for index, row in xldf.iterrows():
        if row['Object_Type'] == 'Attribute':
            nodejson = {}
            #If the data type is enum, it's in the "Type" field from parseRow
            nodejson = parseRow(row)
            propertyname = row['Attribute_Name']
            propertyname = cleanString(propertyname, False)
            classname = row['Class_Name']
            classname = cleanString(classname, False)
            cdeid, cdeversion, cdeurl = getCDEInfoFromExcel(classname, propertyname,xldf)
            if nodejson['Type'] == 'enum':
                nodejson['Enum'] = [cdeurl]
                #Look into pulling a type from the CDE API
                nodejson['Type'] = None
            cdedefinition = "Text"
            if cdeid is not None:
                nodejson['Term'] = []
                temp = {}
                temp['Origin'] = 'caDSR'
                temp['Code'] = cdeid
                temp['Version'] = cdeversion
                cdename, cdedefinition = getCDEName(cdeid, cdeversion)
                temp['Value'] = cdename
                nodejson['Term'].append(temp)
            propjson['PropDefinitions'][propertyname] = nodejson
            propjson['PropDefinitions'][propertyname]['Desc'] = cdedefinition

    propjson['Handle'] = handle
    propjson['Version'] = version
    return propjson
       


def writeYAML(filename, jsonthing):
    with open(filename, 'w') as f:
        yaml.dump(jsonthing, f)
    f.close()

def main(args):
    #Read the configs
    if args.verbose:
        print("Starting to read config file")
    configs = readConfigs(args.config)

    #Read the Excel file into a dataframe
    if args.verbose:
        print(f"Starting to read Excel file{configs['excelfile']}")
    xldf = readExcel(configs['excelfile'], configs['worksheet'])

    #Create the MDF Model file and write 
    if args.verbose:
        print("Starting creation of modeljson")
    modeljson = makeModelFile(xldf, configs['modelHandle'], configs['modelVersion'])
    if args.verbose:
        pprint.pprint(modeljson)

    #writeYAML(args.mdffile, modeljson)
    writeYAML(configs['mdffile'], modeljson)

    #Create the MDF Property file and write
    propjson = makePropFile(xldf, configs['modelHandle'], configs['modelVersion'])

    #writeYAML(args.propfile, propjson)
    writeYAML(configs['mdfprops'], propjson)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="Configuration file to convert CRDS Search XLS file to MDF")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    main(args)