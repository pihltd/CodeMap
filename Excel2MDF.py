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
    return cdename, definition

#def relType(attribute, target):


def makeModelFile(xldf):
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
        nodejson[row['Class_Name']]['Props'].append(row['Attribute_Name'])
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
            #relstring = relterms[sourceCard]+"_to_"+relterms[targetCard]
            relstring = relterms[targetCard]+"_to_"+relterms[sourceCard]
            #There are sometimes relationship names in the Association name column.  Use if there
            if row['Association_Name'] not in ('',' '):
                labelstring = row['Association_Name']
            else:
                labelstring = "of_"+source
            if labelstring in reljson:
                #reljson[labelstring]['Ends'].append({'Src':source, 'Dst':target})
                reljson[labelstring]['Ends'].append({'Src':target, 'Dst':source})
            else:
                temp = {}
                temp['Mul'] = relstring
                #temp['Ends'] = [{'Src':source, 'Dst':target}]
                temp['Ends'] = [{'Src':target, 'Dst':source}]
                reljson[labelstring] = temp

    #Step 4, build the final json
    modeljson['Nodes'] = nodejson
    modeljson['Handle'] = 'CRDC Search'
    modeljson['Version'] = "0.1"
    modeljson['Relationships'] = reljson
    return modeljson

def parseRow(row):
    #Common parsing
    rowjson = {}
    rowjson['Type'] = row['Data_Type']
    rowjson['Desc'] = 'text'
    rowjson['Req'] = 'No'
    return rowjson

def makePropFile(xldf):
    propjson = {}
    propjson['PropDefinitions'] = {}
    
    for index, row in xldf.iterrows():
        if row['Object_Type'] == 'Attribute':
            nodejson = {}
            nodejson = parseRow(row)
            propertyname = row['Attribute_Name']
            cdedefinition = "Text"
            #Need to handle with and without CDE separately since with CDE has 3 lines
            if row['Tag_Name'] in ('caDSR CDE ID', ' '):
                if row['Tag_Name'] == 'caDSR CDE ID':
                    nodejson['Term'] = []
                    temp = {}
                    temp['Origin'] = 'caDSR'
                    temp['Code'] = int(row['Tag_Value'])
                    cdename, cdedefinition = getCDEName(row['Tag_Value'],1)
                    temp['Value'] = cdename
                    #This is where things get funky.  The version should be in the Tag Value column that is two rows later
                    newrowindex = index + 2
                    versionrow = xldf.iloc[newrowindex]
                    temp['Version'] = versionrow['Tag_Value']
                    nodejson['Term'].append(temp)
                propjson['PropDefinitions'][propertyname] = nodejson
                propjson['PropDefinitions'][propertyname]['Desc'] = cdedefinition

    propjson['Handle'] = 'CRDC Search'
    propjson['Version'] = '1.10'
    return propjson

def updateVersion(jsonthing, xldf):
    #In the Excel file, the version is a string that can look like a float.  This turns it to int.
    for index, row in xldf.iterrows():
        if row['Tag_Name'] == 'caDSR CDE Version':
            version = row['Tag_Value']
            if isinstance(version, str):
                version = float(version)
                version = int(version)
            jsonthing['PropDefinitions'][row['Attribute_Name']]['Term'][0]['Version'] = version
    return jsonthing
        


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
    modeljson = makeModelFile(xldf)      
    if args.verbose:
        pprint.pprint(modeljson)

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
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    main(args)