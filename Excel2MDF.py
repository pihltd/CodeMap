#Convert the CRDC Search Excel file to an MDF format file.
import CRDCStuff as crdc
import pandas as pd
import yaml
import argparse
import pprint

def readExcel(filename, sheetname):
    exeldf = pd.read_excel(filename, sheet_name=sheetname)
    return exeldf

def getCDEName(cdeid, version):
    results = crdc.getCDERecord(cdeid, version)
    if 'preferredName' in results['DataElement']:
        cdename = results['DataElement']['preferredName']
    else:
        cdename = results['DataElement']['longName']
    if 'preferredDefinition' in results['DataElement']:
        definition = results['DataElement']['preferredDefinition']
    else:
        definition = results['DataElement']['definition']
    definition = crdc.cleanString(definition, True)
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
        attributename = crdc.cleanString(attributename, False)
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
    attr_df = xldf.loc[xldf['Object_Type'] == 'Attribute']
    propjson = {}
    propjson['PropDefinitions'] = {}
    
    attr_list = attr_df['Attribute_Name'].unique()
    for attr in attr_list:
        temp_df = attr_df.loc[attr_df['Attribute_Name'] == attr]
        nodejson = {}
        nodejson = parseRow(temp_df.iloc[0])
        propertyname = temp_df['Attribute_Name'].iloc[0]
        propertyname = crdc.cleanString(propertyname, False)
        classname = temp_df['Class_Name'].iloc[0]
        classname = crdc.cleanString(classname, False)
        cdeid, cdeversion, cdeurl = getCDEInfoFromExcel(classname, propertyname,temp_df)
        if cdeversion is not None:
            if 'http' in cdeversion:
                cdeversion = None
        if nodejson['Type'] == 'enum':
            if cdeversion is None:
                apiurl = f"https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/{cdeid}"
            else:
                apiurl = f"https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/{cdeid}?version={cdeversion}"
            nodejson['Enum'] = [cdeurl, apiurl]
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


#def writeYAML(filename, jsonthing):
#    with open(filename, 'w') as f:
#        yaml.dump(jsonthing, f)
#    f.close()

def main(args):
    #Read the configs
    if args.verbose:
        print("Starting to read config file")
    #configs = readConfigs(args.config)
    configs = crdc.readYAML(args.config)

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
    crdc.writeYAML(configs['mdffile'], modeljson)

    #Create the MDF Property file and write
    propjson = makePropFile(xldf, configs['modelHandle'], configs['modelVersion'])

    #writeYAML(args.propfile, propjson)
    crdc.writeYAML(configs['mdfprops'], propjson)




if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="Configuration file to convert CRDS Search XLS file to MDF")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    main(args)