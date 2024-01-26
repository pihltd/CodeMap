#import csv
import argparse
import json
import re
import pandas as pd

def buildConstants(handle,version):
    constants = {}
    constants['Batch User'] = "pihl"
    constants['Batch Name'] = "CRDC Batch 1"
    constants['Model Name'] = handle
    constants['Model Description'] = f"{handle} Data model"
    constants['Model Version'] = version
    constants["Context"] = "CRDC"
    constants['Primary Modeling Language'] = "YAML"
    constants['Model Type'] = "Physical Model"
    #constants['Element Type'] = "Node"

    return constants

def parseCRDCSearch(modelfile):
    with open(modelfile, "rb") as f:
        data = json.load(f)
    #Cut down so it's just the list of objects
    data = data['definitions']
    return data

def buildLine(linedict, fieldlist):
    line = []
    for field in fieldlist:
        if field in linedict:
            line.append(linedict[field])
        else:
            line.append("\t")
    return line

def cleanline(text):
    cleantext = re.sub(r'\W+',' ',text)
    return cleantext

def addElement(line, node, description):
    line = {}
    line = buildConstants("CRDCSearch", "1.0")
    line['Element Name'] = node
    line['Element Physical Name'] = node
    if 'description' in description:
        line['Element Description'] = cleanline(description['description'])
    line['Element Type'] = description['type']
    return line

def main(args):

    codemapfileds = ["DO NOT USE","Batch User", "Batch Name","Seq ID","Model Name","Model Description","Model ID (Update Only)", "Model Version","Context",
                     "Primary Modeling Language","Model Type","Element Name","Element Physical Name","Element Description","Element Type","Characteristic Name","Characteristic Physical Name",
                     "Characteristics Order","Characteristic Description","Characteristic Type","Characteristic Min Length","Characteristic Max Length", "Characteristic Data Type","Characteristic Mandatory",
                     "Characteristics PK?","Characteristic Default","CDE ID","CDE Version","Element Mapping Group (Optional if no CDE is associated)","Comments","Characteristic FK?",
                     "FK Element Name","FK Element Name"]
    
    crdcmodel = parseCRDCSearch(args.model)
    datalist = []
    
    index = 1
    for node, description in crdcmodel.items():
        line = buildConstants("CRDCSearch", "1.0")
        line = addElement(line, node, description)
        
        for property, values in description['properties'].items():
            line['Seq ID'] = index
            index = index + 1
            line['Characteristic Name'] = property
            line['Characteristic Physical Name'] = property
            line['Characteristic type'] = 'Property'
            if 'minItems' in values:
                line['Characteristic Min Length'] = values['minItems']
            if 'maxItems' in values:
                line['Characteristic Max Length'] = values['maxItems']
            if 'items' in values:
                line['Characteristic Max Length'] = values['items']
            #Type and #ref used interchangably
            if 'type' in values:
                line['Characteristic Data Type'] = values['type']
            if '$ref' in values:
                line['Characteristic Data Type'] = values['$ref']
            #have everything, write to file:
            datalist.append(line)
            #Do a reset of the line
            line = {}
            line = buildConstants('CRDCSearch', "1.0")
            line = addElement(line, node, description)
    #Put it all in a dataframe
    finaldf = pd.DataFrame(datalist, columns=codemapfileds)
    print(finaldf)
    finaldf.to_csv(args.output, sep="\t")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--model", required=True, help="CRDC JSON model file")
    parser.add_argument("-o", "--output", required=True, help="Output CodeMap csv file")

    args = parser.parse_args()

    main(args)