import pandas as pd
import argparse
import re

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

def cleanline(text):
    cleantext = re.sub(r'\W+',' ',text)
    return cleantext

def buildLine(linedict, fieldlist):
    line = []
    for field in fieldlist:
        if field in linedict:
            line.append(linedict[field])
        else:
            line.append("\t")
    return line

def lineSet(repo, node):
    line = {}
    line = buildConstants(repo)
    line['Element Name'] = node
    line['Element Physical Name'] = node
    return line

def readExcel(filename, sheetname):
    exeldf = pd.read_excel(filename, sheet_name=sheetname)
    return exeldf

def addElement(line, node, description, type):
    line = {}
    line = buildConstants("CRDCSearch", "1.0")
    line['Element Name'] = node
    line['Element Physical Name'] = node
    line['Element Description'] = description
    line['Element Type'] = type
    return line


def main(args):

    codemapfileds = ["DO NOT USE","Batch User", "Batch Name","Seq ID","Model Name","Model Description","Model ID (Update Only)", "Model Version","Context",
                     "Primary Modeling Language","Model Type","Element Name","Element Physical Name","Element Description","Element Type","Characteristic Name","Characteristic Physical Name",
                     "Characteristics Order","Characteristic Description","Characteristic Type","Characteristic Min Length","Characteristic Max Length", "Characteristic Data Type","Characteristic Mandatory",
                     "Characteristics PK?","Characteristic Default","CDE ID","CDE Version","Element Mapping Group (Optional if no CDE is associated)","Comments","Characteristic FK?",
                     "FK Element Name","FK Element Name"]
    pagename = 'CRDC Common Model Report 012424'
    finallist = []
    exceldf = readExcel(args.model, pagename)
    
    index = 1
    for index, row in exceldf.iterrows():
        if row['Tag Name'] == "caDSR CDE Version":
            #Get the last row in finallist
            newlist = finallist[-1]
            #replace the 27th column which should be CDE Version
            newlist[27] = row['Tag Value']
            #Now replace the last row in finallist with the updated one
            finallist[-1] = newlist
         #The Excel output has three rows for each element with a CDE ID, one for ID, one for URL and one for CDE Version.  The if statement eliminates row triplication
        if row['Tag Name'] not in ("caDSR CDE URL", "caDSR CDE Version"):
            line = {}
            if row['Object Type'] == 'Class':
                classdef = cleanline(row['Class Definition'])
            if row['Object Type'] == 'Attribute':
                line = addElement(line, row['Class Name'], classdef, row['Object Type'])
                line['Seq ID'] = index
                index = index + 1
                line['Characteristic Name'] = row['Attribute Name']
                line['Characteristic Physical Name'] = row['Attribute Name']
                line['Characteristic Data Type'] = row['Data Type']
                if row['Tag Name'] == "caDSR CDE ID":
                    line['CDE ID'] = row['Tag Value']
                    line['CDE Version'] = "1.0"
                #print(len(row['Tag Name']))
                if len(str(row['Tag Name'])) == 1:
                    line['Element Mapping Group (Optional if no CDE is associated)'] = row['Class Name']
                finallist.append(buildLine(line, codemapfileds))
    finaldf = pd.DataFrame(finallist, columns=codemapfileds)   
    finaldf.to_csv(args.output, sep='\t') 

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--model", required=True, help="CRDC XLSX model file")
    parser.add_argument("-o", "--output", required=True, help="Output CodeMap csv file")

    args = parser.parse_args()

    main(args)