import argparse
import yaml
import re
import pandas as pd


def buildConstants(handle):
    constants = {}
    constants['Batch User'] = "pihl"
    constants['Batch Name'] = "CRDC Batch 1"
    constants['Model Name'] = handle
    constants['Model Description'] = f"{handle} Data model"
    constants['Model Version'] = "1.3"
    constants["Context"] = "CRDC"
    constants['Primary Modeling Language'] = "YAML"
    constants['Model Type'] = "Physical Model"
    constants['Element Type'] = "Node"

    return constants


def parseMDF(mdffile,type):
    with open(mdffile) as f:
        modeldict = yaml.load(f, Loader=yaml.FullLoader)

    if type == "props":
        modeldict = modeldict['PropDefinitions']
        return modeldict
    elif type == "model":
        if "Handle" in modeldict:
            repo = modeldict['Handle']
        else:
            repo = "CTDC"
        modeldict = modeldict['Nodes']
        return modeldict, repo
   
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

def lineSet(repo, node):
    line = {}
    line = buildConstants(repo)
    line['Element Name'] = node
    line['Element Physical Name'] = node
    return line

def main(args):
    
    codemapfileds = ["DO NOT USE","Batch User", "Batch Name","Seq ID","Model Name","Model Description","Model ID (Update Only)", "Model Version","Context",
                     "Primary Modeling Language","Model Type","Element Name","Element Physical Name","Element Description","Element Type","Characteristic Name","Characteristic Physical Name",
                     "Characteristics Order","Characteristic Description","Characteristic Type","Characteristic Min Length","Characteristic Max Length", "Characteristic Data Type","Characteristic Mandatory",
                     "Characteristics PK?","Characteristic Default","CDE ID","CDE Version","Element Mapping Group (Optional if no CDE is associated)","Comments","Characteristic FK?",
                     "FK Element Name","FK Element Name"]
    
    modeldict, repo = parseMDF(args.model,"model")
    propdict = parseMDF(args.props, "props")
    datalist = []

    index = 1
    for node, properties in modeldict.items():
        line = lineSet(repo, node)
        #properties['Props'] can be null
        if properties['Props'] is not None:
            for propname in properties['Props']:
                if propname in propdict:
                    line['Seq ID'] = index
                    index = index + 1
                    line['Characteristic Name'] = propname
                    line['Characteristic Physical Name'] = propname
                    if "Desc" in propdict[propname]:
                        line['Characteristic Description'] = cleanline(propdict[propname]['Desc'])
                    if "Type" in propdict[propname]:
                        #Type can have a "pattern" dict as a value, so only clean up if a string
                        if isinstance(propdict[propname]['Type'],str):
                            line['Characteristic Type'] = cleanline(propdict[propname]['Type'])
                    if "Req" in propdict[propname]:
                        req = propdict[propname]['Req']
                        if isinstance(req, bool):
                            req = 'Yes'
                        if req == 'Preferred':
                            req = None
                        line['Characteristic Mandatory'] = req
                    if "Term" in propdict[propname]:
                        #This gets a little funky as Term is a list of dictionary
                        for entry in propdict[propname]['Term']:
                            if entry['Origin'] == "caDSR":
                                cdeid = cleanline(entry['Code'])
                                if cdeid not in ['code ID']:
                                    line['CDE ID'] = cleanline(entry['Code'])
                                else:
                                    line['Element Mapping Group (Optional if no CDE is associated)'] = node
                            else:
                                line['Element Mapping Group (Optional if no CDE is associated)'] = node
                    else:
                        line['Element Mapping Group (Optional if no CDE is associated)'] = node
                    #At this point we've goten everything from the MDF file
                    datalist.append(line)
                    #Reset the line
                    line = lineSet(repo, node)
                else:
                    line = lineSet(repo, node)
    #Create the dataframe
    finaldf = pd.DataFrame(datalist, columns=codemapfileds)
    #print(finaldf)
    finaldf.to_csv(args.output, sep="\t")




        
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--props", required=True, help = "MDF props file" )
    parser.add_argument("-m", "--model", required=True, help="MDF model file")
    parser.add_argument("-o", "--output", required=True, help="Output csv file")

    args = parser.parse_args()

    main(args)