import argparse
import yaml
import re
import pandas as pd

def readConfigs(yamlfile):
    with open(yamlfile) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
    return configs

def buildConfigConstants(configjson):
    constants = {}
    for key, value in configjson['modelinfo'].items():
        constants[key] = value
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

def lineSet(node, configs):
    line = {}
    line = buildConfigConstants(configs)
    line['Element Long Name'] = node
    line['Element Physical Name'] = node
    return line

def main(args):

    codemapfields = ["DO NOT USE","Batch User","Batch Name","Seq ID","*Model Name",	"*Model ID","*Model Version","Element Long  Name","*Element Physical Name","Element Description",
                     "Element Type","Characteristic Long Name","*Characteristic Physical Name","Characteristic Order","Characteristic Description","Characteristic Type","Characteristic Min Length",
                    "Characteristic Max Length","Characteristic Data Type", "Characteristic UOM","Characteristic Mandatory?","Characteristic PK?","Characteristic Default","CDE ID",
                    "CDE Version","Element Mapping Group","Characteristic FK?","FK Element Name","FK Element Characteristic Name","Comments"]
    
    configs = readConfigs(args.configfile)
    modeldict, repo = parseMDF(configs['scriptinfo']['modelfile'],"model")
    propdict = parseMDF(configs['scriptinfo']['propsfile'], "props")
    datalist = []

    index = 1
    for node, properties in modeldict.items():
        line = lineSet(node, configs)
        #properties['Props'] can be null
        if properties['Props'] is not None:
            for propname in properties['Props']:
                if propname in propdict:
                    line['Seq ID'] = index
                    index = index + 1
                    line['Characteristic Long Name'] = propname
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
                        line['Characteristic Mandatory?'] = req
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
                    line = lineSet(node, configs)
                else:
                    line = lineSet(node, configs)
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