# Create a Code Map input file from a set of MDF model files
import bento_mdf
from crdclib import crdclib as crdc
import pandas as pd
import argparse


def main(args):
    #Read the config file
    configs = crdc.readYAML(args.configfile)
    #Get the column names that are used by CodeMap in dictionary form
    codemapdict = configs['headers']
    #Create a list of the CodeMap headers.  Used later to create headers in the CodeMap file
    codemap_df = pd.DataFrame(columns=list(codemapdict.values()))
    
    #Read the MDF model file and MDF property file into dictionaries.  Locations for MDF are from the config file.
    mdffiles = configs['mdffiles']
    mdfmodel = bento_mdf.MDF(*mdffiles, handle=configs['modelinfo']['e'])
    props = mdfmodel.model.props

    #proplist should be a tuple of the node, property
    proplist = list(props.keys())
    
    # Index is used in the Seq ID columns and starts with 1
    index = 1
    # Work through each property for each node and transform it into CodeMap format
    # Element == Node, Characteristic == Property
    for node, prop in proplist:
        propdict = props[(node,prop)].get_attr_dict()
        loadline = {}
        #Constants
        for key, value in configs['modelinfo'].items():
            loadline[codemapdict[key]] = value
        #Element Long Name
        loadline[codemapdict['h']] = node
        #Element Physical Name
        loadline[codemapdict['i']] = node
        #Sequence ID
        loadline[codemapdict['d']] = index
        index = index + 1
        #Characteristic Long Name
        loadline[codemapdict['l']] = prop
        #Characteristic Physical Name
        loadline[codemapdict['m']] = prop
        #Characteristic Description
        loadline[codemapdict['o']] = crdc.cleanString(propdict['desc'], True)
        #Characteristic Type
        loadline[codemapdict['s']] = propdict['value_domain']
        #Characteristic Mandatory
        if propdict['is_required'] in ("Preferred", "False"):
            loadline[codemapdict['u']] = "No"
        elif propdict['is_required'] in ("True"):
            loadline[codemapdict['u']] = "No"
        else:
            loadline[codemapdict['u']] = propdict['is_required']
        if mdfmodel.model.props[node,prop].concept is not None: 
            #Propterm is a dictionary with a tuple of term name and source as key, and the term object as the value
            propterms = mdfmodel.model.props[(node,prop)].concept.terms
            for termobject in propterms.values():
                termdict = termobject.get_attr_dict()
                # CDE ID
                loadline[codemapdict['x']] = termdict['origin_id']
                # CDE Version
                loadline[codemapdict['y']] = termdict['origin_version']
                #cdename = termdict['value']
        codemap_df.loc[len(codemap_df)] = loadline
    codemap_df.to_csv(configs['outputfile'], sep="\t", index=False)
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)