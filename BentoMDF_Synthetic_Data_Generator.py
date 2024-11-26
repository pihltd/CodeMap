# Based on Bruce Wang's code
import bento_mdf
from crdclib import crdclib
import argparse
import pandas as pd
import random

def getCDEPVs(cde_json):
    #Returns a list of permissible values.  Returns None if no PVs are found
    if cde_json['DataElement']['ValueDomain']['type'] == 'Enumerated':
        pvlist = []
        for pv in cde_json['DataElement']['ValueDomain']['PermissibleValues']:
            pvlist.append(pv['value'])
        return pvlist
    else:
        return None
    
def getTermDictionary(property_terms):
    for term_object in property_terms.values():
        return term_object.get_attr_dict()


def main(args):
    #Get the configurations
    configs = crdclib.readYAML(args.configfile)
    #Read the MDF files.  Locations for MDF are from the config file.
    mdffiles = configs['mdffiles']
    mdfmodel = bento_mdf.MDF(*mdffiles, handle=configs['handle'])
    nodes = mdfmodel.model.nodes
    props = mdfmodel.model.props
    edges = mdfmodel.model.edges
    
    #node_list is just a list of the node names
    node_list = list(nodes.keys())
    # props_list is a list of (nodename, propname)
    props_list = list(props.keys())
    
    #Create a node_headers dictionary.  Key is nodename, value is list of properties for the node
    node_headers = {}
    for node in node_list:
        node_props = nodes[node].props
        node_headers[node] =list(node_props.keys())
        
    # Now for the fun part, create the df_dictionary where the key is the node, and the value is a dataframe
    df_dictionary = {}
    for node, headers in node_headers.items():
        df_dictionary[node] = pd.DataFrame(columns=headers)
    
    # Create a datafrom from an Excel that has column names and a list of provided values in the columns
    provided_df = pd.read_excel(configs['providedvaluesfile'],sheet_name=configs['sheetname'])
        
    # for each load sheet, go field by field and 1) load with permissible values if they exist 2) Load with fake values if no PV.
    for node, proplist in node_headers.items():
        #need a dictionary to hold property:pvs
        temp_values = {}
        for prop in proplist:
            # If there is a CDE, it is in a Term annotation on the property
            if args.verbose:
                print(f"Node:\t{node}\tProperty:\t{prop}")
            if mdfmodel.model.props[(node,prop)].concept is not None:
                prop_terms= mdfmodel.model.props[(node,prop)].concept.terms
                term_dictionary = getTermDictionary(prop_terms)
                if 'origin_id' in term_dictionary:
                    cdejson = crdclib.getCDERecord(term_dictionary['origin_id'], term_dictionary['origin_version'])
                    pvlist = getCDEPVs(cdejson)
                    if pvlist is not None:
                        temp_values[prop] = pvlist
                    else:
                        temp_values[prop] = None
            else:
                temp_values[prop] = None
        #Set a number of rows to populate
        # TODO: This should be a user defined number
        rowcount = 10
        counter = 1
        while counter <= rowcount:
            line = {}
            for prop in proplist:
                #First use any provided values
                if prop in provided_df:
                    templist = provided_df[prop].to_list()
                    line[prop] = random.choice(templist)
                #If values weren't provided, check if there are permissible values
                elif temp_values[prop] is not None:
                    if len(temp_values[prop]) > 0:
                        line[prop] = random.choice(temp_values[prop])
                    else:
                        line[prop] = counter
                #And if neither of the first two worked, just add the counter number
                # TODO: Probably need some sort of random character generator
                else:
                    line[prop] = str(counter)
            df_dictionary[node].loc[len (df_dictionary[node])] = line
            counter = counter + 1
    for node, df in df_dictionary.items():
        filename = configs["outputpath"]+configs["outputname"]+"_"+node+".csv"
        df.to_csv(filename, sep="\t", index=False)
                    
                
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-v", "--verbose", help="Verbose Output")

    args = parser.parse_args()

    main(args)