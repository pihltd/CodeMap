#This generates the relationship files needed for the Synthetic Data Generator
# It can use the same config file as the data generator
import argparse
import bento_mdf
from crdclib import crdclib
import pandas as pd

def main(args):
    #Get the configs
    configs = crdclib.readYAML(args.configfile)
    
    #Read the model
    mdffiles = configs['mdffiles']
    mdfmodel = bento_mdf.MDF(*mdffiles, handle=configs['handle'])
    
    #Set up dataframes
    columns = ["handle", "source", "target", "relationship"]
    edge_df = pd.DataFrame(columns=columns)
    #finalcolumns = ["name", "required", "needed", "comments"]
    #final_df = pd.DataFrame(columns=finalcolumns)
    finaldict = {}
    
    #Get the edge relationships which are  {(relationship name, source, target), edge object}
    edges = mdfmodel.model.edges
    #Get the nodes which are { nodename: node object}
    nodes = mdfmodel.model.nodes
    
    # The easiest way to deal with relationships is going to be a datafram
    for info, edgeobj in edges.items():
        edge_df.loc[len(edge_df)] = {"handle": info[0], "source": info[1], "target": info[2], "relationship": edgeobj.get_attr_dict()['multiplicity']}
    
    #Need a root node and it should be program
    finaldict['HeadNode'] = [{"name": "program", "count": 1, "Prefix": configs['handle']}]
    nodelist = list(nodes.keys())
    temp = {}
    proptemp = {}
    
    # Populate the IncludeNodes and IncludeProperties sections
    for node in nodelist:
        if node != 'program':
            temp[node] = {"NodeCount":"", "Prefix":""}
            nodeprops = nodes[node].props
            proplist = list(nodeprops.keys())
            proptemp[node] = proplist
    finaldict['IncludeNodes'] = temp
    finaldict['IncludeProperties'] = proplist
    
    # Add the relationships.  Note, this differs significantly from Bruce's representation
    
    reltemp = {}
    sourcelist = edge_df['source'].unique()
    for source in sourcelist:
        rowtemp = {}
        source_df = edge_df.loc[edge_df['source'] == source]
        for row in source_df.iterrows():
            print(row)
            rowtemp['target'] = row['target']
            rowtemp['relationship'] = row['relationship']
        reltemp[source] = rowtemp
    finaldict['RelationshipSpecs'] = reltemp
    
    filename = configs['outputpath']+configs['datainputfilename']
    crdclib.writeYAML(filename, finaldict)
    
    
    

    
    #for info, edge in edges.items():
    #    edge_df.loc[len(edge_df)] = {"handle": info[0], "source": info[1], "target": info[2], "relationship": edge.get_attr_dict()['multiplicity']}
    #print(edge_df)
    #targetlist = edge_df['target'].unique()
    #print(targetlist)
    
    # Program, Project, and Study are required for CRDC, but may not be present. 
    #for target in targetlist:
    #    # There can only be one Program
    #    if target == 'program':
    #        final_df[len(final_df)] = {"name": "program", "required": 1, "needed": 1, "comments": "Can only be one program"}
    #    elif target == "project":
    #        final_df[len(final_df)] = {"name": "program", "required": 1, "comments": "There can be one or more programs per project"}
    #    elif target == "study":
    #        final_df[len(final_df)] = {"name": "program", "required": 1, "comments": "There can be one or more studies per project or program"}
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")
    parser.add_argument("-v", "--verbose", help="Verbose Output")

    args = parser.parse_args()

    main(args)