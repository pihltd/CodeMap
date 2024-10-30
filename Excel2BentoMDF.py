'''
Model.add_node(node: Node)
Model.add_edge(edge: Edge)
Model.add_prop(ent: Entity, prop: Property)
Model.annotate(ent: Entity, term: Term)
Model.add_terms(prop: Property, *terms: *list[Term])
Model.rm_node(node: Node)
Model.rm_edge(edge: Edge)
Model.rm_prop(prop: Property)
Model.rm_term(term: Term)
'''

#Convert the CRDC Search Excel file to an MDF format file using the bento-mdf library
import pandas as pd
import yaml
import argparse
import requests
import json
import pprint
import re
from bento_meta.model import Model, Node, Property, Term, Edge
from bento_mdf.mdf import MDF

def readConfigs(yamlfile):
    with open(yamlfile) as f:
        configs = yaml.load(f, Loader=yaml.FullLoader)
    return configs

def readExcel(filename, sheetname):
    exeldf = pd.read_excel(filename, sheet_name=sheetname)
    return exeldf

def getCDEName(cdeid, version):
    #Safety valve if version is None
    if version is None:
        version = '1'
    url = "https://cadsrapi.cancer.gov/rad/NCIAPI/1.0/api/DataElement/"+str(cdeid)+"?version="+str(version)
    headers = {'accept':'application/json'}
    try:
        results = requests.get(url, headers = headers)
    except requests.exceptions.HTTPError as e:
        pprint.pprint(e)
    if results.status_code == 200:
        results = json.loads(results.content.decode())
        #print(results['DataElement'])
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

def cleanString(inputstring, description):
    if description:
        outputstring = re.sub(r'[\n\r\t?]+', '', inputstring)
        outputstring.rstrip()
    else:
        outputstring = re.sub(r'[\W]+', '', inputstring)
    
    return outputstring

def addNodes(datamodel, df):
    nodelist = df['Class_Name'].unique()
    for node in nodelist:
        nodeobj = Node({'handle': node})
        datamodel.add_node(nodeobj)
    return datamodel

def addProp(datamodel, df_row):
    nodename = df_row['Class_Name']
    propname = df_row['Attribute_Name']
    datatype = df_row['Data_Type']
    #Check to see if the property is already there
    nodes = datamodel.nodes
    existing = list(nodes[nodename].props.keys())
    #Only add a new property if it's not there
    if propname not in existing:
        nodeobj = datamodel.nodes[nodename]
        propobj = Property({'handle': propname, "_parent_handle": nodename, 'is_required': 'No', 'value_domain': datatype})
        datamodel.add_prop(nodeobj, propobj)
    return datamodel

def addTerm(datamodel, df):
    #Note that this assumes all entires in the df have a CDE

    proplist = df['Attribute_Name'].unique()
    for prop in proplist:
        prop_df = df[df['Attribute_Name'] == prop]
        for index, row in prop_df.iterrows():
            node = row['Class_Name']
            if 'ID' in row['Tag_Name']:
                cdeid = row['Tag_Value']
            elif 'Version' in row['Tag_Name']:
                cdever = row['Tag_Value']
                if 'http' in str(cdever):
                    temp = cdever.split('=')
                    cdever = temp[-1]
            elif 'URL' in row['Tag_Name']:
                cdeurl = row['Tag_Value']
        # Fetch the official CDE name and description from caDSR
        cdename, description = getCDEName(cdeid, cdever)
        termvalues = {'handle': prop, 'value':cdename, 'origin_version':cdever, 'origin_name':'caDSR', 'origin_id':cdeid, 'origin_definition': description, 'nanoid': cdeurl}
        termobj = Term(termvalues)
        propobj = datamodel.props[(node, prop)]
        datamodel.annotate(propobj, termobj)
    return datamodel

def addEdge(datamodel, edge_df):
    relterms = {"0..*":"many", "1..*":"one", "1":"one", "0..1":"one", 1:"one"}
    for item, row in edge_df.iterrows():
        source_node = datamodel.nodes[row['Class_Name']]
        target_node = datamodel.nodes[row['Target_Class']]
        handle = f"{row['Class_Name']}_to_{row['Target_Class']}"
        source_card = row['Source_Card']
        target_card = row['Destination_Card']
        source_term = relterms[source_card]
        target_term = relterms[target_card]
        mult_string = source_term+"_to_"+target_term
        edgeobj = Edge({'handle':handle, 'multiplicity':mult_string, 'src':source_node, 'dst':target_node})
        datamodel.add_edge(edgeobj)
    return datamodel


def main(args):
    configs = readConfigs(args.config)
    excel_df = readExcel(configs['excelfile'], configs['worksheet'])

    newmodel = Model(handle="CRDCSubmission")

    #Add the nodes
    newmodel = addNodes(newmodel, excel_df)

    # And now try adding properties to nodes
    prop_df = excel_df[excel_df['Object_Type'] == 'Attribute']
    for index, row in prop_df.iterrows():
        newmodel = addProp(newmodel, row)

    # Finally, add the Term section to any properties that have a CDE
    # Build a df of properties that have a CDE
    cde_df = excel_df[excel_df['Tag_Name'].str.contains('CDE')]
    newmodel = addTerm(newmodel, cde_df)

    #And even more finallly, add the relationships
    # For whatever reason, empty cells in Excel result in a space in pandas
    # Note the the spreadsheet does mark Associations in the Object_Type row, but many of them are blank, so this looks for ones with an established relationship 
    # Similarly, Association_Name is very sparse, so we build our own
    rel_df = excel_df[excel_df['Source_Card'] != " "]
    newmodel = addEdge(newmodel, rel_df)

    #Make an MDF object
    #mdfmodel = MDF(handle='CRDC', model=newmodel)
    #print(mdfmodel)




    if args.verbose:
        nodes = newmodel.nodes
        props = newmodel.props
        terms = newmodel.terms
        edges = newmodel.edges

        for node in nodes:
           print(nodes[(node)].get_attr_dict())
        for prop, value in props:
            print(props[(prop, value)].get_attr_dict())
        for handle, value in terms:
            print(terms[(handle,value)].get_attr_dict())
        for edge, value, value2 in edges:
            print(edges[(edge, value, value2)].get_attr_dict())

    if args.writefile:
        nodes = newmodel.nodes
        props = newmodel.props
        terms = newmodel.terms
        edges = newmodel.edges
        with open('summary.txt', 'w') as f:
            f.write("MODEL NODES\n")
            for node in nodes:
                f.write(str(nodes[(node)].get_attr_dict())+"\n")
            f.write("MODEL PROPERTIES\n")
            for prop, value in props:
                f.write(str(props[(prop, value)].get_attr_dict())+"\n")
            f.write("MODEL TERMS\n")
            for term, value in terms:
                f.write(str(terms[(term, value)].get_attr_dict())+"\n")
            f.write("MODEL RELATIONSHIPS\n")
            for edge, value1, value2 in edges:
                f.write(str(edges[(edge, value1, value2)].get_attr_dict())+"\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="Configuration file to convert CRDS Search XLS file to MDF")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-w", "--writefile", action="store_true", help="Write out summary.txt file")

    args = parser.parse_args()

    main(args)