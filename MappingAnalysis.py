import bento_mdf
import argparse
import pandas as pd
from crdclib import crdclib



def main(args):
    configs = crdclib.readYAML(args.configfile)
    datamodel = bento_mdf.MDF(*configs['mdffiles'])
    
    #Need a list of the required properties
    props = datamodel.model.props
    reqprops = []
    for prop in props:
        info = props[prop]
        propinfo = info.get_attr_dict()
        if propinfo['is_required'] == 'True':
                reqprops.append(propinfo['handle'])
    #print(f"Required Properties:\n{reqprops}")
    
    #Create a dataframe from the Code Map excel sheet
    map_df = pd.read_csv(configs['mapfile'])
    #print(df.head())
    
    
    #check if the field is in the "*Source Element Physical Name" field
    for prop in reqprops:
        #result = map_df.loc[map_df["*Source Element Physical Name"] == prop]
        result = map_df.loc[map_df["*Source Characteristic Physical Name"] == prop]
        if len(result.index) > 0:
            print(f"Query term: {prop}\nResult: {result}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)