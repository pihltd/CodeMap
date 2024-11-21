#Transform a Code Map mapping file to a CTOS transformation file
import pandas as pd
import argparse
from crdclib import crdclib

def main(args):
    configs = crdclib.readYAML(args.configfile)
    codemap_df = pd.read_excel(configs['property_mapping'],sheet_name=configs['property_tab'])
    
    # {Code map field: CTOS Field}
    field_mapping = {"Source Model Version": "lift_from_version", "Source Element": "lift_from_node", "Source Characteristic": "lift_from_property", "Target Model Version": "lift_to_version",
                     "Target Element": "lift_to_node", "Target Characteristic": "lift_to_property"}
    
    # Create a new df with just the needed columns
    ctos_df = codemap_df.loc[:,list(field_mapping.keys())]
    # Rename the columns to the CTOS standard names
    ctos_df.rename(columns=field_mapping, inplace=True)
    # If asked for (and it should be), drop any rows with a blank lift_from_property
    if configs['drop_source_null']:
        ctos_df = ctos_df[ctos_df['lift_from_property'].notna()]
    
    ctos_df.to_csv(configs['outputfile'], sep="\t", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)