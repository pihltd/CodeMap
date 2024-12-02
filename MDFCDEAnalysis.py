import bento_mdf
import argparse
from crdclib import crdclib

def main(args):
    configs = crdclib.readYAML(args.configfile)
    
    mdffiles = configs['mdffiles']
    mdfmodel = bento_mdf.MDF(*mdffiles, handle=configs['handle'])
    
    props = mdfmodel.model.props
    print(f"Total number of properties:\t{len(props)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configfile", required=True,  help="Configuration file containing all the input info")

    args = parser.parse_args()

    main(args)