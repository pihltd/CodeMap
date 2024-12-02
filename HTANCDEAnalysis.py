import pandas as pd
from crdclib import crdclib

def getCDEID(cdeurl):
    linestuff = cdeurl.split("=")
    version = linestuff[3]
    cdeid = linestuff[2].split("&")[0]
    return cdeid, version


def getCDEInfo(cdejson):
    if cdejson['status'] == 'success':
        context = cdejson['DataElement']['context']
        cdename = cdejson['DataElement']['longName']
    else:
        context = "No caDSR Entry"
        cdename = "Bum CDE"
    return context, cdename

htan_df = pd.read_csv(r"C:\Users\pihltd\Documents\github\data-models\HTAN.model.csv")
print(f"Number of attributes:{htan_df.shape[0]}")
print(f"Number of attributes with a source: {htan_df['Source'].count()}")

columns = ["Attribute", "Identifier", "Owner", "Alternate Name"]
summary_df = pd.DataFrame(columns=columns)

cde_df = htan_df.loc[htan_df['Source'].notnull()]
#print(cde_df)
for index, row in cde_df.iterrows():
    if "cadsr.cancer.gov" in row['Source']:
        cdeid, cdeversion = getCDEID(row['Source'])
        cdejson = crdclib.getCDERecord(cdeid, cdeversion)
        cdecontext, cdename = getCDEInfo(cdejson)
        summary_df.loc[len(summary_df)] = {"Attribute": row['Attribute'], "Identifier": cdeid, "Owner": cdecontext, "Alternate Name": cdename}
    elif "gdc.cancer.gov" in row['Source']:
        summary_df.loc[len(summary_df)] = {"Attribute": row['Attribute'], "Identifier": None, "Owner": "GDC/EVS", "Alternate Name": row['Source']}
    elif "miti" in row["Source"]:
        summary_df.loc[len(summary_df)] = {"Attribute": row['Attribute'], "Identifier": None, "Owner": "MITI", "Alternate Name": row['Source']}
    elif "purl.obolibrary.org" in row['Source']:
        summary_df.loc[len(summary_df)] = {"Attribute": row['Attribute'], "Identifier": None, "Owner": "OBO", "Alternate Name": row['Source']}
    elif "dataservice.datacommons.cancer.gov" in row["Source"]:
        summary_df.loc[len(summary_df)] = {"Attribute": row['Attribute'], "Identifier": None, "Owner": "CDS/HTAN", "Alternate Name": row['Source']}
    elif "google" in row["Source"]:
        summary_df.loc[len(summary_df)] = {"Attribute": row['Attribute'], "Identifier": None, "Owner": "HTAN", "Alternate Name": row['Source']}
    elif "humancellatlas" in row["Source"]:
        summary_df.loc[len(summary_df)] = {"Attribute": row['Attribute'], "Identifier": None, "Owner": "HTAN", "Alternate Name": row['Source']}
    else:
        summary_df.loc[len(summary_df)] = {"Attribute": row['Attribute'], "Identifier": None, "Owner": "Other", "Alternate Name": row['Source']}
        
summary_df.to_csv(r"C:\Users\pihltd\Documents\github\data-models\HTAN.Summary.csv", sep="\t")

with open(r"C:\Users\pihltd\Documents\github\data-models\HTAN.SummaryReport.txt", "w") as f:
    f.write(f"Number of attributes:\t{htan_df.shape[0]}\n")
    f.write(f"Number of attributes with a source:\t{htan_df['Source'].count()}\n")
    f.write(f"Number of attribtues with a caDSR CDE ID:\t{summary_df['Identifier'].count()}\n")
    f.write(f"Owner Counts:\n{summary_df['Owner'].value_counts()}")