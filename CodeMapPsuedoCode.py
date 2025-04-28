import os.path, sys, getopt, csv, json, os, datetime, logging
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from datetime import datetime

logger = logging.getLogger(__name__)
FORMAT = '%(asctime)s %(levelname)-1s %(funcName)s:%(lineno)d %(message)s'

def fileexist( filename ):
  return os.path.isfile(filename)

def fileReader( spark, filename ):
  logger.debug("In fileReader proc")
  global spark
  global json_data
  global xml_data
  global readFileType
  fn, fe = os.path.splitext(filename)
  if( fe in ('.csv','.txt') ):
    df = spark.read.csv(filename,mode="DROPMALFORMED",inferSchema=true,header=True)
    readFileType = "csv"
  elif( fe in ('.json') ):
    df = spark.read.option("multiline","true").json( filename )
    with open(filename) as jdata:
      json_data = json.load(jdata)
    readFileType = "json"
  elif( fe in ('.xml') ):
    df = spark.read \
      .format("com.databricks.spark.xml") \
      .option("rootTag","root") \
      .load(filename)
    xml_data = ET.parse(filename)
    readFileType = "xml"
  return df

def openTarget( spark, filename, *args, **kwargs ):
  logger.debug("In openTarget proc.")
  global writeFileType
  fieldnames = kwargs.get('fieldnames',None)
  fn, fe = os.path.splitext(filename)
  if( fe in ('.csv','.txt') ):
    if( fieldnames is None ):
      logger.error('No fieldnames were created for the csv file.')
      exit(3)
    csvfile = open(filename, 'a', newline='')
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writeFileType = "csv"
  elif( fe in ('.json') ):
    csvfile = None
    writer = None
    writeFileType = "json"
  return csvfile, writer

def removeXMLAttribute( string ):
  tstring = string
  if( readFileType != 'XML'):
    tstring = string.replace('@value','')
  return tstring

def removeatfromstring( string ):
  tstring = string
  if( readFileType != 'XML'):
    tstring = string.replace('@','')
  return tstring

def checkVersion(ev):
  # Checking Python version:
  cv = float(sys.version[0]) + float(sys.version[2])/10
  if( ev > cv ):
    print("Error: Script developed and tested with Python " + str(ev) + ".")
    logger.error("Error: Script developed and tested with Python " + str(ev) + ".")
    exit(2)
  return 0

def COALESCE( defaultValue, *args):
  result = defaultValue
  for arg in args:
    if( arg != null ):
      result = arg
      break
  return result

def COUNT( ):
  return ''

def processJsonList( procList, extractField, filterField, filterValue ):
  if( extractField is not None ):
    extractField = extractField.replace( "@value", "" )
  if( filterField is not None ):
    filterField = filterField.repalce( "@", "" )
  for x in procList:
    if( filterValue is None ):
      if( x == extractField ):
        value = procList[extractField]
        break
      else:
        value = x[extractField]
    else:
      if( x[filterField] == filterValue ):
        value = x[extractField]
        break
  return value

def getArrayValue( extractList, json_data ):
  c= 0
  for extract in extractList:
    if( c == 0 ):
      procList = json_data[extract['extractField']]
    else:
      procList = processJsonList( procList, extract['extractField'], extract['filterField'], extract['filterValue'] )
    c += 1
  return procList

def COUNT( extractList, filterField, filterValue, fieldToCount, json_data):
  output = getArrayValue( extractList, json_data )
  c = 0
  for item in output:
    if( item[filterField] == filterValue ):
      if( fieldToCount in str(item) ):        c += 1
  return c

def QUERYVALUE_MAP( df, srcValue, srcMdlItemId, srcMdlVersion, tgtMdlItemId, tgtMdlVersion, srcTable, srcColumn, tgtTable, tgtColumn, vmField, vmtgtField):
  # Param 1 - Dataframe
  # Param 2 - Source Value
  # Param 3 - Source Model ID
  # Param 4 - Source Model Version
  # Param 5 - Target Model ID
  # Param 6 - Target Model Version
  # Param 7 - Source Table
  # Param 8 - Source Column
  # Param 9 - Target Table
  # Param 10 - Target Field Name
  # Param 11 - Lookup Source Column Name
  # Param 12 - Result Column

  global spark
  # cach the dataFrame
  df.cache()

  # create a temporary view
  df.createOrReplaceTempView("VALUE_MAP")

  sqlstmt = 'SELECT ' + vmtgtField + ' as tgtResult FROM VALUE_MAP WHERE SOURCE_MODEL_PUBLIC_ID = \'' + srcMdlItemId + '\' AND SOURCE_MODEL_VERSION = \'' + srcMdlVersion + '\' AND TARGET_MODEL_PUBLIC_ID = \'' + tgtMdlItemId + '\' AND TARGET_MODEL_VERSION = \'' + tgtMdlVersion + '\' AND SOURCE = \'' + srcTable + '.' + srcColumn + '\' AND TARGET = \'' + tgtTable + '.' + tgtColumn + '\' AND ' + vmField.upper() + ' = \'' + srcValue + '\''
  dfRS = spark.sql(sqlstmt)
  df.unpersist()
  return dfRS['tgtResult']

def QUERYXWALK(df, sourceTerminology, selectColumn, inputValue, targetTerminology, targetColumn ):
  # Param 1 - XWALK DataFrame
  # Param 2 - Source Terminology
  # Param 3 - Select Column
  # Param 4 - Input Value
  # Param 5 - Target Terminology
  # Param 6 - Target Column
  global spark
  # cache the dataFrame
  df.cache()

  # create a temporary view
  df.createOrReplaceTempView("XWALK")

  sqlstmt = 'SELECT "' + targetColumn + '" as tgtResult FROM XWALK where "SOURCE_TERMINOLOGY" = \'' + sourceTerminology + '\' AND "TARGET_TERMINOLOGY" = \'' + targetTerminology + '\' AND ' + selectColumn.upper() + ' = \'' + inputValue + '\''
  dfRS = spark.sql(sqlstmt)
  df.unpersist()
  return dfRS['tgtResult']

def getHour( datestring ):
  dt = datetime.strptime( datestring, '%Y-%m-%d %H:%M:%S')
  return dt.hour

def getMinute( datestring ):
  dt = datetime.strptime( datestring, '%Y-%m-%d %H:%M:%S')
  return dt.minute

def getSecond( datestring ):
  dt = datetime.strptime( datestring, '%Y-%m-%d %H:%M:%S')
  return dt.second

def getMonth( datestring ):
  dt = datetime.strptime( datestring, '%Y-%m-%d %H:%M:%S')
  return dt.month

def getDay( datestring ):
  dt = datetime.strptime( datestring, '%Y-%m-%d %H:%M:%S')
  return dt.day

def getYear( datestring ):
  dt = datetime.strptime( datestring, '%Y-%m-%d %H:%M:%S')
  return dt.year

def MIN( df, tgtField, srcField, srcValue ):
  global spark

  df.createOrReplaceTempView("FINDMIN")

  sqlStmt = "SELECT MIN(" + tgtField + ") as RESULT FROM FINDMIN WHERE " + srcField + " = '" + srcValue + "' GROUP BY " + srcField

  dfRS = spark.sql(sqlStmt)

  return dfRS['RESULT']

def MAX( df, tgtField, srcField, srcValue ):
  global spark

  df.createOrReplaceTempView("FINDMAX")

  sqlStmt = "SELECT MAX(" + tgtField + ") as RESULT FROM FINDMAX WHERE " + srcField + " = '" + srcValue + "' GROUP BY " + srcField

  dfRS = spark.sql(sqlStmt)

  return dfRS['RESULT']

def add( val1, val2 ):
  return val1 + val2

def subtract( val1, val2 ):
  return val2 - val1

def multiple( val1, val2 ):

  return val1 * val2

def divide( val1, val2 ):
  if( val2 == 0 ):
    return 0
  else:
    return val1/val2


def processData( spark, DemographicsfileName, participantfileName):
  logger.debug('In processData proc.')  Demographicsdf = fileReader( spark, DemographicsfileName)
  data_itr = Demographicsdf.rdd.toLocalIterator()
 ]
  csvFile, writer = openTarget( spark, participantfileName)
  for row in data_itr:
    participant_id = row[removeXMLAttribute( HTAN_PARTICIPANT_ID )]
    ethnicity = row[removeXMLAttribute( ETHNIC_GROUP )]
    race = row[removeXMLAttribute( RACE )]
    writer.writerow({'participant_id' : participant_id, 'ethnicity' : ethnicity, 'race' : race})
  if( csvFile is not None ):
    logger.debug("Closing file")
    csvFile.close()


def main(argv, prg):
  logger.debug('In main prod')
  outputfileName = ''
  DemographicsfileName = ''
  # Create Spark Session
  sc = SparkContext("local", "My App")
  spark = SparkSession.builder.getOrCreate()

  try:
    opts, args = getopt.getopt(argv,"ha:o:")
  except getopt.GetoptError:
    print( 'Usage: ', prg, ' -a <Demographicsfilename> -o <participantfilename> ')
    sys.exit(2)

  for opt, arg in opts:
    if opt == '-h':
      print( 'Usage: ', prg, ' -a <Demographicsfilename> -o <participantfilename>' )
      sys.exit()
    elif opt == '-a':
      DemographicsfileName = arg
    elif opt == '-o':
      participantfileName = arg

  if fileexist(DemographicsfileName) == False:
    print ('Input file ', DemographicsfileName, ' does not exist.')
    logger.error('Input file ', DemographicsfileName, ' does not exist.')
    sys.exit(2)

  processData( spark, DemographicsfileName, participantfileName)

if __name__ == '__main__':
  logging.basicConfig(filename='codegenerator.log', level=logging.INFO, format=FORMAT)
  logger.info('Started')
  expected_version = 3.6
  checkVersion( expected_version )
  na = len(sys.argv)
  if na < 3:
    sys.argv.append('-h')
  main(sys.argv[1:],sys.argv[0])
  logger.info('Finished')
