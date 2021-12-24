# This classes are for Commonly used functions and others parameters
from pyspark.sql import SparkSession
import sys,datetime
import shutil

class CommonClasses:
    """
    # CommonClasses is used to define the Commonly used functions or variables in the project
    # CreatedBy: Debajyoti Saha
    # CreatedOn: 11-DEC-2021
    """
    def __init__(self):
        self.strName = "CommonClasses"
    
    def f_InitiateSparkSession(self,strApplicationName):
        """
        # Create a Spark Session and return spark session
        # CreatedBy: Debajyoti Saha
        # CreatedOn: 11-DEC-2021
        """
        try:
            print('[I] Creating Spark Session')
            spark = SparkSession.builder\
                .appName(strApplicationName)\
                .getOrCreate()
            # Adding External Class folder path to the env
            strConfigFilePath = './AppConfig.json'
            config_DF = spark.read.format("json").option("multiline", True).load(strConfigFilePath).cache()
            listClassPaths = config_DF.select('REGISTERED_USER_CLASS_PATH').first()[0]
            for strPath in listClassPaths:
                sys.path.insert(1,strPath)

            # Settig the Log Level
            strLogLevel = config_DF.select('LOG_LEVEL').first()[0] 
            spark.sparkContext.setLogLevel(strLogLevel)
            strLogFolderPath = config_DF.select('LOG_FILE_PATH').first()[0]
            strLogFileName = strApplicationName + '-' + datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")+'.log'
            sys.stdout = open(strLogFolderPath+strLogFileName,'w+')
            print('[I] Creating Spark Session')
            v_ResultFlag = "Success"
            print('[I] Spark Session Created for {0} App'.format(strApplicationName))
            return spark
        except Exception as e:
            print('[E]<<<--Error in f_InitiateSparkSession-->>>\n')
            print(str(e))
            v_ResultFlag = "ERROR"
        finally:
            if (v_ResultFlag == "Success"):
                print("[I] Spark Session Initialize Completed Successfully")
            else:
                print("[I] Spark Session Initialize Completed With Error")
    
    def f_CloseSparkSession(self,spark):
        spark.stop()
        print('[I] Spark Session Stopped')
    
    def f_ReadCsvData(self,spark,strSchema,strCsvFilePath,strHeader=True,strFileFormat="csv"):
        """
        # Read CSV File and returns Spark Dataframe
        # CreatedBy: Debajyoti Saha
        # CreatedOn: 11-DEC-2021
        """
        try:
            print('[I] CSV Read started for {0}'.format(strCsvFilePath))
            df_ResultCSV = spark.read\
                .format(strFileFormat)\
                .schema(strSchema)\
                .option("header",strHeader)\
                .load(strCsvFilePath)
            print('[I] CSV Read Completed')
            v_ResultFlag = "Success"
            return df_ResultCSV
        except Exception as e:
            print('[E]<<<--Error in f_ReadCsvData-->>>\n')
            print(str(e))
            v_ResultFlag = "ERROR"
        finally:
            if (v_ResultFlag == "Success"):
                print("[I] CSV Read Completed Successfully")
            else:
                print("[I] CSV Read Completed with Error")

    def f_LoadDataIntoPostgreSQL(self,dfSourceDataframe,strMode,strConnectionFormat,strUrl,strTableName,strUserName,strPassword,strDriverName):
        """
        # Load the Data into PostgreSQL Table(Supports Single Table Load) from Spark Dataframe
        # CreatedBy: Debajyoti Saha
        # CreatedOn: 11-DEC-2021
        """
        try:
            print('[I] Data Load Strated to {0} Table'.format(strTableName))
            dfSourceDataframe.write\
                .mode(strMode)\
                .format(strConnectionFormat)\
                .option("url",strUrl)\
                .option("dbtable",strTableName)\
                .option("user",strUserName)\
                .option("password",strPassword)\
                .option("driver",strDriverName)\
                .save()
            print('[I] {0} Records Loaded into {1} Table in {2} url'.format(dfSourceDataframe.count(),strTableName,strUrl))
            v_ResultFlag = "Success"
            return v_ResultFlag
        except Exception as e:
            print('[E] <<<--Error in f_LoadDataIntoPostgreSQL-->>>\n')
            print(str(e))
            v_ResultFlag = "ERROR"
            return v_ResultFlag
        finally:
            if (v_ResultFlag == "Success"):
                print('[I] Data Load Completed Successfully')
            else:
                print('[I] Data Load Completed with Error')

    def f_MoveDirectory(self,strSourcePath, strTargetPath):
        """
        # Move Processed Files to different directory based on processing Stage
        # CreatedBy: Debajyoti Saha
        # CreatedOn: 12-DEC-2021
        """
        shutil.move(strSourcePath,strTargetPath)

    def f_ReadPostgreTableData(self,spark,strConnectionFormat,strPGJdbcUrl,strTableName,strPGUsername,strPGPassword,strPGDriver):
        """
        # Used to create dataframe based on postgresql table
        # CreatedBy: Debajyoti Saha
        # CreatedOn: 13-Dec-2021
        """
        try:
            print('[I] Postgresql Reader strated')
            df_Results = spark.read.format(strConnectionFormat)\
                .option("url",strPGJdbcUrl)\
                .option("dbtable",strTableName)\
                .option("user",strPGUsername)\
                .option("password",strPGPassword)\
                .option("driver",strPGDriver)\
                .load()
            v_ResultFlag = "Success"
            return df_Results
        except Exception as e:
            print('[E] <<<---->>>')
            print(str(e))
            v_ResultFlag = "ERROR"
        finally:
            if (v_ResultFlag == "Success"):
                print("[I] Dataframe created {0} Successfully".format(strTableName))
            else:
                print("[I] Error While Dataframe creation")