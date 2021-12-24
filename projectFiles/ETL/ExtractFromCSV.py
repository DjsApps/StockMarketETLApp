"""
# STEP-1: Load the raw csv data to PostgreSQL Table 
# CreatedBy: Debajyoti Saha
# CreatedOn: 09-DEC-2021
"""

class LoadStockMarketToPG:
    def __init__(self):
        self.strName = "LoadStockMarketToPG"

    def f_LoadEQUITYMarketCSVToPGTable(self,spark,varFileFormat,varProcessFolderPath,csvFile):
        try:
            schema_obj = StockMarketCsv.StockMarketCsv()
            CSVSchema = schema_obj.RawDataSchema()
            strTableName = config_DF.select('TABLE_NAME_FOR_RAW_DATA').first()[0]
            reader_DF = obj_CommonClass.f_ReadCsvData(spark,CSVSchema,varProcessFolderPath+csvFile,True,varFileFormat)
            reader_DF = reader_DF.withColumn("trade_date",to_timestamp(reader_DF.Date,'dd-MMM-yyyy'))\
                                .withColumnRenamed("series","series")\
                                .withColumnRenamed("OPEN","open_price")\
                                .withColumnRenamed("HIGH","day_high")\
                                .withColumnRenamed("LOW","day_low")\
                                .withColumnRenamed("PREV. CLOSE","prev_day_close")\
                                .withColumnRenamed("ltp","ltp")\
                                .withColumnRenamed("close","closing_price")\
                                .withColumnRenamed("vwap","vwap")\
                                .withColumnRenamed("52W H","fifty_two_week_high")\
                                .withColumnRenamed("52W L","fifty_two_week_low")\
                                .withColumnRenamed("VOLUME","trade_volume")\
                                .withColumnRenamed("VALUE","value")\
                                .withColumnRenamed("No of trades","no_of_trades")\
                                .withColumn("file_name",lit(csvFile))\
                                .withColumn("inserted_time",lit(datetime.datetime.now()))\
                                .withColumn("file_received_datetime",lit(str(time.ctime(os.path.getmtime(varProcessFolderPath+csvFile)))))
            final_reader_DF = reader_DF.select("trade_date","series","open_price","day_high","day_low","prev_day_close","ltp","closing_price","vwap","fifty_two_week_high","fifty_two_week_low","trade_volume","value","no_of_trades","file_name","inserted_time","file_received_datetime")
            obj_CommonClass.f_LoadDataIntoPostgreSQL(final_reader_DF,strMode,strConnectionFormat, strPGJdbcUrl, strTableName, strPGUsername, strPGPassword, strPGDriver)
            v_Status = "Success"
            return v_Status
        except Exception as e:
            print("[E] <<<--Error in f_LoadEQUITYMarketCSVToPGTable-->>>\n")
            print(str(e))
            v_Status = "Error"
            return v_Status
        finally:
            if (v_Status == "Success"):
                print('[I] {0} File Loading Processing Completed'.format(csvFile))
            else:
                print('[I] {0} File Loading Completed with Error'.format(csvFile))

    def f_LoadNiftyIndicesData(self,spark,varFileFormat,varProcessFolderPath,csvFile):
        try:
            schema_obj = StockMarketCsv.StockMarketCsv()
            CSVSchema = schema_obj.niftyIndexRawDataSchema()
            strTableName = config_DF.select('TABLE_NAME_FOR_NIFTY_INDICES_DATA').first()[0]
            record_date = csvFile[csvFile.find('.csv')-11:csvFile.find('.csv')]
            reader_DF = obj_CommonClass.f_ReadCsvData(spark,CSVSchema,varProcessFolderPath+csvFile,True,varFileFormat)
            reader_DF = reader_DF.withColumnRenamed("SYMBOL","symbol")\
                                .withColumnRenamed("OPEN","open_price")\
                                .withColumnRenamed("HIGH","high_price")\
                                .withColumnRenamed("LOW","low_price")\
                                .withColumnRenamed("PREV. CLOSE","previous_day_close_price")\
                                .withColumnRenamed("LTP","ltp")\
                                .withColumnRenamed("CHNG","change_price")\
                                .withColumnRenamed("%CHNG","change_percentage")\
                                .withColumnRenamed("VOLUME","volume")\
                                .withColumnRenamed("VALUE","value")\
                                .withColumnRenamed("52W H","fiftytwo_weeks_high")\
                                .withColumnRenamed("52W L","fiftytwo_weeks_low")\
                                .withColumnRenamed("365 D % CHNG","threesixtyfive_days_percentage_chng")\
                                .withColumnRenamed("30 D % CHNG","thirty_day_percentage_chng")\
                                .withColumn("file_name",lit(csvFile))\
                                .withColumn("inserted_time",lit(datetime.datetime.now()))\
                                .withColumn("file_received_datetime",lit(str(time.ctime(os.path.getmtime(varProcessFolderPath+csvFile)))))\
                                .withColumn("record_date",lit(datetime.datetime.strptime(record_date.upper(),"%d-%b-%Y")))
            final_reader_DF = reader_DF.select("symbol","open_price","high_price","low_price","previous_day_close_price","ltp","change_price","change_percentage","volume","value","fiftytwo_weeks_high","fiftytwo_weeks_low","threesixtyfive_days_percentage_chng","thirty_day_percentage_chng","file_name","inserted_time","file_received_datetime","record_date")
            obj_CommonClass.f_LoadDataIntoPostgreSQL(final_reader_DF,strMode,strConnectionFormat, strPGJdbcUrl, strTableName, strPGUsername, strPGPassword, strPGDriver)
            v_Status = "Success"
            return v_Status
        except Exception as e:
            print("[E] <<<--Error in f_LoadNiftyIndicesData-->>>\n")
            print(str(e))
            v_Status = "Error"
            return v_Status
        finally:
            if (v_Status == "Success"):
                print('[I] {0} File Loading Processing Completed'.format(csvFile))
            else:
                print('[I] {0} File Loading Completed with Error'.format(csvFile))

    def f_LoadShareHoldingpattern(self,spark,varFileFormat,varProcessFolderPath,csvFile):
        try:
            schema_obj = StockMarketCsv.StockMarketCsv()
            CSVSchema = schema_obj.shareHoldingPatternSchema()
            strTableName = config_DF.select('TABLE_NAME_FOR_SHAREHOLDING_PATTERN_DATA').first()[0]
            reader_DF = obj_CommonClass.f_ReadCsvData(spark,CSVSchema,varProcessFolderPath+csvFile,True,varFileFormat)
            reader_DF = reader_DF.withColumnRenamed("COMPANY","company_name")\
                                .withColumnRenamed("PROMOTER & PROMOTER GROUP (A)","promotors_holdings")\
                                .withColumnRenamed("PUBLIC (B)","public_holdings")\
                                .withColumnRenamed("SHARES HELD BY EMPLOYEE TRUSTS (C2)","employee_holdings")\
                                .withColumnRenamed("AS ON DATE","holdings_till_date")\
                                .withColumn("file_name",lit(csvFile))\
                                .withColumn("inserted_time",lit(datetime.datetime.now()))\
                                .withColumn("file_received_datetime",lit(str(time.ctime(os.path.getmtime(varProcessFolderPath+csvFile)))))
            final_reader_DF = reader_DF.select("company_name","promotors_holdings","public_holdings","employee_holdings","holdings_till_date","file_name","inserted_time","file_received_datetime")
            obj_CommonClass.f_LoadDataIntoPostgreSQL(final_reader_DF,strMode,strConnectionFormat, strPGJdbcUrl, strTableName, strPGUsername, strPGPassword, strPGDriver)
            v_Status = "Success"
            return v_Status
        except Exception as e:
            print("[E] <<<--Error in f_LoadShareHoldingpattern-->>>\n")
            print(str(e))
            v_Status = "Error"
            return v_Status
        finally:
            if (v_Status == "Success"):
                print('[I] {0} File Loading Processing Completed'.format(csvFile))
            else:
                print('[I] {0} File Loading Completed with Error'.format(csvFile))

    



if __name__ == '__main__':
    # Importing Libraries
    try:
        import sys,os,datetime,time
        sys.path.insert(1,"projectFiles\CommonClasses")
        from pyspark.sql.functions import lit, col, date_format, to_date,to_timestamp
        import CommonClasses
        # Creating/Initiating Spark Session
        strAppName      = 'ETL-LoadCSVToTable'
        obj_CommonClass = CommonClasses.CommonClasses()
        spark = obj_CommonClass.f_InitiateSparkSession(strAppName)
        import StockMarketCsv
        # Red Config Parameters
        strConfigFilePath       = './AppConfig.json'
        config_DF = spark.read.format("json").option("multiline", True).load(strConfigFilePath).cache()
        varRawFolderPath        = config_DF.select('RAW_DATA_FOLDER').first()[0]
        varInprogressFolderPath = config_DF.select('INPROGRESS_DATA_FOLDER').first()[0]
        varProcessedFolderPath  = config_DF.select('PROCESSES_DATA_FOLDER').first()[0]
        varErrorFolderPath      = config_DF.select('ERROR_DATA_FOLDER').first()[0]
        varFileFormat           = config_DF.select('FILE_FORMAT').first()[0]
        strPGJdbcUrl            = config_DF.select('JDBC_URL').first()[0]
        strPGUsername           = config_DF.select('POSTGRESQL_USERNAME').first()[0]
        strPGPassword           = config_DF.select('POSTGRESQL_PASSWORD').first()[0]
        strPGDriver             = config_DF.select('POSTGRESQL_DRIVER').first()[0]
        strMode                 = config_DF.select('PG_DATALOAD_MODE').first()[0]
        strConnectionFormat     = config_DF.select('PG_CONNECTION_FORMAT').first()[0]
        config_DF.unpersist()
        print('[I] Variables Loaded from Config File {0}'.format(strConfigFilePath))
        # Data Load Process Started
        # Calling Schema Class to get the schema of csv file        
        listCSVFiles = os.listdir(varRawFolderPath)
        obj_Load = LoadStockMarketToPG()
        for csvFile in listCSVFiles:
            if ('Quote-Equity' in csvFile):                
                obj_CommonClass.f_MoveDirectory(varRawFolderPath+csvFile,varInprogressFolderPath+csvFile)
                v_Status = obj_Load.f_LoadEQUITYMarketCSVToPGTable(spark,varFileFormat,varInprogressFolderPath,csvFile)
                if (v_Status == "Error"):
                    obj_CommonClass.f_MoveDirectory(varInprogressFolderPath+csvFile,varErrorFolderPath+csvFile)                
                obj_CommonClass.f_MoveDirectory(varInprogressFolderPath+csvFile,varProcessedFolderPath+csvFile)
            elif ('NIFTY' in csvFile):
                obj_CommonClass.f_MoveDirectory(varRawFolderPath+csvFile,varInprogressFolderPath+csvFile)
                v_Status = obj_Load.f_LoadNiftyIndicesData(spark,varFileFormat,varInprogressFolderPath,csvFile)
                if (v_Status == "Error"):
                    obj_CommonClass.f_MoveDirectory(varInprogressFolderPath+csvFile,varErrorFolderPath+csvFile)                
                obj_CommonClass.f_MoveDirectory(varInprogressFolderPath+csvFile,varProcessedFolderPath+csvFile)
            elif ('Shareholding-Pattern-equities' in csvFile):
                obj_CommonClass.f_MoveDirectory(varRawFolderPath+csvFile,varInprogressFolderPath+csvFile)
                v_Status = obj_Load.f_LoadShareHoldingpattern(spark,varFileFormat,varInprogressFolderPath,csvFile)
                if (v_Status == "Error"):
                    obj_CommonClass.f_MoveDirectory(varInprogressFolderPath+csvFile,varErrorFolderPath+csvFile)                
                obj_CommonClass.f_MoveDirectory(varInprogressFolderPath+csvFile,varProcessedFolderPath+csvFile)
    finally:
        obj_CommonClass.f_CloseSparkSession(spark)
