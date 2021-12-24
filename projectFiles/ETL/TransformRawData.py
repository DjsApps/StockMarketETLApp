"""
# STEP-2: Transform the data into the format which we required like get the company name from file name
# CreatedBy: Debajyoti Saha
# CreatedOn: 12-DEC-2021
"""

class TransformRAWData:
    def __init__(self):
        self.strName = 'TransformRAWData'

    def __DropDuplicateFromDataframe(self,dfSource,distinctColumns):
        df_result = dfSource.dropDuplicates(distinctColumns)
        return df_result
    
    def equityDataInsert(self,spark,strConnectionFormat,strPGJdbcUrl,strTableName,strPGUsername,strPGPassword,strPGDriver,dictSymbolTableMapping,obj_CommonClass,strMode):
        try:
            print("[I] Equity Data Transformation Started")
            df_PG = obj_CommonClass.f_ReadPostgreTableData(spark,strConnectionFormat,strPGJdbcUrl,strTableName,strPGUsername,strPGPassword,strPGDriver)
            print('[I] Total Count from table {1}: {0}'.format(df_PG.count(),strTableName))
            df_PG = self.__DropDuplicateFromDataframe(df_PG,["trade_date","file_name"])
            for keys in dictSymbolTableMapping.keys():
                df_result = df_PG.select("*").filter(f"file_name like '%{keys}%'")
                print('{0}: {1}'.format(keys,df_result.count()))
                df_final_result = df_result.select("trade_date","series","open_price","day_high","day_low","prev_day_close","ltp","closing_price","vwap","fifty_two_week_high","fifty_two_week_low","trade_volume","value","no_of_trades")
                strInserTableName = dictSymbolTableMapping[keys]
                obj_CommonClass.f_LoadDataIntoPostgreSQL(df_final_result,strMode,strConnectionFormat, strPGJdbcUrl, strInserTableName, strPGUsername, strPGPassword, strPGDriver)
            v_status = "Success"
            return v_status
        except Exception as e:
            print("[E] <<<---Error in equityDataInsert--->>>\n")
            print(str(e))
            v_status = "Error"
            return v_status
        finally:
            if (v_status == 'Success'):
                print("[I] Equity Data transformation Completed Successfully")
            else:
                print("[I] Error in Equity Data Transformation")

    def indicesDataInsert(self,spark,strConnectionFormat,strPGJdbcUrl,strTableName,strPGUsername,strPGPassword,strPGDriver,dictindicesTableMapping,obj_CommonClass,strMode):
        try:
            print("[I] NSE Indices Data Transformation Started")
            df_PG = obj_CommonClass.f_ReadPostgreTableData(spark,strConnectionFormat,strPGJdbcUrl,strTableName,strPGUsername,strPGPassword,strPGDriver)
            print('[I] Total Count from table {1}: {0}'.format(df_PG.count(),strTableName))
            df_PG = self.__DropDuplicateFromDataframe(df_PG,["symbol","record_date"])
            for keys in dictindicesTableMapping.keys():
                df_result = df_PG.select("*").filter(f"file_name like '%{keys}%'")
                print('{0}: {1}'.format(keys,df_result.count()))
                df_final_result = df_result\
                                    .withColumnRenamed("symbol","index_symbol")\
                                    .withColumnRenamed("open_price","open_price")\
                                    .withColumnRenamed("high_price","high_price")\
                                    .withColumnRenamed("low_price","low_price")\
                                    .withColumnRenamed("previous_day_close_price","previous_day_close_price")\
                                    .withColumnRenamed("ltp","ltp")\
                                    .withColumnRenamed("change_price","change_price")\
                                    .withColumnRenamed("change_percentage","change_percentage")\
                                    .withColumnRenamed("volume","volume")\
                                    .withColumnRenamed("value","value")\
                                    .withColumnRenamed("fiftytwo_weeks_high","fiftytwo_weeks_high")\
                                    .withColumnRenamed("fiftytwo_weeks_low","fiftytwo_weeks_low")\
                                    .withColumnRenamed("threesixtyfive_days_percentage_chng","threesixtyfive_days_percentage_chng")\
                                    .withColumnRenamed("thirty_day_percentage_chng","thirty_day_percentage_chng")\
                                    .withColumnRenamed("record_date","record_date")
                df_final_result = df_final_result.select("record_date","index_symbol","open_price","high_price","low_price","previous_day_close_price","ltp","change_price","change_percentage","volume","value","fiftytwo_weeks_high","fiftytwo_weeks_low","threesixtyfive_days_percentage_chng","thirty_day_percentage_chng")
                strInserTableName = dictindicesTableMapping[keys]
                obj_CommonClass.f_LoadDataIntoPostgreSQL(df_final_result,strMode,strConnectionFormat, strPGJdbcUrl, strInserTableName, strPGUsername, strPGPassword, strPGDriver)
            v_status = "Success"
            return v_status
        except Exception as e:
            print("[E] <<<---Error in indicesDataInsert--->>>\n")
            print(str(e))
            v_status = "Error"
            return v_status
        finally:
            if (v_status == 'Success'):
                print("[I] NSE Indices Data transformation Completed Successfully")
            else:
                print("[I] Error in NSE Indices Data Transformation")

    def ShareholdingDataInsert(self,spark,strConnectionFormat,strPGJdbcUrl,strTableName,strPGUsername,strPGPassword,strPGDriver,dictShareholdingTableMapping,obj_CommonClass,strMode):
        try:
            print("[I] NSE Shareholding Data Transformation Started")
            df_PG = obj_CommonClass.f_ReadPostgreTableData(spark,strConnectionFormat,strPGJdbcUrl,strTableName,strPGUsername,strPGPassword,strPGDriver)
            print('[I] Total Count from table {1}: {0}'.format(df_PG.count(),strTableName))
            for keys in dictShareholdingTableMapping.keys():
                df_result = df_PG.select("*").filter(f"upper(file_name) like '%{keys}%'")
                print('{0}: {1}'.format(keys,df_result.count()))
                df_final_result = df_result.withColumn("record_date",to_timestamp(df_result.holdings_till_date,'dd-MMM-yyyy'))
                df_final_result = df_final_result.select(col("record_date").alias("holdings_till_date"),col("company_name"),col("promotors_holdings"),col("public_holdings"),col("employee_holdings"))
                strInserTableName = dictShareholdingTableMapping[keys]
                obj_CommonClass.f_LoadDataIntoPostgreSQL(df_final_result,strMode,strConnectionFormat, strPGJdbcUrl, strInserTableName, strPGUsername, strPGPassword, strPGDriver)
            v_status = "Success"
            return v_status
        except Exception as e:
            print("[E] <<<---Error in ShareholdingDataInsert--->>>\n")
            print(str(e))
            v_status = "Error"
            return v_status
        finally:
            if (v_status == 'Success'):
                print("[I] NSE Shareholding Data transformation Completed Successfully")
            else:
                print("[I] Error in NSE Share Holding Data Transformation")


if __name__ == '__main__':
    # Importing Libraries
    try:
        import sys,os,datetime,time
        sys.path.insert(1,"projectFiles\CommonClasses")
        from pyspark.sql.functions import lit, col, date_format, to_date,to_timestamp, substring , instr
        import CommonClasses
        # Creating/Initiating Spark Session
        strAppName      = 'ETL-TransformRawData'
        obj_CommonClass = CommonClasses.CommonClasses()
        spark = obj_CommonClass.f_InitiateSparkSession(strAppName)
        strConfigFilePath       = './AppConfig.json'
        config_DF = spark.read.format("json").option("multiline", True).load(strConfigFilePath).cache()
        strEquityTableName      = config_DF.select('TABLE_NAME_FOR_RAW_DATA').first()[0]
        strIndicesTableName     = config_DF.select('TABLE_NAME_FOR_NIFTY_INDICES_DATA').first()[0]
        strShareholdingTableName= config_DF.select('TABLE_NAME_FOR_SHAREHOLDING_PATTERN_DATA').first()[0]
        strPGJdbcUrl            = config_DF.select('JDBC_URL').first()[0]
        strPGUsername           = config_DF.select('POSTGRESQL_USERNAME').first()[0]
        strPGPassword           = config_DF.select('POSTGRESQL_PASSWORD').first()[0]
        strPGDriver             = config_DF.select('POSTGRESQL_DRIVER').first()[0]
        strConnectionFormat     = config_DF.select('PG_CONNECTION_FORMAT').first()[0]
        dictSymbolTableMapping  = config_DF.select('SYMBOL_TO_TABLE_MAPPING').first()[0][0].asDict()
        dictindicesTableMapping  = config_DF.select('INDICES_TO_TABLE_MAPPING').first()[0][0].asDict()
        dictShareholdingTableMapping  = config_DF.select('SHAREHOLDING_TO_TABLE_MAPPING').first()[0][0].asDict()
        strMode                 = config_DF.select('PG_DATALOAD_MODE').first()[0]
        config_DF.unpersist()
        print('[I] Variables Loaded from Config File {0}'.format(strConfigFilePath))
        objTransformRAWData = TransformRAWData()
        v_status = objTransformRAWData.equityDataInsert(spark,strConnectionFormat,strPGJdbcUrl,strEquityTableName,strPGUsername,strPGPassword,strPGDriver,dictSymbolTableMapping,obj_CommonClass,strMode)
        v_status = objTransformRAWData.indicesDataInsert(spark,strConnectionFormat,strPGJdbcUrl,strIndicesTableName,strPGUsername,strPGPassword,strPGDriver,dictindicesTableMapping,obj_CommonClass,strMode)
        v_status = objTransformRAWData.ShareholdingDataInsert(spark,strConnectionFormat,strPGJdbcUrl,strShareholdingTableName,strPGUsername,strPGPassword,strPGDriver,dictShareholdingTableMapping,obj_CommonClass,strMode)
    finally:
        obj_CommonClass.f_CloseSparkSession(spark)