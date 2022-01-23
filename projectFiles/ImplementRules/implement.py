

if __name__ == '__main__':
    try:
        import sys,os,datetime,time
        sys.path.insert(1,"projectFiles\CommonClasses")
        from pyspark.sql.functions import lit, col, date_format, to_date,to_timestamp, substring , instr
        import CommonClasses
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
        # Created for testing and intial code writting
        strTableName            = "stocks.nse_mahindra_and_mahinra"
        config_DF.unpersist()
        print('[I] Variables Loaded from Config File {0}'.format(strConfigFilePath))
        df_PG = obj_CommonClass.f_ReadPostgreTableData(spark,strConnectionFormat,strPGJdbcUrl,strTableName,strPGUsername,strPGPassword,strPGDriver)
    finally:
        obj_CommonClass.f_CloseSparkSession(spark)
        