import sys,os,datetime,time
import psycopg2 as pg
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, date_format, to_date,to_timestamp
from pyspark.sql.types import StructType, StructField, ArrayType
from pyspark.sql.types import DateType, StringType, DecimalType

# Spark Session starts
spark = SparkSession.builder\
        .appName('ETL-LoadCSVToTable')\
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
print('[I] Spark Session Created')
# Read Config Files
strConfigFilePath = './AppConfig.json'
config_DF = spark.read.format("json").option("multiline", True).load(strConfigFilePath).cache()
varRawFolderPath = config_DF.select('RAW_DATA_FOLDER').first()[0]
varInprogressFolderPath = config_DF.select('INPROGRESS_DATA_FOLDER').first()[0]
varProcessedFolderPath = config_DF.select('PROCESSES_DATA_FOLDER').first()[0]
varFileFormat = config_DF.select('FILE_FORMAT').first()[0]
strPGJdbcUrl = config_DF.select('JDBC_URL').first()[0]
strPGUsername = config_DF.select('POSTGRESQL_USERNAME').first()[0]
strPGPassword = config_DF.select('POSTGRESQL_PASSWORD').first()[0]
strPGDriver = config_DF.select('POSTGRESQL_DRIVER').first()[0]
config_DF.unpersist()
print('[I] Variables Loaded from Config File {0}'.format(strConfigFilePath))


CSVSchema = StructType(fields=[
            StructField('Date',StringType(),True),
            StructField('series',StringType(),True),
            StructField('OPEN',DecimalType(10,2),True), 
            StructField('HIGH',DecimalType(10,2),True), 
            StructField('LOW',DecimalType(10,2),True), 
            StructField('PREV. CLOSE',DecimalType(10,2),True), 
            StructField('ltp',DecimalType(10,2),True), 
            StructField('close',DecimalType(10,2),True), 
            StructField('vwap',DecimalType(10,2),True), 
            StructField('52W H',DecimalType(10,2),True), 
            StructField('52W L',DecimalType(10,2),True), 
            StructField('VOLUME',DecimalType(38,0),True), 
            StructField('VALUE',DecimalType(38,2),True), 
            StructField('No of trades',DecimalType(38,0),True)])

# Process csv files and load into SQL tables
try:
    print('[I] {0} File Read Started'.format(varFileFormat))
    listCSVFiles = os.listdir(varRawFolderPath)
    for csvFile in listCSVFiles:
        reader_DF = spark.read.format(varFileFormat).option("header",True).schema(CSVSchema).csv(varRawFolderPath+csvFile)
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
                             .withColumn("file_received_datetime",lit(str(time.ctime(os.path.getmtime(varRawFolderPath+csvFile)))))
        final_reader_DF = reader_DF.select("trade_date","series","open_price","day_high","day_low","prev_day_close","ltp","closing_price","vwap","fifty_two_week_high","fifty_two_week_low","trade_volume","value","no_of_trades","file_name","inserted_time","file_received_datetime")
        final_reader_DF\
            .write\
            .mode("append")\
            .format("jdbc")\
            .option("url",strPGJdbcUrl)\
            .option("dbtable","stocks.nse_all_data")\
            .option("user",strPGUsername)\
            .option("password",strPGPassword)\
            .option("driver","org.postgresql.Driver")\
            .save()
finally:
    print('[I] File Processing Completed')
    spark.stop()
