from pyspark.sql.types import StructType, StructField, ArrayType, DateType, StringType, DecimalType

class StockMarketCsv:
    """
    # StockMarketCsv Class is used to define the Schema of a csv files
    # CreatedBy: Debajyoti Saha
    # CreatedOn: 11-DEC-2021
    """
    def __init__(self):
        self.strName = 'StockMarketCsv'
    
    def RawDataSchema(self):
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
        return CSVSchema

    def niftyIndexRawDataSchema(self):
        CSVSchema = StructType(fields=[
            StructField('SYMBOL', StringType(), True),
            StructField('OPEN',DecimalType(10,2),True),
            StructField('HIGH',DecimalType(10,2),True),
            StructField('LOW',DecimalType(10,2),True),
            StructField('PREV. CLOSE',DecimalType(10,2),True),
            StructField('LTP',DecimalType(10,2),True),
            StructField('CHNG',DecimalType(5,2),True),
            StructField('%CHNG',DecimalType(4,2),True),
            StructField('VOLUME',DecimalType(),True),
            StructField('VALUE',DecimalType(20,2),True),
            StructField('52W H',DecimalType(10,2),True),
            StructField('52W L',DecimalType(10,2),True),
            StructField('365 D % CHNG',DecimalType(10,2),True),
            StructField('30 D % CHNG',DecimalType(10,2),True)])
        return CSVSchema

    def shareHoldingPatternSchema(self):
        CSVSchema = StructType(fields=[
            StructField("COMPANY",StringType(), True),
            StructField("PROMOTER & PROMOTER GROUP (A)",DecimalType(10,2), True),
            StructField("PUBLIC (B)",DecimalType(10,2),True),
            StructField("SHARES UNDERLYING DRS (C1)",DecimalType(10,2),True),
            StructField("SHARES HELD BY EMPLOYEE TRUSTS (C2)", DecimalType(10,2),True),
            StructField("AS ON DATE",StringType(),True),
            StructField("ACTION",StringType(),True)
        ])
        return CSVSchema