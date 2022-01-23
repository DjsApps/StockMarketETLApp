from datetime import datetime
from pyspark.sql.functions import col, avg

class MovingAverage:
    """
    """
    def __init__(self):
        self.strRuleName = "MovingAverage"
    
    def getMovingAverage(self,dfSourceDataframe, intMovingAvgrequired):
        """
        This method will provide the Simple Moving Average from a Dataframe.
        CreadtedBy: Debajyoti Saha
        CreatedOn: 03-Jan-2022
        """
        try:
            intSimpleMovingAverage = mean(dfSourceDataframe["closing_price"])
            return intSimpleMovingAverage
        except Exception as e:
            print("[E] <<<<---- getMovingAverage ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] Moving Average Completed Successfully")
            else:
                print("[I] Moving Average Completed with Error")

    def getExponentialMovingAverage(self,dfSourceDataframe, intMovingAvgrequired):
        """
        This method will provide the Exponential Moving Average from a Dataframe.
        CreadtedBy: Debajyoti Saha
        CreatedOn: 10-Jan-2022
        """
        try:
            None
        except Exception as e:
            print("[E] <<<<---- getExponentialMovingAverage ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] Exponential Moving Average Completed Successfully")
            else:
                print("[I] Exponential Moving Average Completed with Error")
    
    def CalculateMovingAverage(self,dfSourceDataframe, intMovingAvgrequired):
        """
        This method will provide the Exponential Moving Average from a Dataframe.
        CreadtedBy: Debajyoti Saha
        CreatedOn: 10-Jan-2022
        """
        try:
            None
        except Exception as e:
            print("[E] <<<<---- CalculateMovingAverage ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] Calculate Moving Average Completed Successfully")
            else:
                print("[I] Calculate Moving Average Completed with Error")