from datetime import datetime

class MACD:
    """
    """
    def __init__(self):
        self.strRuleName = "MACD"

    def getMACD(self,dfSourceDataframe, intMovingAvgrequired):
        """
        This method will provide the MACD from a Dataframe.
        CreadtedBy: Debajyoti Saha
        CreatedOn: 03-Jan-2022
        """
        try:
            None
        except Exception as e:
            print("[E] <<<<---- getMACD ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] MACD Completed Successfully")
            else:
                print("[I] MACD Completed with Error")