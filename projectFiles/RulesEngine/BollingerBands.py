from datetime import datetime

class BollingerBands:
    """
    """
    def __init__(self):
        self.strRuleName = "BollingerBands"
    
    def getBollingerBands(self,dfSourceDataframe):
        """
        This method will provide the Bollinger Bands from a Dataframe.
        CreadtedBy: Debajyoti Saha
        CreatedOn: 03-Jan-2022
        """
        try:
            None
        except Exception as e:
            print("[E] <<<<---- getBollingerBands ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] Bollinger Bands Completed Successfully")
            else:
                print("[I] Bollinger Bands Completed with Error")