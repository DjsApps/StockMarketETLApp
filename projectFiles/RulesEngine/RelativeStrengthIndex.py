from datetime import datetime

class RelativeStrengthIndex:
    """
    """
    def __init__(self):
        self.strRuleName = "RelativeStrengthIndex"

    def getRelativeStrengthIndex(self,dfSourceDataframe, intMovingAvgrequired):
        """
        This method will provide the Relative Strength Index from a Dataframe.
        CreadtedBy: Debajyoti Saha
        CreatedOn: 03-Jan-2022
        """
        try:
            None
        except Exception as e:
            print("[E] <<<<---- getRelativeStrengthIndex ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] RSI Completed Successfully")
            else:
                print("[I] RSI Completed with Error")