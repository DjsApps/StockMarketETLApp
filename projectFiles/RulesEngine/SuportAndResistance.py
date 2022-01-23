from datetime import datetime

class SuportAndResistance:
    """
    """
    def __init__(self):
        self.strRuleName = "SuportAndResistance"

    def getSupportLevel(self,dfSourceDataframe):
        """
        Get the Suppot Level for a given dataframe
        CreatedBy: Debajyoti Saha
        CreatedOn: 2022-01-10
        """
        try:
            None
        except Exception as e:
            print("[E] <<<<---- getSupportLevel ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] Support Level Completed Successfully")
            else:
                print("[I] Support Level Completed with Error")

    def getResistanceLevel(self,dfSourceDataframe):
        """
        Get the Resistance Level for a given dataframe
        CreatedBy: Debajyoti Saha
        CreatedOn: 2022-01-10
        """
        try:
            None
        except Exception as e:
            print("[E] <<<<---- getResistanceLevel ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] Resistace Level Completed Successfully")
            else:
                print("[I] Resistace Level Completed with Error")