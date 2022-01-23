from datetime import datetime

class FibonacciRetracement:
    def __init__(self, strRuleName):
        self.strRuleName = "FibonacciRetracement"

    def getFibonacciRetracement(self,flStartingValue,flEndingValue):
        """
        This Method will calculate the Fibonacci Retracement level
        CreatedBy: Debajyoti Saha
        CreatedOn: 22-Dec-2021
        Formula: (flStartingValue - ((flStartingValue-flEndingValue)*fibonacciPercentage))
        """
        try:
            strStartTime = str(datetime.now())
            print("[I] Fibonacci Retracement Calculation Started at {0}".format(strStartTime))
            v_23_6 = (flStartingValue - ((flStartingValue-flEndingValue) * 0.236))
            v_32_8 = (flStartingValue - ((flStartingValue-flEndingValue) * 0.328))
            v_50 = (flStartingValue - ((flStartingValue-flEndingValue) * 0.5))
            v_61_8 = (flStartingValue - ((flStartingValue-flEndingValue) * 0.618))
            v_78_6 = (flStartingValue - ((flStartingValue-flEndingValue) * 0.786))
            v_status = "Success"
            return v_23_6, v_32_8, v_50, v_61_8, v_78_6
        except Exception as e:
            print("[E] <<<<---- getFibonacciRetracement ---->>>>\n")
            print(str(e))
            v_status = "Error"
        finally:
            if (v_status == "Success"):
                print("[I] Fibonacci Retracement Completed Successfully")
            else:
                print("[I] Fibonacci Retracement Completed with Error")