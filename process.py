import json
import Connectors
import Transformations
import AutoML
try:
    Milkproduction_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e9d7cdbe3127dd4c3d8930a", spark, "{'url': '/Demo/Forcasting_Milk_Prod.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib8073bbfa952efa9d363b234ce06e2c6', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    Milkproduction_AutoFE = Transformations.TransformationMain.run(["5e9d7cdbe3127dd4c3d8930a"], {"5e9d7cdbe3127dd4c3d8930a": Milkproduction_DBFS}, "5e9d7cdbe3127dd4c3d8930b", spark, json.dumps({"FE": [{"transformationsData": {}, "feature": "Year_Month", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "144", "mean": "2004.07", "stddev": "5.6", "min": "1995.05", "max": "2013.12", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Month_Number", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "144", "mean": "6.45", "stddev": "3.49", "min": "1", "max": "12", "missing": "0"}}, {"transformationsData": {}, "feature": "Year", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "144", "mean": "2004.01", "stddev": "5.61", "min": "1995", "max": "2013", "missing": "0"}}, {"transformationsData": {"feature_label": "Month"}, "feature": "Month", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "144", "mean": "", "stddev": "", "min": "Apr", "max": "Sept", "missing": "0"}, "transformation": "String Indexer"}, {
                                                                   "transformationsData": {}, "feature": "Cotagecheese_Prod", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "144", "mean": "2.84", "stddev": "0.51", "min": "1.865", "max": "4.47", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Icecream_Prod", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "144", "mean": "71.13", "stddev": "11.2", "min": "47.127", "max": "93.01", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Milk_Prod", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "144", "mean": "2.94", "stddev": "0.48", "min": "2.028", "max": "3.744", "missing": "0"}, "transformation": ""}, {"transformationsData": {}, "feature": "Fat_Price", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "144", "mean": "1.56", "stddev": "0.41", "min": "0.8934", "max": "2.5285", "missing": "0"}, "transformation": ""}, {"feature": "Month_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "144", "mean": "5.02", "stddev": "3.48", "min": "0.0", "max": "13.0", "missing": "0"}}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionRegression(Milkproduction_AutoFE, [
                              "Year_Month", "Month_Number", "Year", "Month", "Cotagecheese_Prod", "Icecream_Prod", "Milk_Prod"], "Fat_Price")

except Exception as ex:
    print(ex)
