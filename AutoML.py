from sklearn.model_selection import train_test_split
from tpot import TPOTRegressor
import pyspark


def functionRegression(sparkDF, listOfFeatures, label):
    sparkDF.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    df = sparkDF.toPandas()
    df.columns.intersection(listOfFeatures)
    X = df.drop(label, axis=1).values
    y = df[label].values
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, random_state=1, test_size=0.2)
    tpotModel = TPOTRegressor(verbosity=3, generations=10, max_time_mins=15,
                              n_jobs=-1, random_state=25, population_size=15)
    tpotModel.fit(X_train, y_train)
    print(tpotModel.score(X_test, y_test))
