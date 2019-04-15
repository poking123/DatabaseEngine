import pandas as pd
import numpy as np

def loadCSV(path):
    t0 = time.time()
    # tableName is the name of the file
    path2 = "../../data/m/A.csv"
    allRows = pd.read_csv(path2)

    allRows.to_csv("A-python.dat", index = False)

    # for row in np.arange(allRows.values.shape[0]):
    #     for i in allRows.iloc[row,:].values:
    #         print(i)
    t1 = time.time()

    print(t1 - t0)

loadCSV(" ");
    
