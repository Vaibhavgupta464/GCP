#Creating a python program on your Google Cloud Shell VM to generate a CSV file "sourcefile1.csv" with 10 columns and 10000 rows.

#importing libraries
import pandas as pd
import numpy as np

#defining function to pick random dates
def random_dates(start, end, n=10):

    start_u = start.value//10**9
    end_u = end.value//10**9

    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit='s')

#my_array will contains random number of shape 10000,5 after that storing that in dataframe  
my_array = np.random.randn(10000, 5)
df = pd.DataFrame(my_array, columns = ['Column_A','Column_B','Column_C','Column_D','Column_E'])

#my_array2 will contains boolean value
my_array2 = np.random.randint(0, 2, size=[10000,4], dtype=np.uint8).view(bool)
df2 = pd.DataFrame(my_array2, columns = ['Column_F','Column_G','Column_H','Column_I'])

#my_array3 contains random dates
start = pd.to_datetime('2015-01-01')
end = pd.to_datetime('2018-01-01')
my_array3 =random_dates(start, end, n = [10000]).to_numpy().reshape([15000, 1])
df3 = pd.DataFrame(my_array3, columns = ['Column_J'])


#merging all dataframes column wise
df4= pd.concat([df, df2, df3], axis=1)

print(df4.head(3))

#saving in local
df4.to_csv('workingfile/sourcefile1.csv',index=False)

