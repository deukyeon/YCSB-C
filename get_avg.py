import sys
import pandas as pd
import numpy as np

groupby_key = {
    'ycsb': ['threads'],
    'tpcc': ['threads', 'num_wh'],
}

try:
    csvfile = sys.argv[1]
    bench = 'ycsb'
    if len(sys.argv) > 1:
        bench = sys.argv[2]
        df = pd.read_csv(csvfile)
        df = df.select_dtypes(include=np.number)
        df = df.groupby(by=groupby_key[bench]).agg('mean')
        df.drop('seq', axis=1, inplace=True)
        print(df)
except Exception as e:
    print(e)
    print(f'Usage: python {sys.argv[0]} csvfile ycsb/tpcc')
