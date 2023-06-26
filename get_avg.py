import sys
import pandas as pd

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
        df = df.groupby(by=groupby_key[bench]).agg('mean')
        print(df)
except:
    print(f'Usage: python {sys.argv[0]} csvfile ycsb/tpcc')
