# -*- coding: utf-8 -*-
"""
Created on Fri Nov 20 21:24:15 2020

@author: sundararaman
"""
import os
import pandas as pd
import numpy as np

datadir = "C:\\Users\\Documents\\personal\\stk\\data"

for root, dirnames, filenames in os.walk(datadir):
    for fn in filenames:
        if (fn.endswith(".csv")) and ("_" not in fn):
            #
            fn1 = fn.replace(".csv", "_1.csv")
            print(fn," - ", fn1)
            df1 = pd.read_csv(os.path.join(datadir, fn))
            df2 = pd.read_csv(os.path.join(datadir, fn1))
            df = pd.concat([df1, df2]).drop_duplicates()
            print(df1.shape[0]+df2.shape[0]-1 == df.shape[0])
            df.to_csv(os.path.join(datadir, "processed", fn), index=None)
