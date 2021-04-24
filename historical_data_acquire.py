# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 14:12:51 2020

@author: sundararaman
"""

import luigi
import os
import requests
#from bs4 import BeautifulSoup
#import util
#import csv
import time

DATA_DIR = 'data'
INPUT_FILE = "stklist.txt"
PREFIX_URL = ""
SUFFIX_URL = "?period1=820454400&period2=1605830400&interval=1d&events=history&includeAdjustedClose=true"

class FetchHistoricalData(luigi.Task):
    
    file_name = luigi.Parameter(default = INPUT_FILE)
    data_dir = luigi.Parameter(default = DATA_DIR)
    #outfile = luigi.Parameter(default = "res.csv")
    
    def require(self):
        return None
    
    def output(self):
        return luigi.LocalTarget('C:/Users/Documents/personal/stk/data/res.csv')
    
    def run(self):
        with open(os.path.join(self.data_dir, self.file_name), "r") as f:
            lines = f.readlines()
            for line in lines:
              #self.outfile = line
              outfile = line.replace("\n", "")
              url = PREFIX_URL + outfile + SUFFIX_URL
              print(url)
              res = requests.get(url)
              with open(os.path.join(self.data_dir, "processed", outfile+".csv"), "w") as out:
                  out.write(str(res.text))
              time.sleep(30)
              

if __name__ == "__main__":
    luigi.build([FetchHistoricalData("newstklist.txt", "C:\\Users\\Documents\\personal\\stk\\data")], workers=1, local_scheduler=True)
