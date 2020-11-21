# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 19:09:39 2020

@author: sundararaman
"""

import luigi
import os
import pandas as pd
import numpy as np
import datetime

class SourceDataReader(luigi.ExternalTask):
    datadir = luigi.Parameter()
    filename = luigi.Parameter()
    
    def requires(self):
        return None
    
    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.datadir, "processed", self.filename))

    
    def complete(self):
        return True
    
class DataCleanerTask(luigi.Task):
    datadir = luigi.Parameter()
    filename = luigi.Parameter()
    
    def requires(self):
        return SourceDataReader(self.datadir, self.filename)
    
    def output(self):
        return luigi.LocalTarget(path=os.path.join(self.datadir, "cleaned", self.filename))
    
    def run(self):
        df = pd.read_csv(self.input().path)
        df.dropna().to_csv(self.output().path, line_terminator='\n', index=None)

class TransformData(luigi.Task):
    filename = luigi.Parameter()
    datadir = luigi.Parameter()
    
    def requires(self):
        return DataCleanerTask(datadir = self.datadir, filename = self.filename)
        
    def output(self):
        return luigi.LocalTarget(path = os.path.join(self.datadir, "transformed", 
                                                     self.filename))
    
    def get_signal(self, price_signal, vol_signal):
        POSITIVE = 'Positive'
        NEGATIVE  ='Negative'
        if price_signal == vol_signal == POSITIVE:
            return POSITIVE
        elif price_signal == vol_signal == NEGATIVE:
            return NEGATIVE
        elif price_signal == POSITIVE and vol_signal == NEGATIVE:
            return POSITIVE
        elif price_signal == NEGATIVE and vol_signal == POSITIVE:
            return NEGATIVE
        
    def run(self):
        now  = datetime.datetime.now()
        df = pd.read_csv(self.input().path)
        df1 = df.dropna()
        df1['Date'] = pd.to_datetime(df1['Date'])
        df1['Month'] = df1['Date'].dt.month
        df1['Year'] = df1['Date'].dt.year
        summarydf = df1.groupby(['Year', 'Month']).agg({'Close':[min, max, np.mean], 
                                                        'Volume':[min, max, np.mean]})
        summarydf.columns = summarydf.columns.droplevel()
        summarydf.reset_index(inplace=True)
        summarydf.columns = ["Year", "Month","Close_Min", 
                             "Close_Max", "Close_Mean", 
                             "Volume_Min", "Volume_Max", "Volume_Mean"]
        summarydf['days_hence_max'] = (now - df1.loc[df1.groupby(['Year', 'Month'])['Close'].idxmax()]['Date']).dt.days.tolist()

        summarydf['days_hence_min'] = (now - df1.loc[df1.groupby(['Year', 'Month'])['Close'].idxmin()]['Date']).dt.days.tolist()
        
        summarydf['month_signal'] = summarydf.apply(lambda x : 'Positive' if x['days_hence_max'] < x['days_hence_min'] else 'Negative', axis = 1) 
        
        summarydf['mvol_days_hence_max'] = (now - df1.loc[df1.groupby(['Year', 'Month'])['Volume'].idxmax()]['Date']).dt.days.tolist()
        
        summarydf['mvol_days_hence_min'] = (now - df1.loc[df1.groupby(['Year', 'Month'])['Volume'].idxmin()]['Date']).dt.days.tolist()
        
        summarydf['vol_month_signal'] = summarydf.apply(lambda x : 'Positive' if x['mvol_days_hence_max'] < x['mvol_days_hence_min'] else 'Negative', axis = 1) 

        
        summarydf2 = df1.groupby(['Year']).agg({'Close':[min, max, np.mean], 
                                                        'Volume':[min, max, np.mean]})
        summarydf2.columns = summarydf2.columns.droplevel()
        summarydf2.reset_index(inplace=True)
        summarydf2.columns = ["Year","Close_Min", 
                             "Close_Max", "Close_Mean", 
                             "Volume_Min", "Volume_Max", "Volume_Mean"]
        
        summarydf2['days_hence_max'] = (now - df1.loc[df1.groupby(['Year'])['Close'].idxmax()]['Date']).dt.days.tolist()
        summarydf2['days_hence_min'] = (now - df1.loc[df1.groupby(['Year'])['Close'].idxmin()]['Date']).dt.days.tolist()
        summarydf2['year_signal'] = summarydf2.apply(lambda x : 'Positive' if x['days_hence_max'] < x['days_hence_min'] else 'Negative', axis = 1) 
        
        summarydf2['yvol_days_hence_max'] = (now - df1.loc[df1.groupby(['Year'])['Volume'].idxmax()]['Date']).dt.days.tolist()
        
        summarydf2['yvol_days_hence_min'] = (now - df1.loc[df1.groupby(['Year'])['Volume'].idxmin()]['Date']).dt.days.tolist()
        
        summarydf2['vol_year_signal'] = summarydf2.apply(lambda x : 'Positive' if x['yvol_days_hence_max'] < x['yvol_days_hence_min'] else 'Negative', axis = 1) 


        fulldf = pd.merge(summarydf, summarydf2, on="Year")
        fulldf['signal'] = fulldf.apply(lambda x : self.get_signal(x['month_signal'], x['vol_month_signal']), axis=1)
        fulldf['vol_signal'] = fulldf.apply(lambda x : self.get_signal(x['year_signal'], x['vol_year_signal']), axis=1)
        fulldf['csignal'] = fulldf['signal'] + '-'+fulldf['vol_signal']
        
        with self.output().open("w") as outfile:
            fulldf.to_csv(outfile, line_terminator='\n', index=None)
            
class TaskManager(luigi.WrapperTask):
    datadir = luigi.Parameter()
    
    def requires(self):
        for root, dirnames, filenames in os.walk(self.datadir):
            for fn in filenames:
                if fn.endswith(".csv") and ("_" not in fn):                    
                    yield TransformData(filename = fn,
                                 datadir = self.datadir)

        
if __name__ == "__main__":
    datadir = "C:\\Users\\sundararaman\\Documents\\personal\\stk\\data"
    '''
    luigi.build([TransformData(
        filename="ITC.NS.csv", 
                             datadir=datadir)], 
                workers=1, 
                local_scheduler=True)
    
    ############
    #inputfilelist = ['ITC.NS.csv', 'SBIN.NS.csv']
    for root, dirnames, filenames in os.walk(datadir):
        for fn in filenames:
            if fn.endswith(".csv"):                    
                luigi.build([TransformData(filename = fn,
                             datadir = datadir)],
                            workers = 2,
                            local_scheduler = False)
    '''
    luigi.build([TaskManager(datadir = datadir)],
                workers = 2,
                local_scheduler=True)
    
        