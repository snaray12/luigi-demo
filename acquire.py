# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 13:26:42 2020

@author: sundararaman
"""
import luigi
import time
from datetime import datetime
import schedule

class HelloWorld(luigi.Task):
    name = luigi.Parameter()
    now =  luigi.Parameter()  
    
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget('helloworld_'+self.now+'.txt')
    def run(self):
        time.sleep(1)
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')
            outfile.write(self.name)
        time.sleep(1)

class NameSubstituter(luigi.Task):
    name = luigi.Parameter()
    now =  luigi.Parameter()   

    def requires(self):
        return HelloWorld("My Name", self.now)
    def output(self):
        return luigi.LocalTarget(self.input().path + '.name_' + self.name+self.now)
    def run(self):
        time.sleep(1)
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)
        time.sleep(1)

def job():
    print("I'm working...")
    #luigi.run()
    now = datetime.now() # time object
    dt_string = now.strftime('%d_%m_%Y_%H_%M_%S')
    luigi.build([NameSubstituter("Simba", dt_string)], workers=1, local_scheduler=True)
    #luigi.build([HelloWorld()], workers=1, local_scheduler=True)

schedule.every(10).seconds.do(job)


while True:
    schedule.run_pending()
    time.sleep(1)
    
    