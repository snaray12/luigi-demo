# -*- coding: utf-8 -*-
"""
Created on Sat Nov 14 13:26:42 2020

@author: sundararaman
"""
import luigi
import time

class HelloWorld(luigi.Task):
    name = luigi.Parameter()
    
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget('helloworld.txt')
    def run(self):
        time.sleep(15)
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')
            outfile.write(self.name)
        time.sleep(15)

class NameSubstituter(luigi.Task):
    name = luigi.Parameter()

    def requires(self):
        return HelloWorld("My Name")
    def output(self):
        return luigi.LocalTarget(self.input().path + '.name_' + self.name)
    def run(self):
        time.sleep(15)
        with self.input().open() as infile, self.output().open('w') as outfile:
            text = infile.read()
            text = text.replace('World', self.name)
            outfile.write(text)
        time.sleep(15)

if __name__ == '__main__':
    #luigi.run()
    luigi.build([NameSubstituter("Simba")], workers=1, local_scheduler=False)
    #luigi.build([HelloWorld()], workers=1, local_scheduler=True)
    