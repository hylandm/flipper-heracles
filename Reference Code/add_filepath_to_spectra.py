"""
Goes through all spectra files and inserts the full path name
 to that file into the relevant database entry.

-Isaac, May 5
"""

from scriptsLib import yield_all_spectra
from glob import glob
import MySQLdb
import MySQLdb.cursors
import os
import credentials as creds
import re
import julian_dates as jd

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db='newsndb', cursorclass=MySQLdb.cursors.DictCursor)
c = DB.cursor()

s = yield_all_spectra(location='/media/raid0/Data/spectra/')
for entry in s:
    fflm,ffits = entry
    print 'adding',fflm
    fpath, fname = os.path.split( fflm )
    fpath = fpath.split('raid0/')[1]
    sql = "UPDATE spectra SET Filepath='%s' WHERE Filename='%s';"%(fpath, fname)
    print sql
    print c.execute( sql )
    DB.commit()
    #raw_input()  #for testing purposes
