"""
Inserts the UT date (parsed from the filename) for all spectra in the SNDB which don't already
 have it.
Run on May 12, 2015

-Isaac
"""
import MySQLdb
import MySQLdb.cursors
import credentials as creds
import re

def parse_filename( f ):
    """
    Parses a *.flm file for observation date and object name.
    Returns (year, month, day), object_name
    """
    datestring = re.search('\d{8}(\.\d+)?', f).group()
    return datestring

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db='newsndb', cursorclass=MySQLdb.cursors.DictCursor)

sqlfind = 'SELECT SpecID,Filename,UT_Date FROM spectra WHERE UT_Date IS NULL LIMIT 1;'
c = DB.cursor()
while True:
    c.execute( sqlfind )
    res = c.fetchone()
    if res == None:
        print 'all done'
        break
    datenum = float( parse_filename(res['Filename']) )
    sqlupdate = 'UPDATE spectra SET UT_Date=%f WHERE SpecID=%d;'%(datenum,int(res['SpecID']))
    print res['Filename'],':::',sqlupdate
    c.execute( sqlupdate )
    DB.commit()
