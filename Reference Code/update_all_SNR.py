"""
Goes through every spectrum in the DB and
re-calculates the SNR for it.

"""

from scriptsLib import yield_all_spectra,getSNR
from os import path
import MySQLdb
import MySQLdb.cursors
import credentials as creds

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db='newsndb', cursorclass=MySQLdb.cursors.DictCursor)

c = DB.cursor()
S = yield_all_spectra( '/media/raid0/Data/spectra/' )
for i,s in enumerate(S):
    try:
        # make sure that this spectrum is in the DB
        fname = path.basename( s[0] )
        c.execute( 'SELECT SpecID,Filename FROM spectra WHERE (Filename = "%s");'%fname )
        res = c.fetchone()
        if res == None:
            # just roll on if it's not in the DB
            continue
        print i,':::',s[0]
        SpecID = res.get('SpecID')
        # now actually get the SNR
        snr = getSNR( s[0] )
        # and now insert it into the DB
        sql = 'UPDATE spectra SET SNR=%f WHERE SpecID=%d;' %(snr,SpecID)
        print sql
        c.execute( sql )
        DB.commit()
    except:
        print 'failed on',s[0]
