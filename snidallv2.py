from subprocess import Popen, PIPE
import multiprocessing
import re,pickle 
from glob import glob
from scriptsLib import yield_all_spectra, get_info_spec_fitsfile
import MySQLdb
import MySQLdb.cursors
from os import path
import credentials as creds
import numpy as np

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db='newsndb', cursorclass=MySQLdb.cursors.DictCursor)

def getSNID( flmfile ):
    """
    Run SNID on an ASCII spectrum,
     simply returning the best type as 
     determined by fraction and slope.
    """
    cmd = "snid plot=0 inter=0 {}".format(flmfile)
    o,e = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE).communicate()
    # if SNID found no matches at all, quit here
    if re.search('Thank you for using SNID! Goodbye.',o) == None:
        return 'NoMatch','NoMatch'

    typestring,subtypestring = o.split('Best subtype(s)')
    # find the best types first
    lines = typestring.split('\n')
    iii = lines.index(' [fraction]') +1
    ftype = lines[iii].split()[2]
    iii = lines.index(' [slope]') +1
    if 'NOTE' in lines[iii]:
        stype = ftype
    else:
        stype = lines[iii].split()[2]
    t = set([ftype,stype])
    t = ','.join(set([ftype,stype]))

    # now find best subtypes:
    lines = subtypestring.split('\n')[:10]
    iii = lines.index(' [fraction]') +1
    ftype = lines[iii].split()[3]
    iii = lines.index(' [slope]') +1
    if 'NOTE' in lines[iii]:
        stype = ftype
    else:
        stype = lines[iii].split()[3]
    st = set([ftype,stype])
    st = ','.join(set([ftype,stype]))
    return t, st

def runSNID( inn ):
    specID,flmfile = inn
    try:
        t,st = getSNID( '/media/raid0/'+flmfile )
        if t == '':
            t = 'NoMatch'
        if st == '':
            st = 'NoMatch'
    except:
        t,st = 'NoMatch','NoMatch'
    insertIntoDB( t, st, specID )

def insertIntoDB( t,st,specID ):
    sql = 'UPDATE spectra SET SNID_Type="%s",SNID_Subtype="%s" WHERE SpecID=%d;' %(t,st,snr,specID)
    c = DB.cursor()
    print sql
    c.execute( sql )
    c.close()
    DB.commit()

def getFilepath( specID ):
    root = '/media/raid0/'
    sql = 'SELECT Filepath,Filename from spectra WHERE SpecID=%d;' %(specID)
    c = DB.cursor()
    c.execute( sql )
    res = c.fetchone()
    c.close()
    return root+res['Filepath']+'/'+res['Filename']

def snidAll( ):
    """
    Run SNID and calculate the SNR for all spectra in the DB.
    """
    c = DB.cursor()
    # first accumulate a list of all files to run in parallel
    print 'querying DB for all objects'
    sql = 'Select Filepath,Filename,SpecID from spectra;'
    c.execute( sql )
    allfiles = []
    open('specErrors.txt','w').write('#SpecID : Filepath : Filename\n')
    while True:
        res = c.fetchone()
        if res == None:
            break
        try:
            allfiles.append( [res['SpecID'], res['Filepath']+'/'+res['Filename']] )
        except:
            # this spectrum is listed in the DB incorrectly
            print 'Failed:',res['SpecID']
            open('specErrors.txt','a').write( '%d : %s : %s\n'%(res['SpecID'], str(res['Filepath']), str(res['Filename'])) )
    c.close()
    # now run all files in parallel
    # start the processor pool
    print 'starting SNID calculations...'
    pool = multiprocessing.Pool( 3 )
    pool.map( runSNID, allfiles )

  
if __name__ == '__main__':
    snidAll()
