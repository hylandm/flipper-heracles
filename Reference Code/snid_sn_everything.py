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
MAX_RESULTS = 100

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

def getSN( flmfile, w=20 ):
    """
    Calculates the S/N ratio at the central wavelength, in a 
     bin +/-<w> angstroms wide.
    Will remove a polynomial fit before calculating noise.
    """
    d = np.loadtxt( flmfile )
    midwl = d[len(d)/2, 0]
    m = (d[:,0] > midwl-w) & (d[:,0] < midwl+w)
    # fit and subtract a line from noise
    p = np.poly1d( np.polyfit( d[:,0][m], d[:,1][m], 3 ) )
    std = np.std( d[:,1][m] - p(d[:,0][m]) )
    return np.abs(np.mean( d[:,1][m] ) / std)

def getBoth( flmfile ):
    try:
        snr = getSN( flmfile )
    except:
        snr = 0.0
    try:
        t,st = getSNID( flmfile )
        if t == '':
            t = 'NoMatch'
        if st == '':
            st = 'NoMatch'
    except:
        t,st = 'NoMatch','NoMatch'
    return t,st,snr

def insertIntoDB( t,st,snr,specID ):
    sql = 'UPDATE spectra SET SNID_Type="%s",SNID_Subtype="%s",SNR=%f WHERE SpecID=%d;' %(t,st,snr,specID)
    c = DB.cursor()
    print sql
    c.execute( sql )
    c.close()
    DB.commit()

def do_it_all( inn ):
    try:
        flmfile, specid = inn
        t,st,snr = getBoth( flmfile )
        insertIntoDB( t,st,snr,specid )
    except:
        open('failed_snid.txt','a').write('%s, %s\n'%(flmfile,specid))
        print 'failed on',flmfile
    

def runAllFlipperSpec( skipstrs=[], rootdir='/media/raid0/Data/spectra/', maxnum=None, files=None ):
    """
    Run SNID and calculate the SNR for all ASCII spectra in the rootdir and children.
     Any filename with any of the skipstrs in it will not be run.
    """
    c = DB.cursor()
    # first accumulate a list of all files to run in parallel
    if files == None:
        S = yield_all_spectra( rootdir )
    else:
        S = files
    fs = []
    for i,s in enumerate(S):
        # make sure that this spectrum is in the DB
        fname = path.basename( s[0] )
        #c.execute( 'SELECT SpecID,Filename FROM spectra WHERE (Filename = "%s" AND SNID_Type IS NULL);'%fname )
        c.execute( 'SELECT SpecID,Filename FROM spectra WHERE (Filename = "%s");'%fname )
        res = c.fetchone()
        if res == None:
            #open('snid_missing_DB.txt','a').write('%s\n'%s[0])
            continue
        print s[0]
        SpecID = res.get('SpecID')
        # don't include any files that match one of the skipstrs
        if not any( [1 if ss in fname else 0 for ss in skipstrs] ):
            fs.append( [s[0], SpecID] )
        if (maxnum!=None) and (len(fs)>=maxnum):
            break
    c.close()
    # now run all files in parallel
    print 'found {} spectra'.format(len(fs))
    
    # start the processor pool
    print 'starting SNID calculations...'
    pool = multiprocessing.Pool( multiprocessing.cpu_count() )
    pool.map( do_it_all, fs )

  
if __name__ == '__main__':
    runAllFlipperSpec()
    
    # run at the end, to catch files that failed due to DB overloading issues or something
    #allfiles = [f.strip().split(',') for f in open('failed_snid1.txt','r').readlines()]
    #runAllFlipperSpec( files = allfiles ) 
