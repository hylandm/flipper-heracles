"""
Goes through all photometry files we can find and inserts them into
 the database.


The mysql definition of the phot table:

CREATE TABLE photometry
(
PhotID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
ObjID INT NOT NULL,
Filename VARCHAR(200) NOT NULL,
Filepath VARCHAR(200) NOT NULL,
Filters VARCHAR(200),
Telescopes VARCHAR(200),
FirstObs DATE,
LastObs DATE,
Reducer VARCHAR(50),
NPoints INT(4),
Notes TEXT,
Reference TEXT,
DateReduced DATE,
Public TINYINT(1) NOT NULL DEFAULT 0
);
"""
from glob import glob
import MySQLdb
import MySQLdb.cursors
import os
import credentials as creds
import re
import julian_dates as jd

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db='newsndb', cursorclass=MySQLdb.cursors.DictCursor)
c = DB.cursor()
oDB = MySQLdb.connect(host='classy.astro.berkeley.edu', user=creds.user, passwd=creds.passwd, db='sndb', cursorclass=MySQLdb.cursors.DictCursor)
oc = oDB.cursor()

def parse_photfile( f ):
    lines = [l for l in open(f,'r').readlines() if l[0]!='#']
    obsdates = [float(l.split()[0]) for l in lines]
    firstobs = min(obsdates)
    lastobs = max(obsdates)
    firstobs = jd.caldate( firstobs ) # tuple of (y,m,d,h,m,s)
    lastobs = jd.caldate( lastobs )
    filters = set([l.split()[4] for l in lines])
    filters = ','.join(filters)
    telescopes = set([l.split()[5] for l in lines])
    telescopes = ','.join(telescopes)
    npoints = len(lines)
    return (firstobs, lastobs, filters, telescopes, npoints)

def insert_photfile( f ):
    fpath,fname = os.path.split( f )
    # trim the fpath relative to the Data directory
    fpath = fpath.split('raid0/')[1]
    # find the object row
    objname = fname.split('.')[0]
    if objname[:2] == 'sn':
        objname = objname[2:]
    # have it match against lower or upper case if it's a single letter
    if len(objname) == 5:
        objname = objname[:-1] + '[%s%s]$'%(objname[-1].lower(),objname[-1].upper())
    # see if there's a related file in the old DB
    cmd = "SELECT t1.* FROM photometry as t1, objects as t2 WHERE ((t1.ObjID = t2.ObjID) AND (t2.ObjName REGEXP '%s'));" %objname
    print cmd
    oc.execute(cmd)
    res = oc.fetchone()
    if res != None:
        # this object is in old database; pull reference info if it exists
        ref = res['Reference']
        if ref == None:
            ref = 'NULL'
    else:
        ref = 'NULL'
    if ref == 'NULL':
        # try everything again from the public objects table
        cmd = "SELECT t1.* FROM photometry_public as t1, objects_public as t2 WHERE ((t1.ObjID = t2.ObjID) AND (t2.ObjName REGEXP '%s'));" %objname
        print cmd
        oc.execute(cmd)
        res = oc.fetchone()
        if res != None:
            # this object is in old database; pull reference info if it exists
            ref = res['Reference']
            if ref == None:
                ref = 'NULL'
        else:
            ref = 'NULL'

    cmd = "SELECT * from objects WHERE (ObjName REGEXP '%s');"%objname
    c.execute(cmd)
    res = c.fetchone()
    if res != None:
        objid = res['ObjID']
    else:
        if fname == 'ptf12gzk.dat':
            objid = 6353
        else:
            # need to add object first!
            open('missing_object.txt','a').write('%s\n' %f)
            print f,'has no object entry.'
            return
    reducer = 'NULL'
    notes = 'NULL'
    datereduced = 'NULL'
    if 'public' in fname:
        pub = 1
    else:
        pub = 0
    # pull other info out of file itself
    firstobs, lastobs, filters, telescopes, npoints = parse_photfile( f )
    firstobs = '%d-%d-%d'%(firstobs[0],firstobs[1],firstobs[2])
    lastobs = '%d-%d-%d'%(lastobs[0],lastobs[1],lastobs[2])
    cmd = "INSERT INTO photometry (ObjID, Filename, Filepath, Filters, Telescopes, FirstObs, LastObs, Reducer, NPoints, Notes, Reference, DateReduced, Public) \n"+\
          "VALUES (%d, '%s', '%s', '%s', '%s', DATE('%s'), DATE('%s'), '%s', %d, '%s', '%s', DATE('%s'), %d);"
    cmd = cmd %(objid, fname, fpath, filters, telescopes, firstobs, lastobs, reducer, npoints, notes, ref, datereduced, pub)
    print cmd
    c.execute(cmd)
    DB.commit()
    #raw_input()
    return


for f in glob( '/media/raid0/Data/photometry/*.dat' ):
    try:
        insert_photfile( f )
    except:
        open('failed_phot_files.txt','a').write('%s\n'%f)

    
