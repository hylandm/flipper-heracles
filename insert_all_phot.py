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
Filters VARCHAR(10),
Telescopes VARCHAR(200),
FirstObs DATE,
LastObs DATE,
Reducer VARCHAR(50),
NPoints INT(4),
Notes TEXT,
Reference TEXT,
DateReduced DATE,
Public TINYINT NOT NULL
);
"""
import MySQLdb
import os
import credentials as creds
import re
import julian_dates as jd

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db='newsndb')
c = DB.cursor()

def parse_photfile( f ):
    lines = [l if l[0]!='#' for l in open(f,'r').readlines()]
    obsdates = [float(l.split()[0]) for l in lines]
    firstobs = min(obsdates)
    lastobs = max(obsdates)
    firstobs = jd.caldate( firstobs ) # tuple of (y,m,d,h,m,s)
    lastobs = jd.caldate( lastobs )
    filters = set(l.split()[4] for l in lines]
    filters = ','.join(filters)
    telescopes = set(l.split()[5] for l in lines]
    telescopes = ','.join(telescopes)
    npoints = len(lines)
    return (firstobs, lastobs, filters, telescopes, npoints)

def insert_photfile( f ):
    fpath,fname = os.path.split( f )
    # trim the fpath relative to the Data directory
    fpath = fpath.split('raid0/')[1]
    # find the object
    objname = fname.split('.')[0].replace('sn','SN ')
    # see if there's a related file in the old DB
    cmd = "SELECT * FROM old_photometry as t1, objects as t2 WHERE ((t1.ObjID = t2.ObjID) AND (t2.ObjName = '%s'));" %objname
    print cmd
    c.execute(cmd)
    res = c.fetchone()
    if res != None:
        # this object is in old database; pull reference info if it exists
        ref = res['Reference']
    else:
        ref = 'NULL'
    # get object ID if you can
    cmd = "SELECT * from objects WHERE (ObjName = %s);"%objname
    c.execute(cmd)
    res = c.fetchone()
    if res != None:
        objid = res['ObjID']
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
    firstobs, lasobs, filters, telescopes, npoints = parse_photfile( f )    
    cmd = "INSERT INTO photometry (ObjID, Filename, Filepath, Filters, Telescopes, FirstObs, LastObs, Reducer, NPoints, Notes, Reference, DateReduced, Public) \n"+\
          "VALUES (%d, '%s', '%s', '%s', '%s', DATE('%s'), DATE('%s'), '%s', %d, '%s', '%s', DATE('%s'), %d);"
    cmd = cmd %(objid, fname, fpath, filters, telescopes, firstobs, lastobs, reduceer, npoints, notes, ref, datereduced, pub)
    print cmd
    c.execute(cmd)
    DB.commit()
    raw_input()
    return


for f in glob( '/media/raid0/Data/photometry/*.dat' ):
    insert_photfile( f )
    #open('failed_files.txt','a').write('%s\n'%f)

    
