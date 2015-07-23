import jdcal
import numpy as np
import re
from dateutil import parser
import MySQLdb
import credentials as creds

# parse all files, and put them in format dict['XXXXyy'] = MJD
def parse_jha( fname ):
    outd = {}
    for l in open(fname,'r').readlines()[1:]:
        l = l.split()
        outd[l[0].strip()] = float( l[1].replace(',','') ) - 2400000.5
    return outd

def parse_rest( fname ):
    outd = {}
    lines = [l for l in open(fname,'r').readlines() if l[0]!='#']
    for l in lines:
        l = l.split()
        outd[l[0].strip()] = float( l[3].split('(')[0] )
    return outd

def parse_hicken( fname ):
    outd = {}
    lines = [l for l in open(fname,'r').readlines() if l[0]!='#']
    for l in lines:
        l = l.split()
        name = l[0].strip()[2:]
        if name[0] == '0':
            name = '20'+name
        else:
            name = '19'+name
        outd[name] = float( l[1] )
    return outd

def parse_betoule( fname ):
    lines = [l for l in open(fname,'r').readlines() if l[0]!='#']
    outd = {}
    for l in lines:
        l = l.split('|')
        name = l[0].strip()
        if re.search('sn\d{4}.+', name):
            name = name[2:]
        val = float(l[1])
        outd[name] = val
    return outd

def parse_folatelli( fname ):
    lines = [l for l in open(fname,'r').readlines() if l[0]!='#']
    outd = {}
    for l in lines:
        l = l.split('|')
        if l[1] != 'B':
            continue
        name = l[0].strip()
        val = float(l[2]) + 2453000.0 - 2400000.5
        outd[name] = val
    return outd

def parse_hyland( fname ):
    lines = [l for l in open(fname,'r').readlines() if l[0]!='#']
    outd = {}
    for l in lines:
        l = l.split('\t')
        name = l[0].split()[1]
        val = parser.parse( l[1] )
        val = jdcal.gcal2jd( val.year, val.month, val.day )[1]
        outd[name] = val
    return outd

# now go through the files in order and get the dates we want in string format
dates = parse_jha( 'data/jha+06.txt' )
dates.update( parse_hicken( 'data/hicken+2009.txt') )
dates.update( parse_folatelli( 'data/folatelli+2010.tsv') )
dates.update( parse_betoule( 'data/betoule+2014.tsv') )
dates.update( parse_rest( 'data/rest+14.txt') )

# testing to make sure I'm not missing any that Michael put in
# hdates = parse_hyland( 'data/hyland.peakdates.mql' )
# for k in hdates.keys():
#     try:
#         print hdates[k], ':::', dates[k]
#     except KeyError:
#         print hdates[k], ':::', dates[k.lower()]
# NOPE.  I got them all.

# now actually put them all into the DB
# Note that the DB produces no error if you attempt to update
#  a non-extant set, so I can just go through all of them
#  and not worry about those that aren't in the DB

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db='newsndb')
c = DB.cursor()
for sn in dates.keys():
    if re.search('\d{4}.$', sn):
        name = 'SN '+sn.upper()
    elif re.search('\d{4}..', sn):
        name = 'SN '+sn.lower()
    else:
        name = sn
    date = jdcal.jd2gcal( jdcal.MJD_0, dates[sn] )
    datestr = '%d-%d-%d'%(date[0], date[1], date[2])
    sql = 'UPDATE objects SET PeakDate=DATE("%s") WHERE ObjName="%s";' %(datestr, name)
    print sql
    c.execute(sql)
c.close()
DB.commit()

    