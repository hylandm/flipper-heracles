"""
Adding new files/objects/spectralruns to the database!


PLAN:

1) a folder is edited, and into it was placed a .flm file with associated .fits
2) that change is noted, and that night the update command is run
3) update runs yield_all_spectra on the folder
4) first handle the objects: a function that takes in the flm,fits,folderpath and 
   either creates a new object and gives the objid or just gives the already-extant obsid
5) second handle the spectralrun: a function that takes in the flm,fits,folderpath and 
   either creates a new spectralrun row and gives the runid or just gives the already-extant runid
6) third actually pull all the info out of the flm,fits files and construct the
   insert command.
"""

import MySQLdb
import MySQLdb.cursors
import credentials as creds
import re
import os
from scriptsLib import *

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db=creds.db, cursorclass=MySQLdb.cursors.DictCursor)

def print_sql( sql, vals=None ):
    """
    Prints an approximation of the sql string with vals inserted. Approximates cursor.execute( sql, vals ).
    Note that internal quotes on strings will not be correct and these commands probably cannot actually be
    run on MySQL - for debuggin only!
    """
    if vals == None:
        print sql
    else:
        s = sql %tuple( map(str, vals) )
        print s

def handle_object_phot( objname ):
    """
    does the best it can to add object info based only upon a name.
    """
    is_sn = '[sS][nN]\d{4}.+'
    if re.search( folder, is_sn ):
        objname = folder.replace('sn','SN ')
    else:
        objname = folder
    # see if object already exists
    sqlfind = 'SELECT ObjID FROM objects WHERE ObjName = %s;'
    c = DB.cursor()
    c.execute( sqlfind, [objname] )
    res = c.fetchone()
    if res:
        # object already exists!
        return res['ObjID']
    else:
    
def handle_object( objname ):
    """
    does the best it can to add object info based only upon a name. objname should be as you want it to be in the SNDB.
    """
    # see if object already exists
    sqlfind = 'SELECT ObjID FROM objects WHERE ObjName = %s;'
    c = DB.cursor()
    c.execute( sqlfind, [objname] )
    res = c.fetchone()
    if res:
        # object already exists!
        return res['ObjID']
    else:
        # need to parse the input files to define the object
        info = get_info_spec_fitsfile( fitsfile )
        sqlinsert = "INSERT INTO objects (ObjName, RA, Decl, Type, TypeReference, Redshift_SN, HostName, HostType, Redshift_Gal, Notes, Public) "+\
                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        vals = [objname]
        # parse simbad to attempt to find more info
        simbad_info = get_SN_info_simbad( objname )
        vals.extend( [simbad_info.get('ra'), simbad_info.get('dec')] )
        vals.extend( [simbad_info.get('Type'), simbad_info.get('TypeReference'), simbad_info.get('Redshift_SN')] )
        vals.extend( [simbad_info.get('HostName'), simbad_info.get('HostType'), simbad_info.get('Redshift_Gal')] )
        vals.append( simbad_info.get('Notes') )
        # assume all newly-inserted objects are not public
        vals.append( 0 )
        # now go ahead and put it in
        print_sql( sqlinsert, vals )
        c.execute( sqlinsert, vals )
        DB.commit()
        # now grab the ObjID from this new entry
        c.execute( sqlfind, [objname] )
        res = c.fetchone()
        c.close()
        return res['ObjID']

def handle_spectralrun( fitsfile, objname ):
    """
    fitsfile should be absolute path.
    """
    # parse the input files to find the runinfo
    info = get_info_spec_fitsfile( fitsfile )
    datestr = '%d-%d-%d' %(info['date'].year, info['date'].month, info['date'].day)
    instrument = info['instrument']
    sqlfind = 'SELECT RunID,Targets FROM spectralruns WHERE (UT_Date = Date(%s)) AND (Instrument = %s);'
    c = DB.cursor()
    c.execute( sqlfind, [datestr, instrument] )
    res = c.fetchone()
    if res:
        # associated spectral run already exists.
        # append this object name onto the end of the objects observed and return RunID
        sqlupdate = "UPDATE spectralruns SET (Targets = %s) WHERE (RunID = %s);"
        newtargets = res['Targets'] + ' | ' + objname
        print_sql( sqlupdate, [newtargets, res['RunID']] )
        c.execute( sqlupdate, [newtargets, res['RunID']] )
        DB.commit()
        return res['RunID']
    else:
        # need to parse the input files to define the spectral run
        sqlinsert = "INSERT INTO spectralruns (UT_Date, Targets, Reducer, Observer, Instrument, Telescope) "+\
                                      "VALUES (DATE(%s), %s, %s, %s, %s, %s);"
        vals = [datestr, objname, info['reducer'], info['observer'], info['instrument'], info['observatory']]
        print_sql( sqlinsert, vals )
        c.execute( sqlinsert, vals )
        DB.commit()
        # now grab the RunID from this new entry
        c.execute( sqlfind, [datestr, instrument] )
        res = c.fetchone()
        c.close()
        return res['RunID']
        
def handle_new_spectrum( flmfile, fitsfile, folder ):
    """
    flmfile and fitsfile should be absolute paths, folder should be just the name of the 
     folder hosting these data (associated with object name).
    """
    # parse the filename
    is_sn = '[sS][nN]\d{4}.+'
    if re.search( folder, is_sn ):
        objname = folder.replace('sn','SN ')
    else:
        objname = folder
    # first see if this file is already in the database
    sqlfind = 'SELECT SpecID FROM spectra WHERE (Filename = %s) and (Filepath = %s);'
    fpath, fname = os.path.split( flmfile )
    # fpath is only from Data on
    fpath = fpath[ fpath.index('Data') : ]
    c = DB.cursor()
    c.execute( sqlfind, [fname, fpath] )
    res = c.fetchone()
    if res:
        # file already in database!
        return res['SpecID']
    else:
        # need to parse the files to insert this spectrum
        sqlinsert = "INSERT INTO spectra (ObjID, RunID, Filename, Filepath, UT_Date, Airmass, Exposure, Position_Angle, Parallactic_Angle, SNR, Min, Max, Blue_Resolution, Red_Resolution, SNID_Type, SNID_Subtype) "+\
                                 "VALUES (%s, %s, %s, %s, DATE(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );"
        # handle the object and spectralrun
        objid = handle_object( objname )
        runid = handle_spectralrun( fitsfile, folder )
        vals = [objid, runid, fname, fpath]
        # pull info from fitsfile 
        info = get_info_spec_fitsfile( fitsfile )
        datestr = '%d-%d-%d' %(info['date'].year, info['date'].month, info['date'].day)
        vals.append( datestr )
        vals.extend( [info['airmass'], info['exptime'] ])
        vals.extend( [info['position_ang'], info['parallac_ang']] )
        # pull info from flmfile
        info = get_info_spec_flmfile( flmfile )
        vals.extend([ info['SNR'], info['MinWL'], info['MaxWL'], info['BlueRes'], info['RedRes'] ])
        # run snid and insert the result
        t,st = getSNID( flmfile )
        vals.extend( [t, st] )
        # actually do the insert
        print_sql( sqlinsert, vals )
        c.execute( sqlinsert, vals )
        DB.commit()
        # now grab the SpecID from this new entry
        c.execute( sqlfind, [fname, fpath] )
        res = c.fetchone()
        c.close()
        return res['SpecID']

def handle_new_lightcurve( photfile ):
    """
    photfile should be absolute path to new lightcurve file.
    """
    fpath,fname = os.path.split( f )
    # trim the fpath relative to the Data directory
    fpath = fpath[ fpath.index('Data') : ]
    # if this file is already in the DB, update the entry instead of creating it
    sqlfind = 'SELECT PhotID FROM photometry WHERE (Filename = %s) and (Filepath = %s);'
    c = DB.cursor()
    c.execute( sqlfind, [fname, fpath] )
    res = c.fetchone()
    photid = res.get('PhotID') # this will be None if an entry doesn't yet exist
    # see if the file is public
    if 'public' in fname:
        public = 1
    else:
        public = 0
    # find the object name
    objname = fname.split('.')[0]
    is_sn = '[sS][nN]\d{4}.+'
    if re.search( objname, is_sn ):
        objname = objname.replace('sn','SN ')
    # get the object id
    objid = handle_object_phot( objname )
    # pull info from photfile
    firstobs, lastobs, filters, telescopes, npoints = parse_photfile( f )
    firstobs = '%d-%d-%d'%(firstobs[0],firstobs[1],firstobs[2])
    lastobs = '%d-%d-%d'%(lastobs[0],lastobs[1],lastobs[2])
    # now actually put it in
    sqlinsert = "INSERT INTO photometry (ObjID, Filename, Filepath, Filters, Telescopes, FirstObs, LastObs, NPoints, Public) "+\
                                "VALUES (%s, %s, %s, %s, %s, DATE(%s), DATE(%s), %s, %s);"
    sqlupdate = "UPDATE photometry SET ObjId = %s, Filename = %s, Filepath = %s, Filters = %s, Telescopes = %s, FirstObs = Date(%s), LastObs = Date(%s), NPoints = %s, Public = %s "+\
                "WHERE (PhotID = %s);"
    vals = [objid, fname, fpath, filters, telescopes, firstobs, lastobs, npoints, public]
    if photid != None:
        vals.append( photid )
        print_sql( sqlupdate, vals )
        c.execute( sqlupdate, vals )
    else:
        print_sql( sqlinsert, vals )
        c.execute( sqlinsert, vals )
    DB.commit()
    # now grab the PhotID from this new/updated entry
    c.execute( sqlfind, [fname, fpath] )
    res = c.fetchone()
    c.close()
    return res['PhotID']

def run_on_folder( folder ):
    specs = yield_all_spectra( folder )
    for flm,fit in specs:
        print flm,':::',fit
        folder = os.path.split(os.path.split( flm )[0])[1]
        specid = handle_new_spectrum( flm, fit, folder )
        print 'inserted with SpecID =',specid
        break