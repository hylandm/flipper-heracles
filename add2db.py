"""
Adding new files/objects/spectralruns to the database!


Any time a folder is changed, run the appropriate function:
 - import_spec_from_folder( path_to_folder )
 - import_phot_from_folder( path_to_folder )
 
-- ishivvers, May 2015
"""

import MySQLdb
import MySQLdb.cursors
import credentials as creds
import re
import os
from glob import glob
from scriptsLib import *
DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db=creds.db, cursorclass=MySQLdb.cursors.DictCursor)

#################################################################
# helper functions
#################################################################

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
        # try to parse the web to define the object
        sqlinsert = "INSERT INTO objects (ObjName, RA, Decl, Type, TypeReference, Redshift_SN, HostName, HostType, Redshift_Gal, Notes, Public) "+\
                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        vals = [objname]
        # parse rochester to get info. all characters are lowercase in ROCHESTER
        info = get_SN_info_rochester( objname.lower().replace(' ','') )
        # parse simbad to attempt to find more info. simbad understands a variety of name formats
        info.update( get_SN_info_simbad( objname ) )
        # if neither method got us any where, raise an error
        if not info:
            raise Exception( 'Cannot find any info on this object : %s'%objname )
        for k in ['RA','Decl','Type','TypeReference','Redshift_SN','HostName','HostType','Redshift_Gal','Notes']:
            # if value wasn't found, value is None (NULL in MySQL)
            vals.append( info.get(k) )
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
    fitsfile should be absolute path. objname should be as you want it in the SNDB.
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
        sqlupdate = "UPDATE spectralruns SET Targets = %s WHERE (RunID = %s);"
        newtargets = res['Targets'] + ' | ' + objname
        print_sql( sqlupdate, [newtargets, res['RunID']] )
        c.execute( sqlupdate, [newtargets, res['RunID']] )
        DB.commit()
        return res['RunID']
    else:
        # need to parse the input files to define the spectral run
        sqlinsert = "INSERT INTO spectralruns (UT_Date, Targets, Reducer, Observer, Instrument, Telescope, Seeing) "+\
                                      "VALUES (DATE(%s), %s, %s, %s, %s, %s, %s);"
        vals = [datestr, objname, info.get('reducer'), info.get('observer'), info.get('instrument'), info.get('observatory'), info.get('seeing')]
        print_sql( sqlinsert, vals )
        c.execute( sqlinsert, vals )
        DB.commit()
        # now grab the RunID from this new entry
        c.execute( sqlfind, [datestr, instrument] )
        res = c.fetchone()
        c.close()
        return res['RunID']
        
def handle_new_spectrum( flmfile, fitsfile, objname ):
    """
    flmfile and fitsfile should be absolute paths, objname should be the name of the 
     object. can be in folder format instead of DB format.
    """
    # parse the objname if it's not in DB format
    if re.search( '[sS][nN]\d{4}.+', objname ):
        # massage folder formats into the normal SN format in the DB
        objname = objname.replace('sn','SN ')
        if re.search( '\d{4}[a-zA-Z]$', objname ):
            objname = objname.upper()
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
                                 "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );"
        # handle the object and spectralrun
        objid = handle_object( objname )
        runid = handle_spectralrun( fitsfile, objname )
        vals = [objid, runid, fname, fpath]
        # pull info from fitsfile 
        info = get_info_spec_fitsfile( fitsfile )
        datestr = '%d-%d-%d' %(info['date'].year, info['date'].month, info['date'].day)
        datefloat = parse_datestring( os.path.basename( flmfile ) )
        vals.append( datefloat )
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

def handle_new_lightcurve( photfile, objname ):
    """
    photfile should be absolute path to new lightcurve file. objname should be the name of the 
     object. can be in folder format instead of DB format.
    """
    # parse the objname if it's not in DB format
    if re.search( '[sS][nN]\d{4}.+', objname ):
        # massage folder formats into the normal SN format in the DB
        objname = objname.replace('sn','SN ')
        if re.search( '\d{4}[a-zA-Z]$', objname ):
            objname = objname.upper()
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
    # get the object id
    objid = handle_object( objname )
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

#################################################################
# main functions
#################################################################

def import_spec_from_folder( folder ):
    """
    Scan a folder for any flm files not in the DB, and insert them appropriately.
    """
    specs = yield_all_spectra( folder, require_fits=True )
    for flm,fit in specs:
        print flm,'is associated with',fit
        folder = os.path.split(os.path.split( flm )[0])[1]
        specid = handle_new_spectrum( flm, fit, folder )
        print '\ninserted with SpecID =',specid

def import_phot_from_folder( folder ):
    """
    Scan a folder for any phot (*.dat) files not in the DB, and insert them appropriately.
    """
    for f in glob( folder+'/*.dat' ):
        photfile = f
        objname = f.split('.')[0]
        handle_new_lightcurve( photfile, objname )
    