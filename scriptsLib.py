###########################################################
#  A collection of python tasks for managing the 
#   flipper group's heracles computer. 
#
#   -  March, 2015 -- ishivvers, hylandm - 
#
###########################################################

import re
from glob import glob
from os import walk, path
import numpy as np
import pyfits as pf
from difflib import get_close_matches
from cStringIO import StringIO
from subprocess import Popen,PIPE
from dateutil import parser
from urllib2 import urlopen
import julian_dates as jd

def yield_all_spectra( location='/media/raid0/Data/spectra/', include_details_flm=False ):
    """
    Searches <location> (and subfolders) for all spectra.
    Finds all files named like *.flm and attempts to associate each with 
    a .fits file in the same folder (or a subfolder).
    This is an iterator, and returns paths as strings (flm, fit) one at a time.

    If include_details_flm == True, will include *.flm files found in a '/details/' subfolder.
    
    Example: 
     > s = yield_all_spectra()
     > path_to_spec_1, path_to_fitsfile_1 = s.next()
     > path_to_spec_2, path_to_fitsfile_2 = s.next()
    """
    for root,subdirs,fnames in walk(location):
        for f in fnames:
            if re.search('.+\.flm',f):
                # we found a spectrum
                flm = root + '/'+f
                matching_fits = None
                # first look for a local matching fits file
                local_fs = map(path.basename, glob(root+'/*.fits'))
                # if there's a pretty good match, just use it
                bestmatch = get_close_matches(f, local_fs, n=1)
                if bestmatch:
                    matching_fits = '%s/%s' %(root,bestmatch[0])
                else:
                    # if there's not, then look in all subfolders one layer deep
                    if subdirs:
                        for sd in subdirs:
                            sub_fs = map(path.basename, glob(root+'/'+sd+'/*.fits'))
                            bestmatch = get_close_matches(f, sub_fs, n=1)
                            if bestmatch:
                                matching_fits = '%s/%s/%s' %(root,sd,bestmatch[0])
                                break
                # record the best matching fits file, or None if no good match found
                fit = matching_fits
                if not include_details_flm:
                    # do not return this pair if the flm file is in a details subfolder
                    if re.search('details', flm):
                        continue
                yield flm, fit
 
def yield_all_images( location='/media/raid0/Data/nickel/follow/' ):
    """
    Searches <location> (and subfolders) for all images.
    Finds all files named like *.fit, *.fit.Z, *.fits, *.fits.Z, *.fts, *.fts.Z
    and yields them one at a time (this is an iterator).

    Example:
     > i = yield_all_images()
     > path_to_image_1 = i.next()
     > path_to_image_2 = i.next()
     > ...
    """
    rx = '\.((fit)|(fits)|(fts))($|\.[zZ]$)'
    for root,subdirs,fnames in walk(location):
        for f in fnames:
            if re.search(rx, f):
                yield '%s/%s' %(root,f)

def get_info_image_fitsfile( fitsfile ):
    """
    Takes in the path to an image fitsfile and attempts to pull
     from it several useful bits of information.
    
    Returns a dictionary containing the relevant header items.
    """
    try:
        hdu = pf.open( fitsfile )
    except IOError:
        # probably a zcatted file
        p = Popen(["zcat", fitsfile], stdout=PIPE)
        hdu = pf.open( StringIO(p.communicate()[0]) )
    hdu.verify('fix')
    head = hdu[0].header
    
    ks = [ ['object','object'],
           ['ra','ra'],
           ['dec','dec'],
           ['ra_d','ra'],
           ['dec_d','dec'],
           ['exptime','exposure'],
           ['exptime2','exptime'],
           ['date','date'],
           ['dateobs','date-obs'],
           ['utc','time'],
           ['date_mjd','mjd-obs'],
           ['airmass','airmass'],
           ['telescope','telescop'],
           ['instrument','instrume'],
           ['observer','observer'],
           ['filter','filters'],
           ['filter2','filtnam'] ]
    
    outdict = {}
    for outk, fitsk in ks:
        try:
            val = head[fitsk]
        except:
            val = None
        if val == None:
            pass
        elif outk in ['exptime','exptime2','date_mjd','airmass', 'utc']:
            val = float(val)
        elif outk in ['date','dateobs']:
            val = parser.parse( val ) #parse the datetime string in a reasonable way
        elif outk == 'ra_d':
            val = _parse_ra( val )
        elif outk == 'dec_d':
            val = _parse_dec( val )
        else:
            try:
                val = val.strip()
            except:
                pass
        outdict[outk] = val

    return outdict    

def get_info_spec_fitsfile( fitsfile ):
    """
    Takes in the path to a spectrum fitsfile and attempts to pull 
     from it several useful bits of information.
    
    Returns a dictionary of:
     {object (string),
      ra (string),
      dec (string),
      ra_d (degrees, float),
      dec_d (degrees, float),
      exptime (seconds, float),
      date (string),
      utc (string),
      date_mjd (MJD, float),
      airmass (float),
      observatory (string),
      instrument (string),
      observer (string),
      reducer (string),
      seeing (float)}
    """
    hdu = pf.open( fitsfile )
    head = hdu[0].header
    
    ks = [ ['object','object'],
           ['ra','ra'],
           ['dec','dec'],
           ['ra_d','ra'],
           ['dec_d','dec'],
           ['exptime','exptime'],
           ['date','date-obs'],
           ['utc','utc'],
           ['date_mjd','mjd-obs'],
           ['airmass','airmass'],
           ['observatory','observat'],
           ['instrument','instrume'],
           ['observer','observer'],
           ['reducer','reducer'],
           ['seeing','seeing'],
           ['position_ang', 'tub'],
           ['parallac_ang', 'opt_pa'] ]
    
    outdict = {}
    for outk, fitsk in ks:
        try:
            val = head[fitsk]
        except:
            val = None
        if val == None:
            pass
        elif outk in ['exptime','date_mjd','airmass', 'position_ang', 'parallac_ang', 'seeing']:
            val = float(val)
        elif outk == 'ra_d':
            val = _parse_ra( val )
        elif outk == 'dec_d':
            val = _parse_dec( val )
        elif outk == ['date']:
            val = parser.parse( val ) #parse the datetime string in a reasonable way
        else:
            val = val.strip()
        outdict[outk] = val
    
    return outdict

def parse_filename( f ):
    """
    Parses a *.flm file for observation date and object name.
    Returns (year, month, day), object_name
    """
    datestring = re.search('\d{8}(\.\d+)?', f).group()
    y = int(datestring[:4])
    m = int(datestring[4:6])
    d = float(datestring[6:])
    obj = f.split(datestring)[0].strip('-')
    return (y,m,d), obj


##########################################
# helper functions
##########################################

def _parse_ra( inn ):
    '''
    Parse input RA string, either decimal degrees or sexagesimal HH:MM:SS.SS (or similar variants).
    Returns decimal degrees.
    '''
    # if simple float, assume decimal degrees
    try:
        ra = float(inn)
        return ra
    except:
        # try to parse with phmsdms:
        res = _parse_sexagesimal(inn)
        ra = 15.*( res['vals'][0] + res['vals'][1]/60. + res['vals'][2]/3600. )
        return ra

def _parse_dec( inn ):
    '''
    Parse input Dec string, either decimal degrees or sexagesimal DD:MM:SS.SS (or similar variants).
    Returns decimal degrees.
    '''
    # if simple float, assume decimal degrees
    try:
        dec = float(inn)
        return dec
    except:
        # try to parse with phmsdms:
        res = _parse_sexagesimal(inn)
        dec = res['sign']*( res['vals'][0] + res['vals'][1]/60. + res['vals'][2]/3600. )
        return dec

def _parse_sexagesimal(hmsdms):
    """
    +++ Pulled from python package 'angles' +++
    Parse a string containing a sexagesimal number.
    
    This can handle several types of delimiters and will process
    reasonably valid strings. See examples.
    
    Parameters
    ----------
    hmsdms : str
        String containing a sexagesimal number.
    
    Returns
    -------
    d : dict
    
        parts : a 3 element list of floats
            The three parts of the sexagesimal number that were
            identified.
        vals : 3 element list of floats
            The numerical values of the three parts of the sexagesimal
            number.
        sign : int
            Sign of the sexagesimal number; 1 for positive and -1 for
            negative.
        units : {"degrees", "hours"}
            The units of the sexagesimal number. This is infered from
            the characters present in the string. If it a pure number
            then units is "degrees".
    """
    units = None
    sign = None
    # Floating point regex:
    # http://www.regular-expressions.info/floatingpoint.html
    #
    # pattern1: find a decimal number (int or float) and any
    # characters following it upto the next decimal number.  [^0-9\-+]*
    # => keep gathering elements until we get to a digit, a - or a
    # +. These three indicates the possible start of the next number.
    pattern1 = re.compile(r"([-+]?[0-9]*\.?[0-9]+[^0-9\-+]*)")
    # pattern2: find decimal number (int or float) in string.
    pattern2 = re.compile(r"([-+]?[0-9]*\.?[0-9]+)")
    hmsdms = hmsdms.lower()
    hdlist = pattern1.findall(hmsdms)
    parts = [None, None, None]
    
    def _fill_right_not_none():
        # Find the pos. where parts is not None. Next value must
        # be inserted to the right of this. If this is 2 then we have
        # already filled seconds part, raise exception. If this is 1
        # then fill 2. If this is 0 fill 1. If none of these then fill
        # 0.
        rp = reversed(parts)
        for i, j in enumerate(rp):
            if j is not None:
                break
        if  i == 0:
            # Seconds part already filled.
            raise ValueError("Invalid string.")
        elif i == 1:
            parts[2] = v
        elif i == 2:
            # Either parts[0] is None so fill it, or it is filled
            # and hence fill parts[1].
            if parts[0] is None:
                parts[0] = v
            else:
                parts[1] = v
    
    for valun in hdlist:
        try:
            # See if this is pure number.
            v = float(valun)
            # Sexagesimal part cannot be determined. So guess it by
            # seeing which all parts have already been identified.
            _fill_right_not_none()
        except ValueError:
            # Not a pure number. Infer sexagesimal part from the
            # suffix.
            if "hh" in valun or "h" in valun:
                m = pattern2.search(valun)
                parts[0] = float(valun[m.start():m.end()])
                units = "hours"
            if "dd" in valun or "d" in valun:
                m = pattern2.search(valun)
                parts[0] = float(valun[m.start():m.end()])
                units = "degrees"
            if "mm" in valun or "m" in valun:
                m = pattern2.search(valun)
                parts[1] = float(valun[m.start():m.end()])
            if "ss" in valun or "s" in valun:
                m = pattern2.search(valun)
                parts[2] = float(valun[m.start():m.end()])
            if "'" in valun:
                m = pattern2.search(valun)
                parts[1] = float(valun[m.start():m.end()])
            if '"' in valun:
                m = pattern2.search(valun)
                parts[2] = float(valun[m.start():m.end()])
            if ":" in valun:
                # Sexagesimal part cannot be determined. So guess it by
                # seeing which all parts have already been identified.
                v = valun.replace(":", "")
                v = float(v)
                _fill_right_not_none()
        if not units:
            units = "degrees"
    
    # Find sign. Only the first identified part can have a -ve sign.
    for i in parts:
        if i and i < 0.0:
            if sign is None:
                sign = -1
            else:
                raise ValueError("Only one number can be negative.")
    
    if sign is None:  # None of these are negative.
        sign = 1
    
    vals = [abs(i) if i is not None else 0.0 for i in parts]
    return dict(sign=sign, units=units, vals=vals, parts=parts)


def getSNID( flmfile ):
    """
    Run SNID on an ASCII spectrum,
     simply returning the best type as 
     determined by fraction and slope.
    """
    try:
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
        if t == '':
            t = 'NoMatch'
        if st == '':
            st = 'NoMatch'
        return t, st
    except:
        return 'NoMatch','NoMatch'

def getSNR( flmfile, w=20 ):
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

def get_info_spec_flmfile( flmfile ):
    """
    Calculates a few things about the input flmfile and returns them as a dictionary.
    """
    outd = {}
    outd['SNR'] = getSNR( flmfile )
    d = np.loadtxt( flmfile )
    outd['MinWL'] = d[0][0]
    outd['MaxWL'] = d[0][-1]
    # take average resolutions of 10 pixels on either end for red/blue resolutions
    outd['BlueRes'] = np.mean(d[0][1:11] - d[0][0:10])
    outd['BlueRes'] = np.mean(d[0][-10:] - d[0][-11:-1])
    return outd

def get_SN_info_simbad( name ):
    """
    Queries simbad for SN coords, redshift, and host galaxy.
    If redshift is not given for SN, attempts to resolve link to 
     host galaxy and report its redshift.
    Returns ( (ra,dec), redshift, host_name, redshift_citation ), with
     values of None inserted whenever it cannot resolve the value.
    """
    simbad_uri = "http://simbad.u-strasbg.fr/simbad/sim-id?output.format=ASCII&Ident=%s"
    result = urlopen( simbad_uri % name.replace(' ','%20') ).read()
    outd = {}

    # try to get the coordinates
    regex_coords = "Coordinates\(FK5.+\): .+"
    res_coords = re.search( regex_coords, result )
    try:
        cs = res_coords.group().split(':')[1].strip()
        outd['RA'] = _parse_ra( cs[:12].strip() )
        outd['Decl'] = _parse_dec( cs[12:].strip() )
    except AttributeError:
        outd['RA'] = None
        outd['Decl'] = None

    # try to get the type
    regex_type = "Spectral type: [^s]+"
    res_type = re.search( regex_type, result )
    try:
        typrow = res_type.group().split(':')[1].split()
        typ = typrow[0].strip()
        typ = typ.replace('SN','')
        outd['Type'] = typ
        typref = typrow[-1].strip()
        if '~' in typref:
            outd['TypeReference'] = None
        else:
            outd['TypeReference'] = typref
    except AttributeError:
        outd['Type'] = None
        outd['TypeReference'] = None

    # try to get the redshift
    regex_redshift = "Redshift:\s+\d+\.\d+.+"
    res_red = re.search( regex_redshift, result )
    try:
        outd['Redshift_SN'] = float(res_red.group().strip('Redshift: ').split(' ')[0])
        outd['Redshift_SN_citation'] = res_red.group().split(' ')[-1]
    except AttributeError:
        outd['Redshift_SN'] = None
        outd['Redshift_SN_citation'] = None

    # try to get the host info
    regex_host = "apparent\s+host\s+galaxy\s+.+?\{(.*?)\}"
    res_host = re.search( regex_host, result )
    try:
        host = res_host.group().split('{')[1].split('}')[0]
        outd['HostName'] = host
    except AttributeError:
        host = None
        outd['HostName'] = host
    if host != None:
        result = urlopen( simbad_uri % host.replace(' ','%20') ).read()
        # get the type of the host galaxy
        regex_type = "Morphological type:\s+[^\s]+\s"
        res_type = re.search( regex_type, result )
        try:
            outd['HostType'] = res_type.group().split(':')[1].strip()
        except AttributeError:
            outd['HostType'] = None
        # get the redshift of the host galaxy
        regex_redshift = "Redshift:\s+\d+\.\d+.+"
        res_red = re.search( regex_redshift, result )
        try:
            outd['Redshift_Gal'] = float(res_red.group().strip('Redshift: ').split(' ')[0])
            outd['Redshift_Gal_citation'] = res_red.group().split(' ')[-1]
        except AttributeError:
            outd['Redshift_Gal'] = None
            outd['Redshift_Gal_citation'] = None
    else:
        outd['Redshift_Gal'] = None
        outd['Redshift_Gal_citation'] = None

    # pull out the notes field
    try:
        outd['Notes'] = result.split('Notes')[1]
    except:
        outd['Notes'] = None
    return outd

def get_SN_info_rochester():
    """
    would be awesome. should implement this.
    """
    pass

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
