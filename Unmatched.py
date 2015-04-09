#A script which writes a file naming all spectra missing a fits file

from glob import glob
from os import walk, path
import re
from difflib import get_close_matches

def yield_all_spectra( location='/media/raid0/data/spectra/' ):
    """
    Searches <location> (and subfolders) for all spectra.
    Finds all files named like *.flm and attempts to associate each with 
    a .fits file in the same folder (or a subfolder).
    This is an iterator, and returns paths as strings (flm, fit) one at a time.
    
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
                yield flm, fit, f

f = open('Unmatched.txt', 'w')
spec = yield_all_spectra()
#A generator which returns a tuple triplet of a spectra's file, its matching fitsfile and its file name.
unmatched_objects_list = []
for pair in spec:
    if pair[1]==None:
        #If there is a flm file without a matching fits file.
        objname=re.sub('[._-].*\Z', '', pair[2])
        #Removes all characters including and after the first instance of '.', '_' or '-'
        if objname not in unmatched_objects_list:
            #store truncated filename (ideally should just be the name of the object) in a list
            unmatched_objects_list.append(objname)
            f.write(objname+'\n')
f.close()