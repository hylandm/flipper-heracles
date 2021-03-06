"""
Goes through all images the code can find and inserts them into
 the database.


The mysql definition of the images table:

CREATE TABLE images
(
ImgID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
Filename VARCHAR(200) NOT NULL,
Filepath VARCHAR(200) NOT NULL,
Filter VARCHAR(10),
ObjName VARCHAR(50),
RA DOUBLE PRECISION NOT NULL,
Decl DOUBLE PRECISION NOT NULL,
Instrument VARCHAR(50),
Telescope VARCHAR(45),
Date DATE NOT NULL,
Exposure INT
);
"""
import MySQLdb
import os
from scriptsLib import yield_all_images,get_info_image_fitsfile
import credentials as creds
import re

DB = MySQLdb.connect(host=creds.host, user=creds.user, passwd=creds.passwd, db='newsndb')
c = DB.cursor()

def insert_image( image ):
    fpath,fname = os.path.split(image)
    # trim the fpath relative to the Data directory
    fpath = fpath.split('raid0/')[1]
    ## see if it's already in the DB
    #cmd = "SELECT * FROM images WHERE ((Filename = '%s') AND (Filepath = '%s'));" %(fname,fpath)
    #print cmd
    #c.execute(cmd)
    #res = c.fetchone()
    #if res != None:
    #    # this image already in DB; just move on
    #    print fpath,fname,'already in DB.'
    #    return
    print 'adding',fpath,fname
    info = get_info_image_fitsfile(image)
    # ignore calibration images
    if info['object'] in ['bias','dark','flat']:
        return
    # parse filtername
    if info['filter']:
        filt = info['filter']
    elif info['filter2']:
        filt = info['filter2']
    else:
        # try and find filter in filename
        try:
            filt = re.findall('[BVRI]\.fit', fname)[0][0]
        except IndexError:
            filt = None
    # find exposure time
    if info['exptime']:
        exp = info['exptime']
    elif info['exptime2']:
        exp = info['exptime2']
    else:
        exp = 0.0
    if exp < 10.0:
        # don't include short observations / calibrations
        return
    # choose the obs date
    if info['dateobs']:
        d = info['dateobs']
    elif info['date']:
        d = info['date']
    else:
        raise Exception('No date found!')
    date = '%d-%d-%d'%(d.year, d.month, d.day)
    # sometimes the nickel images don't include telescope keyword
    if (not info['telescope']) and ('nickel' in info['instrument'].lower()):
        info['telescope'] = 'NICKEL'
    # translate nones into nulls
    for k in info.keys():
        if not info[k]:
            info[k] = 'NULL'
    cmd = "INSERT INTO images (Filename, Filepath, Filter, ObjName, RA, Decl, Instrument, Telescope, Date, Exposure) \n"+\
          "VALUES ('%s', '%s', '%s', '%s', %f, %f, '%s', '%s', '%s', %d);"
    cmd = cmd %(fname, fpath, filt, info['object'], info['ra_d'], info['dec_d'],
                info['instrument'], info['telescope'], date, exp)
    print cmd
    c.execute(cmd)
    DB.commit()
    #raw_input()
    return


#for image in yield_all_images( location='/media/raid0/Data/kait/' ):
#    try:
#        insert_image( image )
#    except Exception as e:
#        open('failed_files.txt','a').write('%s ::: %s\n'%(image,e.args[0]))
#for image in yield_all_images( location='/media/raid0/Data/nickel/' ):
#    try:
#        insert_image( image )
#    except:
#        open('failed_files.txt','a').write('%s ::: %s\n'%(image,e.args[0]))
images = open('failed_files.bak','r').readlines()
for line in images:
    try:
        image = line.split(':::')[0].strip()
        insert_image( image )
    except Exception as e:
        open('failed_files.txt','a').write('%s ::: %s\n'%(image,e.args[0]))

