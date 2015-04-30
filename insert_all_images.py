"""
Goes through all images the code can find and inserts them into
 the database.


The mysql definition of the images table:

CREATE TABLE images
(
ImgID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
Filename VARCHAR(200) NOT NULL,
Filepath VARCHAR(200) NOT NULL,
Filter VARCHAR(4),
ObjName VARCHAR(50),
RA FLOAT NOT NULL,
Decl FLOAT NOT NULL,
Instrument VARCHAR(50),
Telescope VARCHAR(45),
UT_Date DATE NOT NULL,
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

allkait = yield_all_images( location='/media/raid0/Data/kait/' )
allnickel = yield_all_images( location='/media/raid0/Data/nickel/' )

for image in allkait:
    fpath,fname = os.path.split(image)
    # see if it's already in the DB
    cmd = "SELECT * FROM images WHERE ((Filename = '%s') AND (Filepath = '%s'));" %(fname,fpath)
    c.execute(cmd)
    try:
        c.fetchone()
    except MySQLdb.OperationalError:
        # this image already in DB; just move on
        print fpath,fname,'already in DB.'
        continue
    print 'adding',fpath,fname
    info = get_info_image_fitsfile(image)
    cmd = "INSERT INTO images (Filename, Filepath, Filter, ObjName, RA, Decl, Instrument, Telescope, UT_Date, Exposure) VALUES (%s, %s, %s, %s, %f, %f, %s, %s, %s, %d);"
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
    cmd = cmd %(fname, fpath, filt, info['object'], info['ra_d'], info['dec_d'],
                info['instrument'], info['telescope'], info['date'], info['exptime'])
    print cmd
    # c.execute(cmd)
    
