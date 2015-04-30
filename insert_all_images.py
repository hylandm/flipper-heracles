"""
Goes through all images the code can find and inserts them into
 the database.

"""
import MySQLdb
from scriptsLib import yield_all_images,get_info_image_fitsfile

DB = MySQLdb.connect(host=c.host, user=c.user, passwd=c.passwd, db='newsndb')
c = DB.cursor()

allkait = yield_all_images( location='/media/raid0/Data/kait/' )
allnickel = yield_all_images( location='/media/raid0/Data/nickel/' )

for image in allkait:
    fpath,fname = os.path.split(image)
    # see if it's already in the DB
    cmd = 'SELECT * FROM images WHERE ((Filename = fname) AND (Filepath = fpath));'
    c.execute(cmd)
    try:
        c.fetchone()
    except MySQLdb.OperationalError:
        # this image already in DB; just move on
        print root,f,'already in DB.'
        continue
    print 'adding',root,f
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
            filt = re.findall('[BVRI]\.fit', fitsfile)[0][0]
        except IndexError:
            filt = None
    cmd = cmd %(fname, fpath, filt, info['object'], info['ra_d'], info['dec_d'],
                info['instrument'], info['telescope'], info['date'], info['exptime'])
    print cmd
    c.execute(cmd)
    