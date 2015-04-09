import MySQLdb as mdb
import sys

passwords_obj = open('passwords.txt', 'r')
passwords = passwords_obj.read().split() #My passwords contained in this list, entry two is password for classy.astro


try:
    olddb_con = mdb.connect('heracles.astro.berkeley.edu', 'root', passwords[3], 'sndb')
    newdb_con = mdb.connect('heracles.astro.berkeley.edu', 'root', passwords[3], 'newsndb')

    old_cur = olddb_con.cursor()
    new_cur = newdb_con.cursor()
    
    new_cur.execute('select RunID, TelescopeID from spectralruns')   #Creates a tuple list of the Photmetry ID and Filter
    photid_and_tele = new_cur.fetchall()
    photID = [str(i[0]) for i in photid_and_tele]      #list of Photometry ID Numbers
    tele = [str(i[1]) for i in photid_and_tele]        #corresponding filter number
    
    old_cur.execute('select TelescopeID, Name from telescopes') 
    filtdict = dict(old_cur.fetchall())  #dictionary relating filterID and filter
    
    for i in range(len(photid_and_tele)):  #iterate through each item in photID and filt
        new_cur.execute("update spectralruns set Telescope='%s' where RunID=%s" % (filtdict[int(tele[i])],photID[i]))  #Set Filter value
        
    new_cur.execute("select count(*) from spectralruns where Telescope is not null")
    print new_cur.fetchone() 
        
    newdb_con.commit()
    
    new_cur.close()
    old_cur.close()
        
except mdb.Error, e:
  
    print "Error %d: %s" % (e.args[0],e.args[1])
    sys.exit(1)
    
finally:    
        
    if olddb_con: 
        olddb_con.close()
    if newdb_con: 
        newdb_con.close()