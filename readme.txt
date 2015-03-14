This is a repo to manage all of the code used for the
 Spring 2015 project to rebuild the Filippenko group
 computer, backup system, and database management system.

Created March 2015
  - Michael Hyland
  - Isaac Shivvers

Testing 123

Quick Examples:

# find a spectrum, and get some info about it
 s = scriptsLib.yield_all_spectra()
 specfile, fitsfile = s.next()
 while fitsfile == None:
    specfile, fitsfile = s.next()
 info = scriptsLib.get_info_spec_fitsfile( fitsfile )
 print 'info about',fitsfile,':::'
 print info

# find an image, and get some info about it
 i = scriptsLib.yield_all_images()
 fitsfile = i.next()
 info = scriptsLib.get_info_image_fitsfile( fitsfile )
 print 'info about',fitsfile,':::'
 print info
 
