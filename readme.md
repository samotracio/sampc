SAMP Client Package
===================
This package implements a client to provide basic data exchange capabilities
using the SAMP protocol. This allows to easily send and receive tabular data 
to VO applications such as Topcat or Aladin.

It provides two Classes:

* Client :  Python object that is a proxy to send and receive data from/to applications

* Hub : Samp hub that is required to manage the communications between VO applications

Note this has only been tested in Client mode, using the Hub already created 
by Topcat.

Note exchange data is saved as fits files in the directory $HOME/tempo/samptables,
which is wiped when a client class object is instanciated

Requirements
------------
1. sampy : to provide samp access
2. astropy.tables :  to provide fits input/ouput of tables

Basic Usage
-----------
    # (Start Topcat!)
    from sampc import Client
    c = Client()

    # SEND A TABLE
    arr = np.random.random([100,3])
    c.send(arr,'arr')

    # RECEIVE A TABLE  (broadcast SomeTable.fits from Topcat)
    data = c['SomeTable.fits']

    # SEND ROWS     
    c.sendrows('arr',[3,6,15,16])

    # RECEIVE ROWS (broadcast some rows or a subset in Topcat)
    c.rowlist

For further examples see documentation for class ``Client``

Credits
-------
Modifications by E. Donoso, mainly to use astropy.tables, bug fixes when 
receiving multiple rows, and other convenience functions. Original client 
by M. Fouesneau (https://github.com/mfouesneau/pylibs)