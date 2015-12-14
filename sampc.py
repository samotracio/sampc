"""
SAMP Client Package
---------------------------------------------------
	
This package implements a client to provide basic data exchange capabilities
using the SAMP protocol. This allows to easily send and receive tabular data 
to VO applications such as Topcat or Aladin.

It provides two Classes:

- Client :  Python object that is a proxy to send and receive data from/to applications

- Hub : Samp hub that is required to manage the communications between VO applications

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
::

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
"""
__version__ = '1.0'
import atexit, os, urllib
import sampy
import numpy as np
from astropy.table import Table

#==============================================================================
# HUB -- Generate a SAMP hub to manage communications
#==============================================================================
class Hub(object):
    """ 
    This class is a very minimalistic class that provides a working SAMP hub. Usage:
    ::
    
        > h = Hub()

        > h.stop()
    """
    def __init__(self,addr, *args, **kwargs):
        self.SAMPHubServer=sampy.SAMPHubServer(addr=addr, *args, **kwargs)
        atexit.register(self.SAMPHubServer.stop)

    def __del__(self):
        self.SAMPHubServer.stop()

#==========================================================================
# Client -- Generate a SAMP client able to send and receive data
#==========================================================================
class Client(object):
    """
    This class implement an interface to SAMP applications like Topcat and
    Aladin using Sampy module.It allows you to exchange tables with topcat 
    using the SAMP system.

    To instanciate a connection
    ::
    
        # (Start Topcat!)
        from sampc import Client
        c = Client()

    To send data to Topcat
    ::
    
        # Send array / list / dict
        arr = np.random.random([100,3])
        lis = [5,6,10,20,50]
        dic = {'x':[1,2,3,4],'y':['hey','lift','your','head']}
        c.send(arr,'arr')
        c.send(lis,'lis')
        c.send(dic,'dic')
        # Send an array with custom colum names
        c.send(arr,'arrcust',cols=['x','y','z'])
        # Send an astropy table
        from astropy.table import Table
        tab = Table(np.array([[5,20],[6,28]]),names=['mass','temp'])
        c.send(tab,'tab')
        # Send data using client setitem notation
        c['stab1'] = a
        c['stab2',['x','y','z']] = a

    To receive a table from Topcat
    ::
    
        # (broadcast the table from Topcat first!)
        data = c['SomeTable.fits']
        # Property c.tables holds a record of exchanged tables
        print c.tables
    
    Note by pressing c + TAB, the last SAMP message received will print to console
    
    To send rows to Topcat
    ::
    
        c.sendrows('a',15)
        c.sendrows('a',[3,6,15,16])

    To receive rows/subsets from Topcat
    ::

        # (Broadcast row in Topcat)
        c.crow
        # (Broadcast some rows or a subset in Topcat)
        c.rowlist

    To print information about client and tables
    ::
    
        c()
        print c.tables

    To disconnect the client gracefuly use any of these
    ::
    
        c.off()
        c.client.disconnect()

    To reconnect a client previously instanciated
    ::
    
        c.on()
        c.client.connect()

    When receiving tables, note they are not read into the python 
    session until actually beeing accessed, as in ``data=c['sometable.fits']``
    """
    # Destructor ==============================================================
    def __del__(self):
        self.client.disconnect()
        if self.hub != None:  self.hub.stop()

    # Constructor =============================================================
    def __init__(self, addr='localhost', hub=True):
        # Before we start, let's kill off any zombies
        if hub:
            # sampy seems to fall over sometimes if 'localhost' isn't specified
            # even though it shouldn't
            self.hub = sampy.SAMPHubServer(addr=addr)
            self.hub.start()
        else:
            self.hub = None

        self.metadata = {
                            'samp.name' : 'Python Session',
                            'samp.icon.url' :'http://docs.scipy.org/doc/_static/img/scipyshiny_small.png',
                            'samp.description.text' : 'Python Samp Module',
                            'client.version' : '0.1a'
                        }
        
        self.client = sampy.SAMPIntegratedClient(metadata=self.metadata, addr=addr)
        self.client.connect()
        atexit.register(self.client.disconnect)
        
    	  # Bind interaction functions - we will register that we want to listen
    	  # for table.highlight.row (the SAMP highlight protocol), and all the
    	  # typical SAMP-stuff We could include this to listen for other sorts of
    	  # subscriptions like pointAt(), etc.
        #self.client.bindReceiveNotification('table.highlight.row', self.highlightRow)
        #self.client.bindReceiveNotification('table.select.rowList', self.getrows)
        self.client.bindReceiveNotification('table.highlight.row', self.getrows)
        self.client.bindReceiveNotification('table.load.fits', self.receiveNotification)
        self.client.bindReceiveNotification('samp.app.*', self.receiveNotification)
        
        self.client.bindReceiveCall('samp.app.*', self.receiveCall)
        self.client.bindReceiveCall('table.load.fits', self.receiveCall)
        self.client.bindReceiveCall('table.select.rowList', self.receiveCall)
       
        self.tables  = {}
        self.rowlist = {}
        self.crow    = None
        self.lastMessage = None

        tmpdir = os.getenv('HOME') + '/tempo/samptables'
        if not os.path.exists(tmpdir):
            os.makedirs(tmpdir)
        else:
            for f in os.listdir(tmpdir):
                os.remove(tmpdir + '/' + f)   # maybe ask first before deleting ?
        self.tmpdir = tmpdir


    # Generic response protocols ==============================================
    def receiveNotification(self, privateKey, senderID, mType, params, extra):
        print '[SAMP] Notification ', privateKey, senderID, mType, params, extra
        self.lastMessage = {'label':'Notification',
                            'privateKey':privateKey, 
                            'senderID': senderID, 
                            'mType': mType, 
                            'params': params, 
                            'extra': extra }
		
    def receiveResponse(self, privateKey, senderID, msgID, response):
        print '[SAMP] Response ', privateKey, senderID, msgID, response
        
    def receiveCall(self, privateKey, senderID, msgID, mType, params, extra):
        data = { 'privateKey':privateKey, 'senderID':senderID, 'msgID':msgID, 'mType':mType, 'params':params}
        print '[SAMP] Call Received'
        if mType == 'table.load.fits':
            print "[SAMP] Table received."
            self.tables[params['name']]=data['params']
            #Uncomment to set a placeholder in case we want to store data in client.tables
            #self.tables[params['name']]['data'] = None 
        elif mType == 'table.select.rowList':
            self.getrows(privateKey, senderID, mType, params, extra)
        else:
            print '[SAMP] Call'
            print mType
        self.client.ereply(msgID, sampy.SAMP_STATUS_OK, result = {'txt' : 'printed' })

    def __getitem__(self, k):
        return self.get(k)	

    def __setitem__(self, k, data):
        """ Broadcast data to all applications """
        if isinstance(k,tuple) and isinstance(data,np.ndarray):
            self.send(data,k[0],cols=k[1],disc=False)
        else:
            self.send(data,k,disc=False)

    def __call__(self):
        """ print detailed info about the current client """

        for k in self.metadata.keys():
            print k,':', self.metadata[k]

        neighbours = self.getNeighbours()
        if len(neighbours) > 0:
            print "%d detected client(s):" % len(neighbours)
            for n in neighbours:
                print '\t'+self.client.getMetadata(n)['samp.name']
	
        print "Registered tables: %d" % len(self.tables)
        for k in self.tables.keys(): 
            #print '      %s' % k
            print '\t %s' % k

    # Application functions ===================================================
    def off(self):
        """ Just an alias to disconnect easily """
        self.__del__()

    def on(self):
        """ Just an alias to reconnect easily """
        self.client.connect()

    def get(self, k):
        """
        Get a table (that was previously broadcasted)
        
        Parameters
        ----------
        k : string
            Filepath of data table in fits format
        """
        if k in self.tables:
            cTab = self.tables[k]
            u = urllib.urlretrieve(cTab['url'])
            data = Table.read(u[0])
            # Uncomment this to store data in client.tables
            #cTab['data'] = data = Table.read(u[0])
            return data


    def send(self, data, k=None, cols=None, disc=False):
        """
        Broadcast data to all applications
        
        Parameters
        ----------
        data : numpy array, dict, list, or astropy table
            Data to be transfered
        k : string
            Name for data. A fits table will be saved in $HOME$/tempo/samptables/k.fits. **k** defaults to var.fits
        cols : list of strings
            Column names for data. If None, defaults to [col001,col002,...]
        disc : boolean
            If True, disconnects the client after sending the data
        """
	
        if k is None: k = 'var.fits'
        
        if k[-5:]!='.fits': k+='.fits'
        
        fullk = self.tmpdir + '/' + k   # full path to store data file
        
        if isinstance(data, np.ndarray):
            # Convert row vector to column if needed
            if data.ndim == 1: data = np.reshape(data,[-1,1])
            # Create strings col001,col002,... to name columns
            if cols is None:
                nc = data.shape[1]
                cols = ["col"+str(n).zfill(3) for n in range(nc)]
            # Save as astopy table in fits format
            Table(data=data,names=cols).write(fullk,format='fits',overwrite=True)
        elif isinstance(data,list):
            nr=len(data)
            if cols is None: cols=['col001']
            Table(data=np.array(data).reshape(nr,1),names=cols).write(fullk,format='fits',overwrite=True)
        elif isinstance(data,dict):
            Table(data=data.values(),names=data.keys()).write(fullk,format='fits',overwrite=True)
        elif isinstance(data,Table):
            data.write(fullk,format='fits',overwrite=True)

        # Build a class variable to keep track of all sent-received tables
        self.tables[k] = { 'name':k, 
                           'url': 'file://' + fullk
			          #'data': None #Table.read(fullk)
			        }

        # Do actual broadcasting
        print '[SAMP] Data broadcasted as table',k,'stored in',fullk
        r = self._broadcastTable(k) 

        # Disconnect when requested
        if disc: self.client.disconnect()
        return 
   
    def getNeighbours(self):
        """ Returns print information about the current client """
        return self.client.getRegisteredClients()
       
    def getAppId(self, name):
        """ Returns the registered Id of a given application name"""
        neighbours = self.client.getRegisteredClients()
        for neighbour in neighbours:
            metadata = self.client.getMetadata(neighbour)
            try:
                if (metadata['samp.name'] == name):
                    return neighbour
            except KeyError:
                continue

    def isAppRunning(self, name):
        """ 
        Check if a given application is running and registered to the SAMP server
        """
        neighbours = self.client.getRegisteredClients()
        
        for neighbour in neighbours:
            metadata = self.client.getMetadata(neighbour)
            try:
                if (metadata['samp.name'] == name):
                    self.topcat = neighbour
                    return True
            except KeyError:
                continue

        return False
        
    def _broadcastTable(self, table, to='All'):
        """ 
        Do actual broadcasting of a SAMP message instructing to load a fits table
        
        Parameters
        ----------
        table : string
            Filename (including .fits) for data file. The complete file url is
            constructed as $HOME$/tempo/samptables/table.fits
        """ 
        metadata = {
                    'samp.mtype' : 'table.load.fits',
                    'samp.params' : {
                                     'name' : table,
                                     'table-id' : table,
                                     'url' : 'file://' + self.tmpdir + '/' + table
                                    }
                   }
                   
        if len(self.client.getRegisteredClients()) > 0:
            if to.lower() == 'all':
                return self.client.notifyAll(metadata)
                #return self.client.callAndWait(metadata)
            else:
                #if self.isAppRunning(name): 
                if self.isAppRunning(to): 
                    #return self.client.callAndWait(self.getAppId(to), metadata, 3)
                    return self.client.notify(self.getAppId(to), metadata)
                    #qq=kund.client.callAndWait('c1',message,"10")
        else: 
            return False
    
    
    def getrows(self, privateKey, senderID, mType, params, extra):
        """ 
        Receive a single row or list of rows and store in a class property variable 
        
        Use **client.crow** or **client.rowlist** to access the indices of 
        **previously broadcasted** rows 
        """
        filen = params['url']
        if mType == 'table.highlight.row':
            idx   = np.intp(params['row'])
            print '[SAMP] Selected row %s from %s' % (idx,filen)
            print '[SAMP] Row index stored in property -> crow'
            self.crow = idx
        elif mType == 'table.select.rowList':
            idx   = np.intp(params['row-list'])
            print '[SAMP] Selected %s rows from %s' % (len(idx),filen)
            print '[SAMP] List stored in property -> rowlist'
            self.rowlist = idx
        
        self.lastMessage = {'label':'Selected Rows',
                            'privateKey':privateKey, 
                            'senderID': senderID, 
                            'mType': mType, 
                            'params': params, 
                            'extra': extra }        

    def sendrows(self, table, idx, to='All'):
        """ 
        Send a list of row indices 
        
        Parameters
        ----------
        table : string
            Filename (including .fits) for data file. The complete file url  is
            constructed as $HOME$/tempo/samptables/table.fits
        idx : list/tuple/ndarray
            Rows to send
        
        If a single row is broadcasted, the message mtype is ``table.highlight row``
        If multiple rows are broadcasted, the message mtype is ``table.select.rowList``
        """
        if table[-5:]!='.fits': table+='.fits'  # add extension
        fullt = self.tmpdir + '/' + table       # full path to store data file
        if not os.path.exists(fullt):           # check if file is there
            print 'Error :', fullt, 'not found !'
            return
        
        if isinstance(idx, (list, tuple, np.ndarray)):
            mtype = 'table.select.rowList'
            rnam = 'row-list'
            rval = [str(i) for i in idx]
            statusmsg = '[SAMP] Sent ' + str(len(idx)) + ' rows to table ' + fullt
        else:
            mtype = 'table.highlight.row'
            rnam = 'row'
            rval = str(idx)
            statusmsg = '[SAMP] Sent row ' + str(idx) + ' to table ' + fullt
        
        metadata = {
                    'samp.mtype' : mtype,
                    'samp.params' : {
                                     'name' : table,
                                     'table-id' : table,
                                     'url' : 'file://' + fullt,
                                     rnam: rval
                                    }
                   }
                   
        if len(self.client.getRegisteredClients()) > 0:
            print statusmsg
            if to.lower() == 'all':
                self.client.notifyAll(metadata)
                return
                #return self.client.callAndWait(metadata)
            else:
                if self.isAppRunning(to): 
                    self.client.notify(self.getAppId(to), metadata)
                    return 
                    #return self.client.callAndWait(self.getAppId(to), metadata, 3)
        else: 
            return False

