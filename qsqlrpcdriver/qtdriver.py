#!/usr/bin/env python
# encoding: UTF-8

from PyQt4 import QtGui, QtCore, uic, QtSql
from bjsonrpc import connect
from bjsonrpc.connection import RemoteObject
import time

pg2qttype = {}
pg2qttype["bit"] = QtCore.QVariant.BitArray
pg2qttype["varbit"] = QtCore.QVariant.BitArray
pg2qttype["bool"] = QtCore.QVariant.Bool
pg2qttype["bytea"] = QtCore.QVariant.ByteArray
pg2qttype["date"] = QtCore.QVariant.Date
pg2qttype["abstime"] = QtCore.QVariant.DateTime
pg2qttype["reltime"] = QtCore.QVariant.DateTime
pg2qttype["timestamp"] = QtCore.QVariant.DateTime
pg2qttype["timestamptz"] = QtCore.QVariant.DateTime
pg2qttype["numeric"] = QtCore.QVariant.Double
pg2qttype["float4"] = QtCore.QVariant.Double
pg2qttype["float8"] = QtCore.QVariant.Double
pg2qttype["tinterval"] = QtCore.QVariant.Double
pg2qttype["interval"] = QtCore.QVariant.Double
pg2qttype["money"] = QtCore.QVariant.Double
pg2qttype["int2"] = QtCore.QVariant.Int
pg2qttype["int4"] = QtCore.QVariant.Int
pg2qttype["int8"] = QtCore.QVariant.Int # este es de 64 bits
pg2qttype["point"] = QtCore.QVariant.Point
pg2qttype["char"] = QtCore.QVariant.String
pg2qttype["name"] = QtCore.QVariant.String
pg2qttype["text"] = QtCore.QVariant.String # o puede que StringList
pg2qttype["xml"] = QtCore.QVariant.String
pg2qttype["path"] = QtCore.QVariant.String
pg2qttype["varchar"] = QtCore.QVariant.String
pg2qttype["time"] = QtCore.QVariant.Time
pg2qttype["timetz"] = QtCore.QVariant.Time
pg2qttype["uuid"] = QtCore.QVariant.ULongLong

QtDriverFeature={
    0 : "QSqlDriver::Transactions	0	Whether the driver supports SQL transactions.",
    1 : "QSqlDriver::QuerySize	1	Whether the database is capable of reporting the size of a query. Note that some databases do not support returning the size (i.e. number of rows returned) of a query, in which case QSqlQuery::size() will return -1.",
    2 : "QSqlDriver::BLOB	2	Whether the driver supports Binary Large Object fields.",
    3 : "QSqlDriver::Unicode	3	Whether the driver supports Unicode strings if the database server does.",
    4 : "QSqlDriver::PreparedQueries	4	Whether the driver supports prepared query execution.",
    5 : "QSqlDriver::NamedPlaceholders	5	Whether the driver supports the use of named placeholders.",
    6 : "QSqlDriver::PositionalPlaceholders	6	Whether the driver supports the use of positional placeholders.",
    7 : "QSqlDriver::LastInsertId	7	Whether the driver supports returning the Id of the last touched row.",
    8 : "QSqlDriver::BatchOperations	8	Whether the driver supports batched operations, see QSqlQuery::execBatch()",
    9 : "QSqlDriver::SimpleLocking	9	Whether the driver disallows a write lock on a table while other queries have a read lock on it.",
    10: "QSqlDriver::LowPrecisionNumbers	10	Whether the driver allows fetching numerical values with low precision.",
    11: "QSqlDriver::EventNotifications	11	Whether the driver supports database event notifications.",
    12: "QSqlDriver::FinishQuery	12	Whether the driver can do any low-level resource cleanup when QSqlQuery::finish() is called.",
    13: "QSqlDriver::MultipleResultSets	13	Whether the driver can access multiple result sets returned from batched statements or stored procedures.",
}
DEBUG_MODE = False

CACHE_BLOCK_SIZE = 10
import json

class QSqlLlampexResult(QtSql.QSqlResult):
    """
        Result Class for SqlLlampexDriver.
        Is the equivalent of a cursor, and is connected with the server.
    """
    
    def __init__(self, driver):
        QtSql.QSqlResult.__init__(self,driver)
        self.sqldriver = driver
        self.cur = self.sqldriver.conn.call.getCursor()
        self.setup()
    
    def setup(self):
        self.cache = {}
        self.lastRowNumber = None
        self.lastRowObject = None
        self.cacheQueue = []
        self._currentSize = None
    
    def data(self,i):
        # This function never gets called. it is replaced everytime a new row is queried.
        return None
    """
    def data(self,i):
        
        ret = self.getData(self.at()) # TODO: Hacer desaparecer el self.at() porque la implementación del driver no lo requiere.
        print "$$ -> LlampexResult.data(%d) -> %s" % (i,repr(ret))
        return ret[i]
    """    
    def cacheBlock(self, k):
        if k in self.cache: return self.cache[k]
        if DEBUG_MODE:
            print "$$ -> LlampexResult.cacheBlock(%d) " % (k)
        retrange = self.cur.method.getDataAtRange(k*CACHE_BLOCK_SIZE,CACHE_BLOCK_SIZE)
        self.cache[k] = retrange
        self.cacheQueue.append(k)
        sz = len(self.cacheQueue)
        if sz > 3000:
            poplist = self.cacheQueue[:10]
            self.cacheQueue[:10] = []
            for p in poplist:
                del self.cache[p]
            # print poplist
        return retrange
        
    def getData(self, row):
        if self.lastRowNumber != row:
            k = int(row/CACHE_BLOCK_SIZE)
            l = row%CACHE_BLOCK_SIZE
            retrange = self.cacheBlock(k)
            # -- read ahead --
            if l >= CACHE_BLOCK_SIZE:
                self.cacheBlock(k+3)
            elif l >= (3*CACHE_BLOCK_SIZE) / 4:
                self.cacheBlock(k+2) 
            elif l >= CACHE_BLOCK_SIZE / 2:
                self.cacheBlock(k+1) 
            # -- read ahead --
            try:    
                self.lastRowObject = retrange.value[l]
            except IndexError:
                return None
            self.lastRowNumber = row
            self.data = self.lastRowObject.__getitem__
            
        return self.lastRowNumber
        
    
    def isNull(self,index):
        if DEBUG_MODE:
            print "$$ -> LlampexResult.isNull(%d)" % (index) 
        return False
    
    @property
    def currentSize(self):
        if self._currentSize is None:
            if DEBUG_MODE:
                print "$$ -> LlampexResult.currentSize() /*QUERY*/"
            self._currentSize = self.cur.call.rowcount()
        return self._currentSize
        
    def reset(self,query):
        """This method is called when do a new query. Check if is a
        SELECT or not, and consequently prepares the result class"""
        if DEBUG_MODE:
            print "$$ -> LlampexResult.reset(%s)" % (repr(query))
        self.setup()
        
        isExecuted = False
        if unicode(query).strip().lower().startswith("select"):
            if self.cur.call.executeSelect(unicode(query)):
                self.setSelect(True)
                self._currentSize = None;
                # self.cur.call.rowcount()
                isExecuted = True
        else:
            if self.cur.call.execute(unicode(query)):
                self.setSelect(False)
                self._currentSize = -1;
                self.cur.call.commit()
                isExecuted = True
        
        if isExecuted:
            self.fields = self.cur.call.fields()
            self.setActive(True)
        else:
            #TODO no imprime nada :S
            if DEBUG_MODE:
                print "$$ Error: Unable to create query"
            self.setLastError(QtSql.QSqlError("QSqlLlampexDriver: QSqlLlampexResult", "Unable to create query", QtSql.QSqlError.StatementError))
        
        return isExecuted
        
        #if self.cur.call.execute(unicode(query)):
        #    self.fields = self.cur.call.fields()
        #    self.setActive(True)
        #    if unicode(query).strip().lower().startswith("select"):
        #        self.setSelect(True)
        #        self.currentSize = self.cur.call.rowcount()
        #    else:
        #        self.setSelect(False)
        #        self.currentSize = -1;
        #        self.cur.call.commit()
        #    return True
        #else:
        #    #TODO no imprime nada :S
        #    if DEBUG_MODE:
        #        print "$$ Error: Unable to create query"
        #    self.setLastError(QtSql.QSqlError("QSqlLlampexDriver: QSqlLlampexResult", "Unable to create query", QtSql.QSqlError.StatementError))
        #    return False
    
    def fetch(self,i):
        if self.lastRowNumber == i: return True
        #if DEBUG_MODE:
        #    print "$$ -> LlampexResult.fetch(%d)" % (i)
        # TODO: Los cursores se pueden posicionar desde BOF hasta EOF ambos inclusive. Probablemente fetch deba reflejar esto.
        if not self.isActive():
            self.data = None
            return False
        if i < 0:
            self.data = None
            return False
        if i >= self.currentSize:
            self.data = None
            return False
        if self.at() == i:
            return True
        if self.getData(i) is not None:
            self.setAt(i)
            return True
        return False
    
    def fetchFirst(self):
        if DEBUG_MODE:
            print "$$ -> LlampexResult.fetchFirst()"
        return self.fetch(0)
    
    def fetchLast(self):
        if DEBUG_MODE:
            print "$$ -> LlampexResult.fetchLast()"
        return self.fetch(self.currentSize -1)
    
    def size(self):
        #if DEBUG_MODE:
        #    print "$$ -> **** LlampexResult.size() *** -> %d" % (self.currentSize)
        return self.currentSize
    
    def numRowsAffected(self):
        if DEBUG_MODE:
            print "$$ -> LlampexResult.numRowsAffected()"
        return 0
    
    def record(self):
        """This method return in a QSqlRecord the fields (rows) of the last query"""
        if DEBUG_MODE:
            print "$$ -> LlampexResult.record() (crear QSqlRecord())"
        info = QtSql.QSqlRecord()
        if not self.isActive() or not self.isSelect():
            return info
        
        global pg2qttype
        for field in self.fields:
            f = QtSql.QSqlField(field[0],pg2qttype[field[1]])
            info.append(f)
            #if DEBUG_MODE:
            #    print "$$ -> field:", field
        
        #if DEBUG_MODE:
        #    print "$$ <<< LlampexResult.record()"
        return info
    

class QSqlLlampexDriver(QtSql.QSqlDriver):
    """QSqlLlampexDriver is a special QSqlDriver that acts as an intermediary
    with the help of bjsonrpc between QSql (in client) and the Llampex Server"""
    
    def __init__(self, *args):
        """Driver constructor. Can recieve an existing connection"""
        
        QtSql.QSqlDriver.__init__(self)
        if DEBUG_MODE:
            print "init", args
        # emulate Cpp-style function overload.
        # a) None
        # b) (QObject*) parent 
        # c) (RPCConn*) conn , (QObject*) parent
        
        parent = None
        conn = None
        for arg in args:
            if isinstance(arg, QtCore.QObject): 
                assert(parent is None)
                parent = arg
            else:
                assert(conn is None)
                conn = arg
        
        self.conn = conn
        self.features = {}
        
        
    def hasFeature(self,f):
        """Check if the driver has a specific QSqlDriver feature"""
        if f not in self.features: 
            self.features[f] = self._hasFeature(f)
        
        return self.features[f]
        
        
    def _hasFeature(self,f):
        if DEBUG_MODE:
            print "~~ hasFeature:",QtDriverFeature.get(f,f)
        if f == QtSql.QSqlDriver.Transactions: return True
        elif f == QtSql.QSqlDriver.QuerySize: return True
        elif f == QtSql.QSqlDriver.BLOB: return False #?
        elif f == QtSql.QSqlDriver.Unicode: return True #?
        elif f == QtSql.QSqlDriver.PreparedQueries: return False
        elif f == QtSql.QSqlDriver.NamedPlaceholders: return False #?
        elif f == QtSql.QSqlDriver.PositionalPlaceholders: return False #?
        elif f == QtSql.QSqlDriver.LastInsertId: return False #?
        elif f == QtSql.QSqlDriver.BatchOperations: return False
        elif f == QtSql.QSqlDriver.SimpleLocking: return False
        elif f == QtSql.QSqlDriver.LowPrecisionNumbers: return False
        elif f == QtSql.QSqlDriver.EventNotifications: return False
        elif f == QtSql.QSqlDriver.FinishQuery: return False
        elif f == QtSql.QSqlDriver.MultipleResultSets: return False
        else:
            raise ValueError
        return False
    
    def open(self,db,user,passwd,host,port,options):
        if DEBUG_MODE:
            print "~~ open database"

        ok = True
        
        if self.conn is None:
            if not host is None and not port is None:
                self.conn = connect(host,port)
            else:
                if DEBUG_MODE:
                    print "~~ Error opening: You must indicate host and port"
                ok = False
        
        if not isinstance(self.conn, RemoteObject):
            if db is not None and user is not None and passwd is not None:
                if not self.conn.call.login(unicode(user),unicode(passwd),unicode(db)):
                    if DEBUG_MODE:
                        print "~~ Error connecting: User, password or project are incorrectly"
                    ok = False                
            else:
                if DEBUG_MODE:
                    print "~~ Error opening: You must indicate db, user and password"
                ok = False
        
        self.setOpen(ok)
        self.setOpenError(not ok)
        
        return ok
        
    def close(self):
        if DEBUG_MODE:
            print "~~ close"
        self.setOpen(False)
        self.setOpenError(False)
    
    def createResult(self):
        """Returns a new QSqlLlampexResult (a special result to improve
        the Driver, see its Documentation"""
        
        if DEBUG_MODE:
            print "~~ createResult"
        return QSqlLlampexResult(self)
        
    def beginTransaction(self):
        if DEBUG_MODE:
            print "~~ beginTransaction"
        if self.isOpen():
            return True
        else:
            if DEBUG_MODE:
                print "Error: Database Not Open"
            return False    
        
    def commitTransaction(self):
        if DEBUG_MODE:
            print "~~ commitTransaction"
        try:
            self.conn.call.commit()
            return True
        except:
            return False
    
    def record(self,tableName):
        """Method that get a tableName and return it fields(rows)"""
        
        if DEBUG_MODE:
            print "~~ -> LlampexDriver.record() of "+tableName
            
        tmpCursor = QSqlLlampexResult(self)
        tmpCursor.reset("SELECT * FROM "+tableName)
        return tmpCursor.record()
    
    def formatValue(self,field,trimStrings):
        """Method for format variables to automatically create SQL Queries"""
        
        if DEBUG_MODE:
            print "~~ formatVaule"
        
        r = super(QSqlLlampexDriver,self).formatValue(field,trimStrings)
        
        if field.type() == QtCore.QVariant.Bool:
            if field.value().toBool():
                r = "TRUE"
            else:
                r = "FALSE"
        
        #TODO Test other types to change
        
        return r
                