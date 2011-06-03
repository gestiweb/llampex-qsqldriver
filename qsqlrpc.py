#!/usr/bin/env python
# encoding: UTF-8

from PyQt4 import QtGui, QtCore, uic, QtSql
from bjsonrpc import connect

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


class QSqlLlampexResult(QtSql.QSqlResult):
    
    def __init__(self, driver):
        QtSql.QSqlResult.__init__(self,driver)
        self.sqldriver = driver
        
        self.currentSize = 0
    
    def data(self,i):
        ret = self.sqldriver.getData(i,self.at())
        print "$$ -> LlampexResult.data(%d) -> %s" % (i,repr(ret))
        return ret
        
        #return Qvariant
    
    def isNull(self,index):
        print "$$ -> LlampexResult.isNull(%d)" % (index)
        return False
    
    def reset(self,query):
        print "$$ -> LlampexResult.reset(%s)" % (repr(query))
        if self.sqldriver.executeQuery(query):
            self.setActive(True)
            if str(query).strip().lower().startswith("select"):
                self.setSelect(True)
                self.currentSize = self.sqldriver.getCurrentSize()
            else:
                self.setSelect(False)
                self.currentSize = -1;
                self.sqldriver.commitTransaction()
            return True
        else:
            #TODO no imprime nada :S
            print "$$ Error: Unable to create query"
            self.setLastError(QtSql.QSqlError("QSqlLlampexDriver: QSqlLlampexResult", "Unable to create query", QtSql.QSqlError.StatementError))
            return False
    
    def fetch(self,i):
        print "$$ -> LlampexResult.fetch(%d)" % (i)
        if not self.isActive():
            return False
        if i < 0:
            return False
        if i >= self.currentSize:
            return False
        if self.at() == i:
            return True
        
        self.setAt(i)
        return True
    
    def fetchFirst(self):
        print "$$ -> LlampexResult.fetchFirst()"
        return self.fetch(0)
    
    def fetchLast(self):
        print "$$ -> LlampexResult.fetchLast()"
        return self.fetch(self.currentSize -1)
    
    def size(self):
        print "$$ -> LlampexResult.size() -> %d" % (self.currentSize)
        return self.currentSize
    
    def numRowsAffected(self):
        print "$$ -> LlampexResult.numRowsAffected()"
        return 0
    
    def record(self):
        print "$$ -> LlampexResult.record() (crear QSqlRecord())"
        info = QtSql.QSqlRecord()
        if not self.isActive() or not self.isSelect():
            return info
        
        global pg2qttype
        for field in self.sqldriver.fields:
            f = QtSql.QSqlField(field[0],pg2qttype[field[1]])
            info.append(f)
            print "$$ -> field:", field
        
        print "$$ <<< LlampexResult.record()"
        return info
    

class LlampexDriverPrivate(object):
    def __init__(self, conn = None):
        print "hello world from private"

class QSqlLlampexDriver(QtSql.QSqlDriver):
    """def __new__(cls, *args):
        self = super(QSqlLlampexDriver, cls).__new__(cls, *args)
        print "NEW:", repr(self)
        return self
    """    
    def __init__(self, *args):
        QtSql.QSqlDriver.__init__(self)
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
        
        self.p = LlampexDriverPrivate(conn = conn)
        self.c = connect()
        self.c = self.c.call.getCursor()
        #setattr(self,"open",self.open_)
        
    def hasFeature(self,f):
        print "~~ hasFeature: "+str(f)
        if f == 0: return True
        if f == 1: return True
        if f == 4: return True
        return False
    
    def open(self,db,user,passwd,host,port,options):
        print "~~ open database"
        ok = self.c.call.openDB()
        self.setOpen(ok)
        self.setOpenError(not ok)
        return ok
        
    def close(self):
        print "~~ close"
        self.c.call.closeDB()
        self.setOpen(False)
        self.setOpenError(False)
    
    def createResult(self):
        print "~~ createResult"
        return QSqlLlampexResult(self)
        
    def executeQuery(self,query):
        print "~~ execute query:", query
        ret = self.c.call.execute(unicode(query))
        self.fields = self.c.call.fields()
        return ret
        
    def getCurrentSize(self):
        size = self.c.call.rowcount()
        print "~~ current size -> %d " % size
        return self.c.call.rowcount()
        
    def getCursorPosition(self):
        print "~~ getPosition"
        return self.c.call.rownumber()
        
    def fetchOne(self):
        print "~~ fetch one"
        self.c.call.fetchOne()
        
    def getData(self, i, row):
        print "~~ getData:"+str(i)+" | "+str(row)
        self.c.call.scroll(row,'absolute')
        self.fetchOne()
        return self.c.call.getData(i)
        
    def beginTransaction(self):
        print "~~ beginTransaction"
        if self.isOpen():
            return True
        else:
            print "Error: Database Not Open"
            return False    
        
    def commitTransaction(self):
        print "~~ commitTransaction"
        try:
            self.c.call.commit()
            return True
        except:
            return False
    
    def record(self,tableName):
        print "~~ record (driver) of "+tableName
        
        self.executeQuery("SELECT * FROM "+tableName)
        
        info = QtSql.QSqlRecord()
        
        global pg2qttype
        for field in self.fields:
            f = QtSql.QSqlField(field[0],pg2qttype[field[1]])
            info.append(f)
            print "$$ -> field:", field
        
        print "$$ <<< LlampexResult.record()"
        return info
    
    def formatValue(self,field,trimStrings):
        print "~~ formatVaule"
        
        r = super(QSqlLlampexDriver,self).formatValue(field,trimStrings)
        
        if field.type() == QtCore.QVariant.Bool:
            if field.value().toBool():
                r = "TRUE"
            else:
                r = "FALSE"
        
        #TODO Test other tipes to change
        
        return r
                
        