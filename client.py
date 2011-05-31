#!/usr/bin/env python
# encoding: UTF-8

import os.path

import sys
from PyQt4 import QtGui, QtCore, uic, QtSql
from bjsonrpc import connect

class QSqlLlampexResult(QtSql.QSqlResult):
    
    def __init__(self, driver):
        QtSql.QSqlResult.__init__(self,driver)
        self.sqldriver = driver
        
        self.currentSize = 0
    
    def data(self,i):
        print "data"
        return self.sqldriver.getData(i,self.at())
        #return Qvariant
    
    def isNull(self,index):
        print "isNull"
        pass #return bool
    
    def reset(self,query):
        print query
        if self.sqldriver.executeQuery(query):
            self.setActive(True)
            if str(query).strip().lower().startswith("select"):
                self.setSelect(True)
                self.currentSize = self.sqldriver.getCurrentSize()
            else:
                self.setSelect(False)
                self.currentSize = -1;
            return True
        else:
            #TODO no imprime nada :S
            self.setLastError(QtSql.QSqlError("QSqlLlampexDriver: QSqlLlampexResult", "Unable to create query", QtSql.QSqlError.StatementError))
            return False
    
    def fetch(self,i):
        print "fetch: %d" % i
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
        print "fetchFirst"
        return self.fetch(0)
    
    def fetchLast(self):
        print "fetchLast"
        return self.fetch(self.currentSize -1)
    
    def size(self):
        print "size"
        return self.currentSize
    
    def numRowsAffected(self):
        print "numRowsAffected"
        pass #return int
    
    def record(self):
        print "record"
        info = QtSql.QSqlRecord()
        if not self.isActive() or not self.isSelect():
            return info
        
        for i in range(self.currentSize):
            f = QtSql.QSqlField(str(self.data(i)),QtCore.QVariant.String) #TODO arreglar TYPE
            info.append(f)
        
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
        #setattr(self,"open",self.open_)
        
    def hasFeature(self,f):
        print "hasFeature", f
        return False
    
    def open(self,db,user,passwd,host,port,options):
        print "open"
        ok = self.c.call.openDB()
        self.setOpen(ok)
        self.setOpenError(not ok)
        return ok
        
    def close(self):
        print "close"
        self.c.call.closeDB()
        self.setOpen(False)
        self.setOpenError(False)
    
    def createResult(self):
        print "result"
        return QSqlLlampexResult(self)
        
    def executeQuery(self,query):
        print "execute query"
        return self.c.call.execute(unicode(query))
        
    def getCurrentSize(self):
        print "current size"
        return self.c.call.rowcount()
        
    def getCursorPosition(self):
        print "getPosition"
        return self.c.call.rownumber()
        
    #def setAt(self,i):
    #    print "setAt"
    #    print self.c.call.scroll(i,'absolute')
        
    def fetchOne(self):
        print "fetch one"
        return self.c.call.fetchOne()
        
    def getData(self, i, row):
        print "getData:"+str(i)+" "+str(row)
        print self.c.call.scroll(row,'absolute')
        self.fetchOne()
        return self.c.call.getData(i)
    
def initializeModel(model,db):
    model.setQuery('select * from users',db)
    model.setHeaderData(0, QtCore.Qt.Horizontal, "ID")
    model.setHeaderData(1, QtCore.Qt.Horizontal, "Username")
    model.setHeaderData(2, QtCore.Qt.Horizontal, "Password")
    model.setHeaderData(3, QtCore.Qt.Horizontal, "Active")
    model.setHeaderData(4, QtCore.Qt.Horizontal, "Admin")
    
def createView(title, model):
    view = QtGui.QTableView()
    view.setModel(model)
    view.setWindowTitle(title)
    view.show()
    
if __name__ == '__main__':
    import sys
    app = QtGui.QApplication(sys.argv)
    
    global llampex_driver, db
    print
    print "*** Testing Llampex Qt Database Driver..."
    llampex_driver = QSqlLlampexDriver()
    print llampex_driver
    db = QtSql.QSqlDatabase.addDatabase(llampex_driver, "myconnection")
    print "Database:",db
    
    if not db.open():
        print "unable to open database"
        sys.exit(1)
        
    #plainModel = QtSql.QSqlQueryModel()
    #initializeModel(plainModel,db)
    #createView("Plain Query Model", plainModel)
    
    query = QtSql.QSqlQuery("select * from users",db)
    #query = QtSql.QSqlQuery("insert into users VALUES (7,'pepito','password','false','false')",db)
    while query.next():
        print "next!"
        print "-----------------> "+query.value(1).toString()
    
    
    
    #db.close()
    del db
    QtSql.QSqlDatabase.removeDatabase("myconnection")