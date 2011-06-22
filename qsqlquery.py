#!/usr/bin/env python
# encoding: UTF-8

import os.path

import sys
from PyQt4 import QtGui, QtCore, uic, QtSql

from bjsonrpc import connect

import clientoptions
import qsqlrpc


    
if __name__ == '__main__':
    import sys
    app = QtGui.QApplication(sys.argv)
    
    #global llampex_driver, db
    #print
    #print "*** Testing Llampex Qt Database Driver..."
    #
    #llampex_driver = qsqlrpc.QSqlLlampexDriver()
    #print llampex_driver
    #db = QtSql.QSqlDatabase.addDatabase(llampex_driver, "myconnection")
    #db.setDatabaseName("laperla")
    #db.setUserName("llampexuser")
    #db.setPassword("llampexpasswd")
    #db.setHostName("127.0.0.1")
    #db.setPort(10123)
    #
    #print "Database:",db
    #
    #if not db.open():
    #    print "unable to open database"
    #    sys.exit(1)
    
    clientoptions.prepareParser()
    qsqlrpc.DEBUG_MODE = clientoptions.getDebug()
    db = clientoptions.getDB()
    
    query = QtSql.QSqlQuery("select * from "+clientoptions.getTable(),db)

    #query = QtSql.QSqlQuery("insert into users VALUES (7,'pepito','password','false','false')",db)
    while query.next():
        print "next!"
        print "-----------------------------------------> "+query.value(0).toString()
    
    #db.close()
    del query
    clientoptions.db = None
    del db
    QtSql.QSqlDatabase.removeDatabase("myconnection")
    

