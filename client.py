#!/usr/bin/env python
# encoding: UTF-8

import os.path

import sys
from PyQt4 import QtGui, QtCore, uic, QtSql

from bjsonrpc import connect

import qsqlrpc



    
if __name__ == '__main__':
    import sys
    app = QtGui.QApplication(sys.argv)
    
    global llampex_driver, db
    print
    print "*** Testing Llampex Qt Database Driver..."
    
    conn = connect()
    llampex_driver = qsqlrpc.QSqlLlampexDriver(conn)
    print llampex_driver
    db = QtSql.QSqlDatabase.addDatabase(llampex_driver, "myconnection")
    print "Database:",db
    
    if not db.open():
        print "unable to open database"
        sys.exit(1)
    
    query = QtSql.QSqlQuery("select * from users",db)
    #query = QtSql.QSqlQuery("insert into users VALUES (7,'pepito','password','false','false')",db)
    while query.next():
        print "next!"
        print "-----------------------------------------> "+query.value(1).toString()
    
    
    
    #db.close()
    del db
    QtSql.QSqlDatabase.removeDatabase("myconnection")