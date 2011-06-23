#!/usr/bin/env python
# encoding: UTF-8

from optparse import OptionParser
from PyQt4 import QtGui, QtCore, uic, QtSql
import qsqlrpc

parser = OptionParser()
llampex_driver = None
db = None

def prepareParser():
    global parser
    parser.add_option("-d", "--dbname", dest="dbname", default="llampex",
                      help="DB name to connect and get results. Default '%default'.")
    
    parser.add_option("-u", "--user", dest="user", default="llampexuser",
                  help="Valid user to connect DB. Default '%default'.")
    
    parser.add_option("-p", "--password", dest="password", default="llampexpasswd",
                  help="Valid password to connect DB. Default '%default'.")
    
    parser.add_option("-t", "--table", dest="table", default="users",
                  help="Valid table to get results. Default '%default'.")
    
    parser.add_option("-D", "--debug", dest="debug", default=False,
                      action="store_true",
                  help="To print debug messages of driver.")
    
def getDB():
    #TODO Solve this warning: QSqlDatabasePrivate::removeDatabase: connection 'myconnection' is still in use, all queries will cease to work.
    global parser
    global llampex_driver, db
    (options, args) = parser.parse_args()
    
    llampex_driver = qsqlrpc.QSqlLlampexDriver()

    db = QtSql.QSqlDatabase.addDatabase(llampex_driver, "myconnection")
    
    db.setDatabaseName(options.dbname)
    db.setUserName(options.user)
    db.setPassword(options.password)
    db.setHostName("127.0.0.1")
    db.setPort(10123)
    
    if not db.open():
        print "Unable to open database"
        parser.print_help()
        import sys
        sys.exit(1)
    return db

def getDebug():
    global parser  
    (options, args) = parser.parse_args()
    return options.debug

def getTable():
    global parser  
    (options, args) = parser.parse_args()
    return options.table