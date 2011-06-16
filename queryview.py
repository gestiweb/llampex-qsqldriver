#!/usr/bin/env python
# encoding: UTF-8

import os.path

import sys
from PyQt4 import QtGui, QtCore, uic, QtSql
import qsqlrpc
import clientoptions
    
def initializeModel(model,db,table):
    model.setQuery('select * from '+table,db)
    print "<< setQuery Initialize Model"
    
    #If you don't automatize, you can put the headers you want
    #model.setHeaderData(0, QtCore.Qt.Horizontal, "ID")
    #model.setHeaderData(1, QtCore.Qt.Horizontal, "Username")
    #model.setHeaderData(2, QtCore.Qt.Horizontal, "Password")
    #model.setHeaderData(3, QtCore.Qt.Horizontal, "Active")
    #model.setHeaderData(4, QtCore.Qt.Horizontal, "Admin")
    
view = None
def createView(title, model):
    global view
    view = QtGui.QTableView()
    view.setModel(model)
    view.setWindowTitle(title)
    view.show()
    
if __name__ == '__main__':
    import sys
    app = QtGui.QApplication(sys.argv)
    
    #global llampex_driver, db
    #print
    #print "*** Testing Llampex Qt Database Driver..."
    #llampex_driver = qsqlrpc.QSqlLlampexDriver()
    #print llampex_driver
    #db = QtSql.QSqlDatabase.addDatabase(llampex_driver, "myconnection")
    #print ">> Database:",db
    #
    #if not db.open():
    #    print "unable to open database"
    #    sys.exit(1)
    
    clientoptions.prepareParser()
    qsqlrpc.DEBUG_MODE = clientoptions.getDebug()
    db = clientoptions.getDB()
    
    plainModel = QtSql.QSqlQueryModel()
    print ">> Initialize Model"
    initializeModel(plainModel,db,clientoptions.getTable())
    print ">> Create View"
    createView("Plain Query Model", plainModel)
    print ">> OK"
    
    app.exec_()
    del view

    #db.close()
    del db
    QtSql.QSqlDatabase.removeDatabase("myconnection")
    