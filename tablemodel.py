#!/usr/bin/env python
# encoding: UTF-8

import os.path

import sys
from PyQt4 import QtGui, QtCore, uic, QtSql
import qsqlrpc
import clientoptions

def initializeModel(model,table):
    model.setTable(table)

    model.setEditStrategy(QtSql.QSqlTableModel.OnManualSubmit)
    model.select()

def createView(title, model):
    view = QtGui.QTableView()
    view.setModel(model)
    view.setWindowTitle(title)
    return view
        
if __name__ == '__main__':
    import sys
    app = QtGui.QApplication(sys.argv)
    
    #global llampex_driver, db
    #print
    #print "*** Testing Llampex Qt Database Driver..."
    #llampex_driver = qsqlrpc.QSqlLlampexDriver()
    #print llampex_driver
    #db = QtSql.QSqlDatabase.addDatabase(llampex_driver, "myconnection")
    #
    #db.setDatabaseName("llampex")
    #db.setUserName("llampexuser")
    #db.setPassword("llampexpasswd")
    #db.setHostName("127.0.0.1")
    #db.setPort(10123)
    #print ">> Database:",db
    #
    #if not db.open():
    #    print "unable to open database"
    #    sys.exit(1)
    
    clientoptions.prepareParser()
    qsqlrpc.DEBUG_MODE = clientoptions.getDebug()
    db = clientoptions.getDB()
        
    model = QtSql.QSqlTableModel(None,db)

    initializeModel(model,clientoptions.getTable())

    view1 = createView("Table Model (View 1)", model)
    view2 = createView("Table Model (View 2)", model)

    view1.show()
    view2.move(view1.x() + view1.width() + 20, view1.y())
    view2.show()
    
    app.exec_()
    
    del model
    del view1
    del view2

    #db.close()
    clientoptions.db = None
    del db
    QtSql.QSqlDatabase.removeDatabase("myconnection")