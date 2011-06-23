#!/usr/bin/env python
# encoding: UTF-8

import os.path

import sys
from PyQt4 import QtGui, QtCore, uic, QtSql
import qsqlrpc
import clientoptions

def initializeModel(model):
    model.setTable("projectusers")

    model.setEditStrategy(QtSql.QSqlTableModel.OnManualSubmit)
    model.setRelation(1, QtSql.QSqlRelation('projects', 'id', 'code'))
    model.setRelation(2, QtSql.QSqlRelation('users', 'id', 'username'))

    model.setHeaderData(0, QtCore.Qt.Horizontal, "ID")
    model.setHeaderData(1, QtCore.Qt.Horizontal, "Project")
    model.setHeaderData(2, QtCore.Qt.Horizontal, "User")

    model.select()


def createView(title, model):
    view = QtGui.QTableView()
    view.setModel(model)
    view.setItemDelegate(QtSql.QSqlRelationalDelegate(view))
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
    #print ">> Database:",db
    #
    #if not db.open():
    #    print "unable to open database"
    #    sys.exit(1)
    
    clientoptions.prepareParser()
    qsqlrpc.DEBUG_MODE = clientoptions.getDebug()
    db = clientoptions.getDB()
    
    if not clientoptions.getTable() == "users":
        print "Sorry, in this example you cannot change the table."
        print "This example works with the three tables of Llampex DB."
    else:
        model = QtSql.QSqlRelationalTableModel(None,db)
        initializeModel(model)
        view = createView("Relational Table Model", model)
        view.show()
        app.exec_()
        
        del model
        del view

    clientoptions.db = None
    del db
    QtSql.QSqlDatabase.removeDatabase("myconnection")
    