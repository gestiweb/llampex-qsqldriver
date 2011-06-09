#!/usr/bin/env python
# encoding: UTF-8

import os.path

import sys
from PyQt4 import QtGui, QtCore, uic, QtSql
import qsqlrpc


class EditableSqlModel(QtSql.QSqlQueryModel):
    def __init__(self, db):
        QtSql.QSqlQueryModel.__init__(self)
        self.db = db
        
    
    def flags(self, index):
        flags = super(EditableSqlModel, self).flags(index)

        if index.column() in (1, 2, 3, 4):
            flags |= QtCore.Qt.ItemIsEditable

        return flags

    def setData(self, index, value, role):
        if index.column() not in (1, 2, 3, 4):
            return False

        primaryKeyIndex = self.index(index.row(), 0)
        id = self.data(primaryKeyIndex)

        self.clear()

        if index.column() == 1:
            ok = self.setValue(id, "username", value)
        elif index.column() == 2:
            ok = self.setValue(id, "password", value)
        elif index.column() == 3:
            ok = self.setValue(id, "active", value)
        else:
            ok = self.setValue(id, "admin", value)

        self.refresh()
        return ok

    def refresh(self):
        self.setQuery('select * from users ORDER BY id',db)
        self.setHeaderData(0, QtCore.Qt.Horizontal, "ID")
        self.setHeaderData(1, QtCore.Qt.Horizontal, "Username")
        self.setHeaderData(2, QtCore.Qt.Horizontal, "Password")
        self.setHeaderData(3, QtCore.Qt.Horizontal, "Active")
        self.setHeaderData(4, QtCore.Qt.Horizontal, "Admin")

    def setValue(self, id, field, value):
        query = QtSql.QSqlQuery(db)
        query.prepare('update users set '+field+' = ? where id = ?')
        query.addBindValue(value)
        query.addBindValue(id)
        return query.exec_()

def initializeModel(model,db):
    model.setQuery('select * from users ORDER BY id',db)
    print "<< setQuery Initialize Model"
    
    model.setHeaderData(0, QtCore.Qt.Horizontal, "ID")
    model.setHeaderData(1, QtCore.Qt.Horizontal, "Username")
    model.setHeaderData(2, QtCore.Qt.Horizontal, "Password")
    model.setHeaderData(3, QtCore.Qt.Horizontal, "Active")
    model.setHeaderData(4, QtCore.Qt.Horizontal, "Admin")
    
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
    
    global llampex_driver, db
    print
    print "*** Testing Llampex Qt Database Driver..."
    llampex_driver = qsqlrpc.QSqlLlampexDriver()
    print llampex_driver
    db = QtSql.QSqlDatabase.addDatabase(llampex_driver, "myconnection")
    print ">> Database:",db
    
    if not db.open():
        print "unable to open database"
        sys.exit(1)
    
    editableModel = EditableSqlModel(db)
    print ">> Initialize Editable Model"
    initializeModel(editableModel,db)
    print ">> Create View"
    createView("Plain Query Model", editableModel)
    print ">> OK"
    
    app.exec_()
    
    del editableModel
    del view

    #db.close()
    del db
    QtSql.QSqlDatabase.removeDatabase("myconnection")