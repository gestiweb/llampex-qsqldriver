#!/usr/bin/env python
# encoding: UTF-8

import os.path

import sys
from PyQt4 import QtGui, QtCore, uic, QtSql
import qsqlrpc

class TableEditor(QtGui.QDialog):
    def __init__(self, tableName, db, parent=None):
        super(TableEditor, self).__init__(parent)
    
        self.model = QtSql.QSqlTableModel(self,db)
        self.model.setTable(tableName)
        self.model.setEditStrategy(QtSql.QSqlTableModel.OnManualSubmit)
        if not self.model.select():
            print "Selection Fails"
            sys.exit(1)

        #self.model.setHeaderData(0, QtCore.Qt.Horizontal, "ID")
        #self.model.setHeaderData(1, QtCore.Qt.Horizontal, "Username")
        #self.model.setHeaderData(2, QtCore.Qt.Horizontal, "Password")
        #self.model.setHeaderData(3, QtCore.Qt.Horizontal, "Active")
        #self.model.setHeaderData(4, QtCore.Qt.Horizontal, "Admin")

        view = QtGui.QTableView()
        view.setModel(self.model)

        submitButton = QtGui.QPushButton("Submit")
        submitButton.setDefault(True)
        revertButton = QtGui.QPushButton("&Revert")
        quitButton = QtGui.QPushButton("Quit")

        buttonBox = QtGui.QDialogButtonBox(QtCore.Qt.Vertical)
        buttonBox.addButton(submitButton, QtGui.QDialogButtonBox.ActionRole)
        buttonBox.addButton(revertButton, QtGui.QDialogButtonBox.ActionRole)
        buttonBox.addButton(quitButton, QtGui.QDialogButtonBox.RejectRole)

        submitButton.clicked.connect(self.submit)
        revertButton.clicked.connect(self.model.revertAll)
        quitButton.clicked.connect(self.close)

        mainLayout = QtGui.QHBoxLayout()
        mainLayout.addWidget(view)
        mainLayout.addWidget(buttonBox)
        self.setLayout(mainLayout)

        self.setWindowTitle("Cached Llampex Table")

    def submit(self):
        if self.model.database().transaction():
            if self.model.submitAll():
                self.model.database().commit()
            else:
                self.model.database().rollback()
                QtGui.QMessageBox.warning(self, "Cached Table",
                            "The database reported an error: %s" % self.model.lastError().text())
        else:
            print "Fatal Error: Transaction fails"
        
if __name__ == '__main__':
    
    import sys
    app = QtGui.QApplication(sys.argv)
    
    global llampex_driver, db
    print
    print "*** Testing Llampex Qt Database Driver..."
    llampex_driver = qsqlrpc.QSqlLlampexDriver()
    print llampex_driver
    db = QtSql.QSqlDatabase.addDatabase(llampex_driver, "myconnection")
    db.setDatabaseName("laperla")
    db.setUserName("angel")
    db.setPassword("calidae")
    db.setHostName("127.0.0.1")
    db.setPort(10123)
    
    print ">> Database:",db
    
    if not db.open():
        print "unable to open database"
        sys.exit(1)

    editor = TableEditor('clientes',db)
    editor.show()
    editor.exec_()
    
    del editor    
    del db
    QtSql.QSqlDatabase.removeDatabase("myconnection")