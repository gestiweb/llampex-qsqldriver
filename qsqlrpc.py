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
    # TODO: Documentar la clase y sus métodos
    
    def __init__(self, driver):
        QtSql.QSqlResult.__init__(self,driver)
        self.sqldriver = driver
        # TODO: Crear el cursor remoto y asociarlo.
        self.c = self.sqldriver.c # TODO: No debería usar self.c.call.* , debería crear un cursor y asociarlo a self.cur 
        self.setup()
    
    def setup(self):
        self.cache = {}
        self.currentSize = 0
    
    def data(self,i):
        ret = self.getData(i,self.at()) # TODO: Hacer desaparecer el self.at() porque la implementación del driver no lo requiere.
        # print "$$ -> LlampexResult.data(%d) -> %s" % (i,repr(ret))
        return ret
        
    def getData(self, i, row):
        if row not in self.cache:
            retrange = self.c.call.getDataAtRange(row,20)
            for n, ret in enumerate(retrange):
                self.cache[row+n] = ret
                
        ret = self.cache[row] 
        return ret[i]
        
    
    def isNull(self,index):
        # TODO: Condicionar los debugs a una variable debug global.
        print "$$ -> LlampexResult.isNull(%d)" % (index) 
        return False
    
    def reset(self,query):
        print "$$ -> LlampexResult.reset(%s)" % (repr(query))
        self.setup()
        if self.sqldriver.executeQuery(query): # TODO: Documentar este algoritmo.
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
        # TODO: Los cursores se pueden posicionar desde BOF hasta EOF ambos inclusive. Probablemente fetch deba reflejar esto.
        # print "$$ -> LlampexResult.fetch(%d)" % (i)
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
    

class LlampexDriverPrivate(object): # TODO: Para que sirve esto actualmente? no sobra?
    def __init__(self, conn = None):
        print "hello world from private"

class QSqlLlampexDriver(QtSql.QSqlDriver):
    # TODO: limpiar comentarios de codigo antiguo que no se va a usar..
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
        self.c = connect() # TODO: Permitir usar una conexión ya existente.
        self.c = self.c.call.getCursor() # TODO: esto pertenece al Result, no al driver. Cada result tiene que tener un cursor.
        #setattr(self,"open",self.open_)
        
    def hasFeature(self,f):
        # TODO: No usar numeros, usar nombres de variables para que se entienda.
        # TODO: incluir todas las variables posibles aquí y marcarlas a false. Si aparece una desconocida, raise ValueError.
        print "~~ hasFeature: "+str(f)
        if f == 0: return True
        if f == 1: return True
        if f == 4: return True
        return False
    
    def open(self,db,user,passwd,host,port,options):
        print "~~ open database"
        # TODO: La conexión la abrimos ya en la declaración. Tal vez debería venir aquí. Como se tratan los valores de db, user, passwd.. ? 
        
        ok = self.c.call.openDB() # TODO: y esto qué es y para qué es? probablemente innecesario.
        self.setOpen(ok)
        self.setOpenError(not ok)
        return ok
        
    def close(self):
        print "~~ close"
        self.c.call.closeDB() # TODO: revisar el sentido de esto
        # TODO: faltará cerrar el cursor aquí
        self.setOpen(False)
        self.setOpenError(False)
    
    def createResult(self):
        print "~~ createResult"
        return QSqlLlampexResult(self)
        
    def executeQuery(self,query):
        print "~~ execute query:", query
        # TODO: borrar esta función. una query sólo puede ser ejecutada con un cursor, desde un result.
        ret = self.c.call.execute(unicode(query))
        self.fields = self.c.call.fields()
        return ret
        
    def getCurrentSize(self):
        # TODO: borrar esta función. esta función sólo puede ser ejecutada con un cursor, desde un result.
        size = self.c.call.rowcount()
        print "~~ current size -> %d " % size
        return self.c.call.rowcount()
        
    def getCursorPosition(self):
        # TODO: borrar esta función. esta función sólo puede ser ejecutada con un cursor, desde un result.
        print "~~ getPosition"
        return self.c.call.rownumber()
        
    def fetchOne(self):
        # TODO: borrar esta función. esta función sólo puede ser ejecutada con un cursor, desde un result.
        print "~~ fetch one"
        self.c.call.fetchOne()
        
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
        # TODO: reestructurar esta función. Hace una query sin usar un result.
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
        
        #TODO Test other types to change
        
        return r
                
        