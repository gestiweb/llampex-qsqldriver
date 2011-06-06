#!/usr/bin/env python
# encoding: UTF-8
import threading
# UNICODE HANDLING:
#  In Python 2, if you want to receive uniformly all your database input in 
#  Unicode, you can register the related typecasters globally as soon as 
#  Psycopg is imported:
import psycopg2
import psycopg2.extensions
#psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
#psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

from bjsonrpc.exceptions import ServerError
from bjsonrpc.handlers import BaseHandler
from bjsonrpc import createserver

def tuplenormalization(rows):
    retrows = []
    for row in rows:
        retrow = []
        for field in row:
            if field is None:
                retfield = None
            elif type(field) is bool:
                retfield = field
            else:
                retfield = unicode(field)
            retrow.append(retfield)
        retrows.append(retrow)
    return retrows
    

def withrlock(function):
    def lockfn(self,*args,**kwargs):
        if self.cur is None: raise ServerError, "Cursor not Open!"
        self.rlock.acquire()
        ret = function(self,*args,**kwargs)
        self.rlock.release()
        return ret

    lockfn.__name__ = function.__name__
    return lockfn

class LlampexProject(BaseHandler):
    
    def _setup(self):
        # TODO: la configuración de conexión no puede estar grabada en el código fuente.
        self.conn = psycopg2.connect("dbname=llampex user=llampexuser password=llampexpasswd host=localhost port=5432")
    
    def getCursor(self):
        # TODO: Sintaxis alternativa para crear directamente un cursor con una query dada.
        return CursorSQL(self)
    
    
        
class CursorSQL(BaseHandler):
        
    #def _setup(self):
    #    self.rlock = threading.RLock()
    #    self.lastResult = ()
        
    globaldata = { 'cursornumber' : 1 }
    def __init__(self, rpc):
        BaseHandler.__init__(self,rpc)
        self.conn = rpc.conn
        # TODO: agregar un RLock general para evitar que esto se ejecute paralelamente.
        cursornumber = self.globaldata['cursornumber']
        self.globaldata['cursornumber'] += 1 
        self.curname = "rpccursor_%04x" % cursornumber
        # <<<< TODO
        self.rlock = threading.RLock()
        self.lastResult = ()
        self.lastResultRow = -99
        
    def openDB(self): # TODO: función innecesaria. La declaración del cursor implica tener un cursor de base de datos.
        try:
            self.cur = self.conn.cursor()
            print "opened"
            return True
        except:
            print "error opening"
            return False

    def executaConsulta(self, sql): # TODO: métodos deben ser en inglés
        # TODO: revisar si esta función aún se usa y si no es util, borrarla.
        # ens connectem a la bbdd, preparem el cursor i fem la consulta,
        self.cur.execute(sql)
        
        #preparem una llista pels resultats, i recorrem el cursor per guardar-ho tot a la llista
        result = []
        
        for row in self.cur:
            result.append(row)
            
        return result;
        
    @withrlock    
    def description(self):
        # TODO: Función innecesaria, se borra.
        "Returns field properties"
        return self.cur.description

    @withrlock    
    def fields(self):
        "Returns field list"
        descrip = self.cur.description
        if descrip is None: return None
        print descrip # TODO: prints solo si hay que depurar algo
        tmpCur = self.conn.cursor()
        fields = []
        #fields = [(l[0],l[1]) for l in descrip]
        for l in descrip:
            tmpCur.execute("SELECT typname FROM pg_type WHERE oid = "+str(l[1]))
            type = tmpCur.fetchone()
            fields.append((l[0],type[0]))
        
        return fields
    
    @withrlock    
    def commit(self):
        "Commits the current transaction."
        self.conn.commit()
    
    @withrlock    
    def rollback(self):
        "Rollbacks the changes for the current transaction."
        self.conn.rollback()
    
    @withrlock    
    def closeDB(self):
        "Closes the cursor."
        self.cur.close()
        self.conn.close() # TODO: Y cierras también la base de datos... y cuando llegue el siguiente cursor? 
        self.cur = None
        
    @withrlock    
    def execute(self, sql, params = None):
        # TODO: ver si es posible que desde Qt nos envien la consulta con placeholders (usando params) en lugar de construir ellos la SQL.
        "Executes the specified SQL with given parameters."
        try:
            if params:
                self.cur.execute(sql,params)
            else:
                self.cur.execute(sql)
            return True
        except:
            return False
        
    @withrlock    
    def selecttable(self, tablename, fieldlist = ["*"], wherelist = [], orderby = [], limit = 5000, offset = 0):
        # TODO: Función que no se usa, se debe borrar. 
        """
            Selects the specified table with the specified columns and filtered with the specified values
            returns the field list and some other info.
        """
        self._sqlparams = {
            'fields' : ",".join(fieldlist),
            'table' : tablename,
            'limit' : limit,
            'offset' : offset
            }
        txtwhere = []
        for where1 in wherelist:
            txt = ""
            if type(where1) is not dict: raise ServerError, "WhereClauseNotObject"
            fieldname = where1.get("fieldname")
            op = where1.get("op")
            value = where1.get("value")
            if not op and not fieldname:
                txt = self.cur.mogrify("%s", [value])
            elif op in "< > = >= <= LIKE ILIKE IN ~".split():
                txt = self.cur.mogrify(fieldname + " " + op + " %s", [value])
            else:
                raise ServerError, "WhereClauseBadFormatted: " + repr(where1)
            
            if not txt: raise ServerError, "WhereClauseEmpty"
            txtwhere.append(txt)
        
        if not txtwhere: txtwhere = ["TRUE"]
        self._sqlparams["where"] = " AND ".join(txtwhere)
        

        self.cur.execute(""" 
            SELECT %(fields)s 
            FROM %(table)s 
            LIMIT 0
            """ % self._sqlparams)
        
        descrip = self.cur.description
        if descrip is None: raise ServerError, "UnexpectedQueryError"
        self._sqlinfo = {}
        
        self._sqlinfo["fields"] = [l[0] for l in descrip]
        if not orderby:
            orderby = self._sqlinfo["fields"][:1]
            
        self._sqlparams["orderby"] = ", ".join(orderby)
        return self.getmoredata(limit)
    
    @withrlock    
    def getmoredata(self, amount = 5000):
        # TODO: funcion obsoleta, borrar.
        
        self._sqlparams["limit"] = amount
        sql = """ 
            SELECT COUNT(*) as c FROM (
                SELECT 0
                FROM %(table)s 
                WHERE %(where)s 
                LIMIT %(limit)d OFFSET %(offset)d 
            ) a
            """ % self._sqlparams
        try:
            self.cur.execute(sql)
        except Exception:
            print "SQL::" + sql
            raise
        row = self.cur.fetchone()
        self._sqlinfo["count"] = row[0] 
        
        def thread_moredata():
            self.rlock.acquire() # Will wait until parent call finishes.
            self.cur.execute(""" 
                SELECT %(fields)s 
                FROM %(table)s 
                WHERE %(where)s 
                ORDER BY %(orderby)s 
                LIMIT %(limit)d OFFSET %(offset)d
                """ % self._sqlparams)
            self.rlock.release()
            self._sqlparams["offset"] += self._sqlparams["limit"]
            
        
        th1 = threading.Thread(target = thread_moredata)
        th1.start()
        
        return self._sqlinfo
        
        

    @withrlock    
    def fetch(self, size=20):
        "Fetches many rows. Use -1 or None for querying all available rows."
        if size is None or size <= 0:
            return tuplenormalization(self.cur.fetchall())
        else:
            return tuplenormalization(self.cur.fetchmany(size))
            
    @withrlock    
    def fetchOne(self):
        "Fetches one row"
        #self.lastResult = tuplenormalization(self.cur.fetchone())
        self.lastResultRow = self.cur.rownumber
        self.lastResult = self.cur.fetchone()
        
    @withrlock    
    def getData(self, i):
        # TODO: Borrar esta función. Es incorrecto enviar una única celda.
        "return data"
        print self.lastResult
        return self.lastResult[i]

    @withrlock    
    def getDataAtRow(self, row):
        "return data at row"
        if self.lastResultRow != row:
            self.scroll(row,'absolute')
            self.fetchOne()
        
        return self.lastResult

    @withrlock    
    def getDataAtRange(self, row, size = 15):
        "return data at row range"
        self.scroll(row,'absolute')
        rows = []
        while (len(rows) < size): 
            self.fetchOne()
            rows.append(self.lastResult)
            if self.lastResult is None: break
            
        return rows

        
    @withrlock    
    def scroll(self, value, mode = 'relative'):
        """Moves the cursor up and down specified by *value* rows. mode can be
        set to 'absoulte'. """
        try:
            return self.cur.scroll(value, mode)
        except (psycopg2.ProgrammingError, IndexError), e:
            print "scrollError!!!!!!!!!!"
            return None
        
    @withrlock    
    def rowcount(self):
        "Returns the count of rows for the last query executed."
        return self.cur.rowcount
        
    @withrlock    
    def rownumber(self):
        "Return the row index where the cursor is in a zero-based index."
        return self.cur.rownumber
        
    @withrlock
    def query(self):
        "Return the latest SQL query sent to the backend"
        return self.cur.query
        
    @withrlock
    def statusmessage(self):
        "Return the latest Status message returned by the backend"
        return self.cur.query
    
    @withrlock
    def copy_from(self,*args):
        "Coipes a data set from a file to the server"
        raise ServerError, "NotImplementedError"
    
    @withrlock
    def copy_to(self,*args):
        "Dumps a data set from table to a file"
        raise ServerError, "NotImplementedError"


s = createserver(handler_factory=LlampexProject, host="0.0.0.0") # creamos el servidor

s.debug_socket(True) # imprimir las tramas enviadas y recibidas.

s.serve() # empieza el bucle infinito de servicio.