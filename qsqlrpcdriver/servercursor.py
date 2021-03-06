#!/usr/bin/env python
# encoding: UTF-8
import threading
# UNICODE HANDLING:
#  In Python 2, if you want to receive uniformly all your database input in 
#  Unicode, you can register the related typecasters globally as soon as 
#  Psycopg is imported:
import psycopg2
import psycopg2.extras
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

from bjsonrpc.exceptions import ServerError
from bjsonrpc.handlers import BaseHandler
from bjsonrpc import createserver

import time

def tuplenormalization(rows):
    retrows = []
    for row in rows:
        retrow = []
        if not row is None:
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

class CursorSQL(BaseHandler):
        
    globaldata = { 'cursornumber' : 1 }
    setupLock = threading.Lock()
    
    def __init__(self, rpc):   
        BaseHandler.__init__(self,rpc)
        self.conn = rpc.conn
        self.cur = self.conn.cursor()
        #TODO hay que arreglar el curname, sino fallara!
        self.scur = self.conn.cursor(self.curname, cursor_factory=psycopg2.extras.DictCursor)
             
    def _setup(self):
        self.setupLock.acquire()
        
        cursornumber = self.globaldata['cursornumber']
        self.globaldata['cursornumber'] += 1 
        self.curname = "rpccursor_%04x" % cursornumber
        
        self.setupLock.release()
        
        self.rlock = threading.RLock()
        self.lastResult = ()
        self.lastResultRow = -99
        
    @withrlock    
    def fields(self):
        "Returns field list"
        descrip = self.cur.description
        if descrip is None: return None
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
    def closeCursor(self):
        "Closes the cursor."
        self.cur.close()
        self.cur = None
        
    @withrlock
    def execute(self, sql, params = None):
        # TODO: ver si es posible que desde Qt nos envien la consulta con placeholders (usando params) en lugar de construir ellos la SQL.
        # en principio sí: http://doc.qt.nokia.com/4.7-snapshot/qsqldriver.html#DriverFeature-enum, ya lo miraremos
        "Executes the specified SQL with given parameters."
        try:
            if params:
                self.cur.execute(sql,params)
            else:
                self.cur.execute(sql)
            return True
        except Exception,e:
            print "SQL Execute error", e
            return False
    
    @withrlock
    def getCursorSize(self):
        minsize = 0
        maxsize = None
        delta = None
        bigblock_sz = 100000
        smallblock_sz = 32
        rows = 1
        it = 0
        while maxsize is None or minsize < maxsize:
            t1 = time.time()
            mode = ""
            it += 1
            if it > 100: break
            if not minsize and not maxsize: rows = bigblock_sz
            elif minsize and not maxsize: rows += bigblock_sz
            elif not minsize and maxsize: rows /= 3
            else: 
                rows = (maxsize - minsize + 1) / 2 + minsize
                delta = maxsize - minsize
            
            try:
                if maxsize and maxsize - minsize < smallblock_sz:
                    frompos = max(minsize-1, 0)
                    ret=self.scur.scroll(frompos,"absolute")
                    r = self.scur.fetchall()
                    delta = len(r)
                    #print "*** Obtained %d lines." % delta
                        
                    if minsize == 0:
                        print "Delta:", delta, "ret:", repr(ret)
                        rows = maxsize = minsize = delta
                    else:
                        rows = minsize
                        minsize = minsize + delta - 1
                        maxsize = minsize
                    mode = "=="
                else:
                    r=self.scur.scroll(rows-1,"absolute")
                    r=self.scur.fetchone()
                    if not r: raise ValueError
            except ValueError:
                mode = "<-"
            else:
                if not mode: mode = "->"
            t2 = time.time()
            timedelta = (t2-t1)
            
            print "Testing rows %d: %s (%s-%s [%s]) %s (%.2fms)" % (it,repr(rows), repr(minsize), repr(maxsize), repr(delta), mode, timedelta*1000)
            if mode == "<-" and (not maxsize or maxsize > rows - 1): maxsize = rows - 1            
            if mode == "->" and (not minsize or minsize < rows): minsize = rows           
        print "END Testing rows %d: %s (%s-%s [%s]) %s (%.2fms)" % (it,repr(rows), repr(minsize), repr(maxsize), repr(delta), mode, timedelta*1000)
        #if minsize is None: return 0
        return minsize 
            
        
    
    @withrlock
    def executeSelect(self, sql, params = None):
        # TODO: ver si es posible que desde Qt nos envien la consulta con placeholders (usando params) en lugar de construir ellos la SQL.
        # en principio sí: http://doc.qt.nokia.com/4.7-snapshot/qsqldriver.html#DriverFeature-enum, ya lo miraremos
        "Executes the specified SQL with given parameters."
        try:
            if params:
                self.cur.execute(sql,params)
            else:
                #countsql = "SELECT COUNT(*) FROM ( %s ) t" % sql
                times = []
                #times.append(time.time())
                #self.cur.execute(countsql)
                #self.querySize = self.cur.fetchone()
                #self.querySize = self.querySize[0]
                times.append(time.time())
                
                try:
                    self.cur.execute(sql+" LIMIT 1")
                except Exception, e:
                    self.conn.rollback()
                    raise
                times.append(time.time())
                try:
                    self.scur.execute(sql)
                except Exception, e:
                    self.conn.rollback()
                    raise
                times.append(time.time())
                self.querySize = None
                for n,t1, t2 in zip(range(len(times)-1),times[:-1],times[1:]):
                    delta = t2-t1
                    ms = delta * 1000
                    if ms > 5:
                        print "executeSelect time %d: %.2fms" % (n, ms)
            return True
        except Exception, e:
            print e
            return False

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
    def getDataAtRow(self, row):
        "return data at row"
        if self.lastResultRow != row:
            self.scroll(row,'absolute')
            self.fetchOne()
        
        return tuplenormalization(self.lastResult)

    @withrlock    
    def getDataAtRange(self, row, size = 15):
        "return data at row range"
        self.scroll(row,'absolute')
        #rows = []
        #while (len(rows) < size): 
        #    self.fetchOne()
        #    rows.append(self.lastResult)
        #    if self.lastResult is None: break
        #    
        #return tuplenormalization(rows)
        
        return tuplenormalization(self.scur.fetchmany(size))

        
    @withrlock    
    def scroll(self, value, mode = 'relative'):
        """Moves the cursor up and down specified by *value* rows. mode can be
        set to 'absoulte'. """
        try:
            return self.scur.scroll(value, mode)
        except (psycopg2.ProgrammingError, IndexError), e:
            print "scrollError!!!!!!!!!!"
            return None
        
    @withrlock    
    def rowcount(self):
        "Returns the count of rows for the last query executed."
        if self.querySize is None:
            self.querySize = self.getCursorSize()
        
        return self.querySize
        
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
    