#!/usr/bin/env python
# encoding: UTF-8
import threading
# UNICODE HANDLING:
#  In Python 2, if you want to receive uniformly all your database input in 
#  Unicode, you can register the related typecasters globally as soon as 
#  Psycopg is imported:
import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

from optparse import OptionParser

from bjsonrpc.exceptions import ServerError
from bjsonrpc.handlers import BaseHandler
from bjsonrpc import createserver

import qsqlrpcdriver.servercursor as servercursor

class LlampexProject(BaseHandler):
    
    def _setup(self):
        global conn
        self.conn = None
        
    def login(self,username,password,project):
        # to no repeat work, i hard code here the login.
        # in real production, this have to check if user and password are valids, if the project is of the user...
        global parser
        self.conn = psycopg2.connect("dbname="+project+" user="+username+" password="+password+" host="+options.host+" port="+options.port)
        if self.conn is None:  raise ServerError, "InvalidConnectionError"
        return True
    
    def getCursor(self):
            return servercursor.CursorSQL(self)



if __name__ == '__main__':
    
    parser = OptionParser()
    
    parser.add_option("-d", "--dbname", dest="dbname", default="llampex",
                      help="DB name to connect. Default '%default'.")
    
    parser.add_option("-u", "--user", dest="user", default="llampexuser",
                  help="Valid user to connect DB. Default '%default'.")
    
    parser.add_option("-p", "--password", dest="password", default="llampexpasswd",
                  help="Valid password to connect DB. Default '%default'.")
    
    parser.add_option("-H", "--host", dest="host", default="127.0.0.1",
                  help="Host where is the DB. Default '%default'.")
    
    parser.add_option("-P", "--port", dest="port", default="5432",
                  help="Port of the host to acces DB. Default '%default'.")
    
    (options, args) = parser.parse_args()
    try:
        conn = psycopg2.connect("dbname="+options.dbname+" user="+options.user+" password="+options.password+" host="+options.host+" port="+options.port)
    except:
        import sys
        print "Could not connect to DB."
        parser.print_help()
        sys.exit(1)
    
    s = createserver(handler_factory=LlampexProject, host="0.0.0.0") # creamos el servidor
    s.debug_socket(True) # imprimir las tramas enviadas y recibidas.
    s.serve() # empieza el bucle infinito de servicio.
