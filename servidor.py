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

from bjsonrpc.exceptions import ServerError
from bjsonrpc.handlers import BaseHandler
from bjsonrpc import createserver

import servercursor

class LlampexProject(BaseHandler):
    
    def _setup(self):
        
        self.conn = psycopg2.connect("dbname=llampex user=llampexuser password=llampexpasswd host="+sys.argv[1]+" port="+sys.argv[2])
           
        
    def login(self,username,password,project):

        
        # to no repeat work, i hard code here the login.
        # in real production, this have to check if user and password are valids, if the project is of the user...
        # for this example, I connect to a project with db laperla
        
        self.conn = psycopg2.connect("dbname=laperla user=llampexuser password=llampexpasswd host="+sys.argv[1]+" port="+sys.argv[2])
        
        if self.conn is None:  raise ServerError, "InvalidConnectionError"
        
        return True
    
    def getCursor(self):
            return servercursor.CursorSQL(self)
            


if __name__ == '__main__':

    import sys
    if not len(sys.argv) == 3:
        print "You must indicate host and port. Example: python server.py localhost 5432"
        sys.exit(1)
    
    s = createserver(handler_factory=LlampexProject, host="0.0.0.0") # creamos el servidor
    
    s.debug_socket(True) # imprimir las tramas enviadas y recibidas.
    
    s.serve() # empieza el bucle infinito de servicio.
