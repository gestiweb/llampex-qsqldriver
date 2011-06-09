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

import serverCursor

class LlampexProject(BaseHandler):
    
    def _setup(self):
        # TODO: la configuración de conexión no puede estar grabada en el código fuente.
        # TODO: ya, pero realmente hay que leerla ahora? Cuando la integremos en llampex
        #   ya lo leera, por ahora así vale, no? :S
        self.conn = psycopg2.connect("dbname=llampex user=llampexuser password=llampexpasswd host=king.calidae.net port=5432")
    
    def getCursor(self):
            return serverCursor.CursorSQL(self)
            

s = createserver(handler_factory=LlampexProject, host="0.0.0.0") # creamos el servidor

s.debug_socket(True) # imprimir las tramas enviadas y recibidas.

s.serve() # empieza el bucle infinito de servicio.