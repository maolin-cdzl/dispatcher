import logging

from dbtypes import *
from dispatchdb import DispatchDB

class Dispatcher:
    def __init__(self,options):
        self.options
        self.db = None

    def setDB(self,db):
        self.db = db

    def defaultCtx(self):
        return self.options.get('default_ctx',None)

    def dispatch(self,ctx,account):
        if ctx is None:
            ctx = self.defaultCtx()

        platform = self.db.ctx_map.get(ctx,None)
        if platform is None:
            platform = self.db.ctx_map.get(self.defaultCtx(),None)
            if platforma is None:
                logging.error('default ctx invalid')
                return None
    
        user = platform.getUser(account)
        if user is None:
            return self.db.ctx_default.get(ctx,None)

        server = platform.dispatch(user.company)
        if server is None:
            server = self.db.ctx_default.get(ctx,None)
        return server


