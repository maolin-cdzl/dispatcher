import logging

class DispatchDB:
    def __init__(self):
        self.platform_map = {}
        self.ctx_map = {}
        self.ctx_default = {}

    def majorInfo(self):
        for platform in self.platform_map.values():
            platform.majorInfo()
        for ctx in self.ctx_map.keys():
            logging.info('ctx %s point to platform %s,default server %s' % (ctx,self.ctx_map[ctx].name,self.ctx_default[ctx]))

    def addPlatformat(self,pf):
        if pf is None:
            return
        self.platform_map[pf.name] = pf

    def addCtx(self,pfname,ctx,defaultServer):
        if pfname is None or ctx is None or defaultServer is None:
            return
        platform = self.platform_map.get(pfname,None)
        if platform is None:
            logging.error('ctx %s, platform %s is not exists' % (ctx,pfname))
            return

        server = platform.servers.get(defaultServer,None)
        if server is None:
            logging.error('ctx %s server %s is not exists' % (ctx,defaultServer))
            return
        self.ctx_map[ctx] = platform
        self.ctx_default[ctx] = server

    def addServer(self,pfname,server):
        if pfname is None or server is None:
            return
        platform = self.platform_map.get(pfname,None)
        if platform is None:
            logging.error("server's platform %s is not exists" % pfname)
            return
        platform.addServer(server)

    def addCompanyRule(self,pfname,cid,sname):
        if pfname is None or cid is None or sname is None:
            return
        platform = self.platform_map.get(pfname,None)
        if platform is None:
            logging.error('rule for company %s, platform %s is not exists' % (cid,pfname))
            return
        platform.addCompanyRule(cid,sname)

    def addAgentRule(self,pfname,agent,sname):
        if pfname is None or agent is None or sname is None:
            return
        platform = self.platform_map.get(pfname,None)
        if platform is None:
            logging.error('rule for agent %s, platform %s is not exists' % (agent,pfname))
            return
        platform.addAgentRule(agent,sname)

