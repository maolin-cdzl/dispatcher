import logging

class Platform:
    def __init__(self):
        self.name = None
        self.dbconf = None
        self.servers = {}
        self.company_rules = {}
        self.agent_rules = {}
        self.udb = None

    @staticmethod
    def create(row):
        if ('platform' is not in row) or ('db_server' is not in row) or ('db_user' is not in row) or ('db_password' is not in row) or ('db_database' is not in row):
            logging.error('Platform information missing')
            return None
        pf = Platform()
        pf.name = row['platform']
        pf.dbconf = { 'server': row['db_server'], 'user': row['db_user'], 'password': row['db_password'], 'database': row['db_database'] }
        return pf

    def setUdb(self,udb):
        self.udb = udb

    def getUser(self,account):
        return self.udb.user_map.get(account,None)

    def addServer(self,server):
        if server is None:
            return
        self.servers[server.name] = server

    def getServer(self,sname):
        return self.servers.get(sname,None)

    def addCompanyRule(self,cid,sname):
        if cid is None or sname is None:
            return
        if sname not in self.servers:
            logging('Rule of company %s point to a unexists server %s' % (cid,sname))
            return
        self.company_rules[cid] = sname

    def addAgentRule(self,agent,sname):
        if agent is None or sname is None:
            return
        if sname not in self.servers:
            logging('Rule of agent %s point to a unexists server %s' % (agent,sname))
            return
        self.agent_rules[agent] = sname

    def dispatch(self,cid):
        company = self.udb.company_map.get(user.company,None)
        if company is None:
            logging.error('Company %s not exists,this should be bug!' % cid)
            return None
        
        if company.cid in self.company_rules:
            return self.getServer(self.company_rules.get(company.cid))
        for parent in company.parents:
            if parent in self.company_rules:
                return self.getServer(self.company_rules.get(parent.cid))

        if company.agent in self.agent_rules:
            return self.getServer(self.agent_rules.get(company.agent))

        return None

