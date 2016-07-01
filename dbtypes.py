import logging

class User:
    def __init__(self):
        self.account = None
        self.uid = None
        self.company = None

    @staticmethod
    def create(row):
        if ('account' not in row) or ('uid' not in row) or ('company' not in row):
            return None
        u = User()
        u.account = str(row['account'])
        u.uid = str(row['uid'])
        u.company = str(row['company'])
        return u

class Company:
    def __init__(self):
        self.cid = None
        self.agent = None
        self.parent = None
        self.parents = [] 

    @staticmethod
    def create(row):
        if 'cid' not in row:
            return None
        c = Company()
        c.cid = str(row['cid'])
        parent = row.get('parent',None)
        if parent is not None: 
            if parent != 0:
                c.parent = str(parent)

        agent = row.get('agent',None)
        if agent is not None:
            if agent != 0:
                c.agent = str(agent)
        return c

class Server:
    def __init__(self):
        self.name = None
        self.ip = None
        self.port = None

    @staticmethod
    def create(row):
        if ('server' not in row) or ('ip' not in row) or ('port' not in row):
            return None
        s = Server()
        s.name = str(row['server'])
        s.ip = str(row['ip'])
        s.port = row['port']
        return s
