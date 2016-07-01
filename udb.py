from sets import Set
import logging

class UDB :
    def __init__(self):
        self.user_map = {}
        self.company_map = {}

    def addUser(self,u):
        if u is None:
            return
        if u.company not in self.company_map:
            # this maybe is not error.
            #logging.error("User %s 's company %s is not exists" % (u.account,u.company))
            return
        self.user_map[u.account] = u

    def addCompany(self,c):
        if c is None:
            return
        self.company_map[c.cid] = c

    def completeCompany(self):
        for company in self.company_map.values():
            self._complete(company)


    def _complete(self,c):
        company = c
        while True:
            parent = company.parent
            if parent is not None:
                if parent in c.parents:
                    logging.error('Loop parent of %s' % cid)
                    break;
                company = self.company_map.get(parent,None)
                if company is None:
                    logging.error('Parent %s is not exists' % parent)
                    break
                c.parents.append(parent)
                if c.agent is None and company.agent is not None:
                    c.agent = company.agent
            else:
                break
        


