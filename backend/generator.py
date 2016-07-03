import logging
import Queue
import string
import random
import pymssql
import mysql.connector
import zmq
import threading
from dbtypes import User,Company,Server
from echat_platform import Platform
from udb import UDB
from dispatchdb import DispatchDB

class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None

class DatabaseCursor:
    def __init__(self,conn):
        if isinstance(conn,mysql.connector.MySQLConnection):
            self.cursor = conn.cursor(cursor_class=MySQLCursorDict)
        elif isinstance(conn,pymssql.Connection):
            self.cursor = conn.cursor(as_dict=True)
        else:
            raise RuntimeError('Unsupport database type')

    def __enter__(self):
        return self.cursor

    def __exit__(self,exc_type, exc_value, exc_tb):
        self.cursor.close()
        self.cursor = None

def database_connect(dbconf):
    dbtype = dbconf.get('type')
    server = dbconf.get('server')
    user = dbconf.get('user')
    password = dbconf.get('password')
    database = dbconf.get('database')

    if dbtype is None or server is None or user is None or password is None or database is None:
        raise RuntimeError('Database connection information missing')

    if dbtype == 'mssql':
        return pymssql.connect(server,user,password,database)
    elif dbtype == 'mysql':
        return mysql.connector.connect(host=server,user=user,password=password,database=database)
    else:
        raise RuntimeError('Unsupport database type: %s' % dbtype)

def GenerateUDB(dbconf):
    udb = UDB()
    try:
        conn = database_connect(dbconf)
        with DatabaseCursor(conn) as cursor:
            cursor.execute('SELECT Corg_ID as cid,Corg_Parent as parent,Aorg_ID as agent FROM tb_ComOrg')
            for row in cursor:
                udb.addCompany( Company.create(row) )

        with DatabaseCursor(conn) as cursor:
            cursor.execute('SELECT User_ID as uid,User_Account as account,User_CompanyID as company FROM tb_User WHERE User_Enable <> 0')
            for row in cursor:
                udb.addUser( User.create(row) )
        logging.info('Read from MsSql done')
    except Exception as e:
        logging.error('exception when read database %s' % server,e)
    finally:
        if conn is not None:
            conn.close()
            conn = None

    udb.completeCompany()
    logging.info('Read %d user' % len(udb.user_map))
    logging.info('Read %d company' % len(udb.company_map))
    return udb

def GenerateDispatchDB(dbconf):
    logging.info('Start generate dispatch database')
    ddb = DispatchDB()
    try:
        conn = database_connect(dbconf)
        with DatabaseCursor(conn) as cursor:
            cursor.execute('SELECT platform,db_type,db_server,db_user,db_password,db_database FROM tb_platform')
            for row in cursor:
                ddb.addPlatformat( Platform.create(row) )

        with DatabaseCursor(conn) as cursor:
            cursor.execute('SELECT server,platform,ip,port FROM tb_server')
            for row in cursor:
                ddb.addServer(row.get('platform'),Server.create(row))

        with DatabaseCursor(conn) as cursor:
            cursor.execute('SELECT ctx,platform,default_server FROM tb_ctx')
            for row in cursor:
                ddb.addCtx(row.get('platform'),row.get('ctx'),row.get('default_server'))

        with DatabaseCursor(conn) as cursor:
            cursor.execute('SELECT platform,company,server FROM tb_company_rule')
            for row in cursor:
                ddb.addCompanyRule(row.get('platform'),row.get('company'),row.get('server'))

        with DatabaseCursor(conn) as cursor:
            cursor.execute('SELECT platform,agent,server FROM tb_agent_rule')
            for row in cursor:
                ddb.addAgentRule(row.get('platform'),row.get('agent'),row.get('server'))
    except Exception as e:
        logging.error('exception when read dispatch db: {0}'.format(e))
    finally:
        if conn is not None:
            conn.close()
            conn = None

    ddb.majorInfo()

    for platform in ddb.platform_map.values():
        udb = GenerateUDB(platform.dbconf)
        if udb is None:
            logging.error('Will not update dispatchdb: Can not generate platform %s user database' % platform.name)
            return None
        platform.setUdb( udb )

    return ddb
        

class DDBGenerator(threading.Thread):
    def __init__(self,**kwargs):
        super(DDBGenerator,self).__init__()
        if 'ruledb' not in kwargs:
            raise RuntimeError('DDBGenerator need ruledb configuration')
        self.dbconf = kwargs['ruledb']
        self.q = Queue.Queue()
        self.address = 'inproc://%s' % (''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)))
        self.ctx = zmq.Context()
        self.s_out = None
        self.s_in = None
        if 'period' in kwargs:
            self.period = int(kwargs['period'])
        else:
            self.period = 60

    def start(self):
        self.s_out = self.ctx.socket(zmq.PAIR)
        self.s_out.bind(self.address)
        super(DDBGenerator,self).start()

        ev = self.s_out.poll(1000,zmq.POLLOUT)
        if not ( ev & zmq.POLLOUT ):
            raise RuntimeError('Error when construct DDBGenerator socket pair')

    def stop(self):
        if self.isAlive():
            self.s_out.send('quit')
            self.join(1.0)
            if self.isAlive():
                raise RuntimeError('DDBGenerator thread stop timeouted')
            self.s_out.close()
            self.s_out = None
            self.ctx = None

    def socket(self):
        return self.s_out

    def push(self,ddb):
        try:
            while not self.q.empty():
                self.q.get_nowait()
        except Queue.Empty:
            pass
        
        self.q.put(ddb)

    def pop(self):
        ddb = None
        try:
            while not self.q.empty():
                ddb = self.q.get_nowait()
        except Queue.Empty:
            pass
        return ddb

    def run(self):
        try:
            self.s_in = self.ctx.socket(zmq.PAIR)
            self.s_in.connect(self.address)
            ev = self.s_in.poll(1000,zmq.POLLOUT)
            if not ( ev & zmq.POLLOUT ):
                return

            poller = zmq.Poller()
            poller.register(self.s_in,zmq.POLLIN)

            dbconf = self.dbconf
            timeout = self.period * 1000
            while True:
                events = poller.poll(timeout)
                if len(events) == 0:
                    logging.info('start refresh dispatch database')
                    db = GenerateDispatchDB(dbconf)
                    if db is not None:
                        logging.info('fetch dispatch database success')
                        self.push(db)
                        self.s_in.send('product')
                    else:
                        logging.info('fetch dispatch database failed')
                else:
                    break
        finally:
            self.s_in.close()
            self.s_in = None
