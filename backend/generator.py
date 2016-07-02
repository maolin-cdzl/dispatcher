import logging
import pymssql
import mysql.connector
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
        raise
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
        raise
    finally:
        if conn is not None:
            conn.close()
            conn = None

    for platform in ddb.platform_map.values():
        platform.setUdb( GenerateUDB(platform.dbconf) )

    return ddb
        
