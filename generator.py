import logging
import pymssql
from dbtypes import User,Company
from udb import UDB


def GenerateUDB(dbconf):
    server = dbconf.get('server')
    user = dbconf.get('user')
    password = dbconf.get('password')
    database = dbconf.get('database')

    if server is None or user is None or password is None or database is None:
        raise RuntimeError('Database connection information missing')

    logging.info('Start read from MsSql')
    udb = UDB()

    try:
        with pymssql.connect(server,user,password,database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute('SELECT Corg_ID as cid,Corg_Parent as parent,Aorg_ID as agent FROM tb_ComOrg')
                for row in cursor:
                    udb.addCompany( Company.create(row) )

                cursor.execute('SELECT User_ID as uid,User_Account as account,User_CompanyID as company FROM tb_User WHERE User_Enable <> 0')
                for row in cursor:
                    udb.addUser( User.create(row) )
        logging.info('Read from MsSql done')
    except Exception as e:
        logging.error('exception when read database %s' % server,e)
        return None

    udb.completeCompany()
    logging.info('Read %d user' % len(udb.user_map))
    logging.info('Read %d company' % len(udb.company_map))
    return udb

def GenerateDispatchDB(opt):
    server = opt.get('server')
    user = opt.get('user')
    password = opt.get('password')
    database = opt.get('database')
    
    ddb = DispatchDB()
    try:
        with pymssql.connect(server,user,password,database) as conn:
            with conn.cursor(as_dict=True) as cursor:
                cursor.execute('SELECT platform,db_server,db_user,db_password,db_database FROM tb_platform')
                for row in cursor:
                    ddb.addPlatformat( Platform.create(row) )

                cursor.execute('SELECT server,platform,ip,port FROM tb_server')
                for row in cursor:
                    ddb.addServer(row.get('platform'),Server.create(row))

                cursor.execute('SELECT ctx,platform,default_server FROM tb_ctx')
                for row in cursor:
                    ddb.addCtx(row.get('platform'),row.get('ctx'),row.get('default_server'))

                cursor.execute('SELECT platform,company,server FROM tb_company_rule')
                for row in cursor:
                    ddb.addCompanyRule(row.get('platform'),row.get('company'),row.get('server'))

                cursor.execute('SELECT platform,agent,server FROM tb_agent_rule')
                for row in cursor:
                    ddb.addAgentRule(row.get('platform'),row.get('agent'),row.get('server'))
    except Exception as e:
        logging.error('exception when read dispatch db: {0}'.format(e))
        return None
    for platform in ddb.platform_map.values():
        platform.setUdb( GenerateUDB(platform.dbconf) )

    return ddb
        
