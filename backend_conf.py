options = {
    'debug': True,
    'default_ctx': 'rel',
    'backend-address': 'tcp://127.0.0.1:5000',
    'ruledb': {
        'type': 'mysql',
        'server': 'localhost',
        'user': 'dispatcher',
        'password': 'shanlitech@231207',
        'database': 'dispatch'
    },
    'sync_period' : 60,
    'auth' : {
        #'whitelist': [],               # could be None which means allow any ip
        #'public_key_dir': 'public_keys',     # path that contains client public key
                                        # allow abspath or relation path.
                                        # default public_keys
        #'private_key_dir': 'private_keys',    # default private_keys
        #'private_key_file': 'server.key_secret',    # default server.key_secret 
    }
}

