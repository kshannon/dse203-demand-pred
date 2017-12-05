import wrapper2 as wp

def wrapperFactory(source,env='remote'):
    print "---" + env + "----"
    if source == "asterixWrapper":
        wrp = wp.asterixWrapper(env=env)
    elif source == "postgresWrapper":
        wrp = wp.postgresWrapper(env=env)
    else:
        raise Exception("request for unknown wrapper %s" % (source))
    
    return wrp