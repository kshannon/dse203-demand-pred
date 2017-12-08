import wrapper2 as wp

def wrapperFactory(source,env='remote',tablelogger=None):
    #print "---" + env + "----"
    if source == "asterixWrapper":
        wrp = wp.asterixWrapper(env=env,tablelogger=tablelogger)
    elif source == "postgresWrapper":
        wrp = wp.postgresWrapper(env=env,tablelogger=tablelogger)
    else:
        raise Exception("request for unknown wrapper %s" % (source))
    
    return wrp