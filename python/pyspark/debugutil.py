import os

HAS_CHECK_DEBUGGER = False
def check_debugger():
    global HAS_CHECK_DEBUGGER
    
    if HAS_CHECK_DEBUGGER:
        return
    
    HAS_CHECK_DEBUGGER = True
    env = dict(os.environ)
    debug_server = env.get('PYTHON_REMOTE_DEBUG_SERVER')
    debug_port = env.get('PYTHON_REMOTE_DEBUG_PORT')
        
    if debug_server != None and debug_port != None:
        try:
            import pydevd
            print('connecting debug server %s, port %s'
                    % (debug_server, debug_port))
            pydevd.settrace(debug_server, 
                            port=int(debug_port), 
                            stdoutToServer=False, 
                            stderrToServer=False)
        except Exception, e:
            print(e)
            raise Exception('init debugger fail.' )