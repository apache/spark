"""
Remote debugging module for PySpark workers using debugpy for VS Code.
This file replaces the default PySpark worker daemon to enable debugging.
"""

import os
from pyspark import daemon, worker


def remote_debug_wrapped(*args, **kwargs):
    """
    Wrapper function that enables debugpy debugging before calling worker.main
    """
    try:
        import debugpy
        
        # Check if debugpy is already listening (avoid multiple listeners)
        if not debugpy.is_client_connected():
            # Listen on port 56800 for VS Code to attach
            debugpy.listen(('localhost', 56800))
            print(f"PySpark worker {os.getpid()} waiting for debugger on port 56800...")
            
            # Wait for debugger to attach
            debugpy.wait_for_client()
            print(f"Debugger attached to PySpark worker {os.getpid()}")
            
    except ImportError:
        print("debugpy not available - worker will run without debugging")
    except Exception as e:
        print(f"Debug setup failed: {e}")
    
    # Call the actual worker main function
    worker.main(*args, **kwargs)


# Replace the default worker main with our debug-enabled version
daemon.worker_main = remote_debug_wrapped

if __name__ == '__main__':
    import os
    daemon.manager()
