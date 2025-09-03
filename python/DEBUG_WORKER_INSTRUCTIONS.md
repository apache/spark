# Debug PySpark Workers - Step-by-Step Instructions

## The Issue Was:
The `PYSPARK_WORKER_DEBUG*` environment variables don't actually exist in PySpark. The correct way to debug PySpark workers is using a custom daemon module.

## What I've Set Up:

1. **`remote_debug.py`**: A custom daemon module that wraps PySpark workers with debugpy
2. **Updated VS Code config**: Uses the custom daemon module via `PYSPARK_SUBMIT_ARGS`
3. **Proper debugger attachment**: Worker waits for debugger on port 56800

## How to Debug:

### Step 1: Set Breakpoints
Set breakpoints in `python/pyspark/sql/pandas/serializers.py` wherever you want to debug:
- Line 258: `ArrowStreamArrowUDTFSerializer.dump_stream()` method start
- Line 270+: Inside the coercion loop where `self._create_array()` is called

### Step 2: Start the Worker (Wait for Attach)
1. **Run the debug configuration**: Press `F5` → Select **"Debug PySpark Worker (Wait for Attach)"**
2. **Watch console output**: You should see:
   ```
   PySpark worker <PID> waiting for debugger on port 56800...
   ```
3. **Worker will pause here** waiting for VS Code to attach

### Step 3: Attach VS Code Debugger  
1. **While Step 2 is still waiting**, open the command palette (`Cmd+Shift+P`)
2. **Type**: "Debug: Select and Start Debugging"
3. **Select**: "Attach to Worker Port 56800"  
4. **You should see**: "Debugger attached to PySpark worker <PID>"

### Step 4: Continue and Hit Breakpoints
1. **Press F5** in the attached debugger to continue execution
2. **The worker will continue** and hit your breakpoints in `serializers.py`
3. **Debug away!** Inspect variables, step through code, etc.

## Key Files Created:

- `python/remote_debug.py` - Custom PySpark daemon with debugpy integration
- `.vscode/launch.json` - Updated with `PYSPARK_SUBMIT_ARGS` configuration

## Troubleshooting:

- **If worker doesn't wait**: Check that `PYSPARK_SUBMIT_ARGS` includes the daemon module config
- **If attachment fails**: Verify port 56800 isn't in use by another process  
- **If no breakpoints hit**: Make sure the test actually executes worker code (some tests might fail earlier)
- **Install debugpy**: `pip install debugpy` if not already installed

## Why This Works:

- PySpark uses `spark.python.daemon.module=remote_debug` to load our custom worker wrapper
- Our wrapper calls `debugpy.wait_for_client()` before executing the actual worker code  
- This gives you full debugging access to the worker process where `serializers.py` runs

The worker will now properly pause and wait for your debugger to attach!
