# VSCode Debugger Support

This directory is to support [VSCode Python debugger](https://code.visualstudio.com/docs/python/debugging)
for PySpark.

## Usage

First of all, you need to install `debugpy`.

```
pip install debugpy
```

Set a breakpoint in VSCode normally by clicking the red dot on the left of the line number. You can
set the breakpoint in user code, UDF code or PySpark engine code.

Then, instead of run your script with

```
python your_script.py
```

Run it with the script wrapper we provided

```
python/run-with-vscode-breakpoints python your_script.py
```

Your VSCode debugger should be brought up automatically when the breakpoint is hit.

This also works for `run-tests` or any other scripts - under the hood it just sets two environment variables.
If you want, you can set them manually.

```
python/run-with-vscode-breakpoints python/run-tests --testnames="..."
```

`daemon` is not tracked by default because of potential issues for fork. However if you need to debug `daemon`
you can do

```
python/run-with-vscode-breakpoints --hook-daemon your_script.py
```
