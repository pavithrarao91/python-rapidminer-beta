# rapidminer

## Studio
Class for using a locally installed RapidMiner Studio instance. You can read from and write to the repositories defined in Studio (and use even remote repositories this way) and you can execute processes.

```python
Studio(self, studio_home=None, **kwargs)
```

Initializes a new connector to a local Rapidminer Studio instance. Every command will launch a new Studio instance, executing the required operations in batch mode.

Arguments:
- `studio_home`: path to installation directory of the Rapidminer Studio. If None, the location will be taken from the RAPIDMINER_HOME environment variable if defined, or the current directory, if RAPIDMINER_HOME is not defined.

Possible `kwargs` arguments:
- `logger`: a Logger object to use. By default a very simple logger is used, with INFO level, logging to stdout.
- `loglevel`: the loglevel, as an int value. Common values are defined in the standard logging module. Only used, if logger is not defined.
- `rm_stdout`: the output stream to redirect the output of underlying Studio launches. By default the output is directed to the logger associated with this connector. Log records from Studio are labeled with new element 'key'='studio', while the logs from python with 'key'='python'.
- `password`: password for a remote repository, if its password is not saved

### read_resource
```python
Studio.read_resource(self, input)
```

Reads the resource(s) from the given repository location(s) / file(s)

Arguments:
- `input`: the path(s) to the resource(s). If no extension is specified, the path is treated as a repository location. If file extension is specified, it is treated as a file.

Returns: 
- the resource(s) as pandas DataFrame(s), a pickle-able python object(s) or a file-like object(s). If multiple inputs are specified, the same number of inputs will be returned, as a tuple of objects.

### write_resource
```python
Studio.write_resource(self, object, output)
```

Writes the pandas DataFrame(s) to RapidMiner repository location(s) / regular file(s).

Arguments
- `object`: can be a pandas DataFrame, a pickle-able python object or a file-like object. Multiple items can be specified as list or tuple.
- `output`: the path(s) to the resource(s). The same number of outputs is required as the number of dataframes. If no extension is specified, the path is treated as a repository location. If file extension is specified, it is treated as a file.

### run_process
```python
Studio.run_process(self, path, inputs=None, **kwargs)
```

Runs a RapidMiner process.

Arguments:
- `path`: path to the *.rmp RapidMiner process file.
- `inputs`: inputs used by the RapidMiner process, can be a pandas DataFrame, a pickle-able python object or a file-like object.

Possible `kwargs` arguments:
`operator`: the name of the RapidMiner operator to execute. If None (default) the whole process is executed.

Returns:
the results of the RapidMiner process, as a list of pandas DataFrame objects.

