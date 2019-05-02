# rapidminer.core.server

## Server

Class for using a local or remote RapidMiner Server instance directly. You can read from and write to the Server repository and you can execute processes using the scalable Job Agent architecture.

```python
Server(self, url='http://localhost:8080', username=None, **kwargs)
```

Initializes a new connector to a local or remote Rapidminer Server instance. It also installs the auxiliary webservice required by this library to be able to interact with the Server repository directly.

Arguments:
- `url`: Server url path (hostname and port as well)
- `username`: user to use Server with

Possible `kwargs` arguments:
- `password`: password for the username. If not provided, you will need to enter it.
- `webservice`: this API requires an auxiliary process installed as a webservice on the Server instance. This parameter specifies the name for this webservice. The webservice is automatically installed if it has not been.
- `processpath`: path in the repository where the process behind the webservice will be saved. If not specified, a user prompt asks for the path, but proposes a default value.
- `tempfolder`: repository folder on Server that can be used for storing temporary objects by run_process method. Default value is "tmp" inside the user home folder. Note that in case of certain failures, you may need to delete remaining temporary objects from this folder manually.
- `install`: boolean. If set to false, webservice installation step is completely skipped.

### read_resource
```python
Server.read_resource(self, input)
```

Reads the resource from the specified Server repository location

Arguments:
- `input`: the path(s) to the resource(s) inside Server repository

Returns: 
- the resource(s) as a pandas DataFrame(s). If multiple inputs are specified, the same number of inputs will be returned, as tuple of DataFrame objects. Otherwise, the return value is a single DataFrame.

### write_resource
```python
Server.write_resource(self, dataframe, output)
```

Writes the pandas DataFrame to the Server repository

Arguments:
- `dataframe`: the pandas DataFrame(s). Multiple DataFrames can be specified as list or tuple.
- `output`: the path(s) to the resource(s) inside Server repository. The same number of outputs is required as the number of dataframes.

### run_process
```python
Server.run_process(self, path, inputs=None, **kwargs)
```

Runs a RapidMiner process.

Arguments:
- `path`: path to the *.rmp RapidMiner process file.
- `inputs`: inputs used by the RapidMiner process, as a list of pandas DataFrame objects or a single pandas DataFrame.
- `ignore_cleanup_errors`: boolean. Determines if any error during temporary data cleanup should lead to an error. Default value is True.

Possible `kwargs` arguments:
- `queue`: the name of the queue to submit the process to. Default is DEFAULT
- `macros`: optional dict that sets the macros in the process context according to the key-value pairs

Returns:
- the results of the RapidMiner process, as a list of pandas DataFrame objects.

### getQueues
```python
Server.getQueues(self)
```

Gets information of the available queues in the Server instance

Returns:
- a JSON array of objects representing each queue with its properties

