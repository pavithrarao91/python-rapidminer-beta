# RapidMiner Python package - BETA version

This Python package allows you to interact with RapidMiner Studio and Server. You can collaborate using the RapidMiner repository and leverage the scalable Server infrastructure to run processes. This document shows examples on how to use the package. Additional notebook files provide more advanced examples. There is an API document for each classes: [Studio](docs/Studio.md), [Server](docs/Server.md), [Scoring](docs/Scoring.md).

## Table of contents

- [Requirements](#requirements)
- [Known current limitations](#known-current-limitations)
- [Overview](#requirements)
- [Installation](#installation)
- [Studio](#studio)
- [Server](#server)
- [Scoring](#scoring)

## Requirements

* RapidMiner Studio 9.3.0-BETA for Studio class
* RapidMiner Server 9.3.0-BETA for Server class
* Python Scripting extension 9.3.0-BETA installed in both cases

## Known current limitations

This BETA version is not intended to be used in a production environment.

* Python version: 
  * Tests were only carried out using *Python 3.7*
  * Python 2 is currently not supported, but it may be supported in the GA release or in the near future
  * Please report if you run into problems using version Python 3.x
* Server read and write methods can only handle data currently, Studio read and write methods can handle other objects as well
* Studio class: Encoding may cause failure when there are non-ascii characters in the path or in macros. Please report other encoding issues that you run into
* Studio class: If logging is disabled, e.g. `os.devnull` is used, and there is an error, you will see a different error than the real one - this will be fixed
* Studio class: Accessing a remote repository is possible if password is saved locally. If not, `password` field could be used, but it does not work yet 

## Overview

Both Studio and Server classes provide a read and a write method for reading / writing data and other objects, and a run method to run processes. The method signatures are the same, with somewhat different extra parameters.

Studio class requires a local Studio installation and is suitable in the following cases:
* Implementing certain data science steps in Python using your favorite IDE or notebook implementation. You may even use the resulting code afterwards in a RapidMiner process within an *Execute Python* operator.
* You are using coding primarily, but you want to incorporate methods that are impemented in a RapidMiner process.
* Creating batch tasks that also interact with the repository and / or run processes.

Server class connects directly to a Server instance without the need of a Studio installation. It is suitable in the following cases:
* Collaborating with RapidMiner users, sharing data easily.
* Calling, running, scheduling processes on the RapidMiner Server platform from a local script.

## Installation

The BETA version can be installed from this repository:

- install in one step:

        $ pip install git+https://github.com/rapidminer/python-rapidminer-beta.git

- or clone and install:

        $ git clone https://github.com/dadadel/python-rapidminer-beta.git
        $ cd python-rapidminer-beta
        $ python setup.py install

## Studio

You need to have a locally installed RapidMiner Studio instance to use this class. The only thing you need to provide is your installation path. Once that is specified, you can read from and write data or other objects to any configured repository. You can also run processes from files or from the repository. In this section, we show you some examples on how to read and write repository data and run processes. For more advanced scenarios see the included [IPython notebook](examples/studio_examples.ipynb) and the [documentation of the `Studio` class](docs/Studio.md).

First you need a `Connector` object to interact with Studio. Once you have that, you can read and write data or  run a process with a single line. To create a `Studio` `Connector` object, run the following code:

```python
import rapidminer
connector = rapidminer.Studio("/path/to/you/studio/installation")
```

where you replace `"/path/to/you/studio/installation"` with the location of your Studio installation. In case of Windows, a typical path is `"C:\Program Files\RapidMiner\RapidMiner Studio"`. In case of Mac, the path is usually `"/Applications/RapidMiner Studio.app/Contents/Resources/RapidMiner-Studio"`. Alternatively you can define this location via the `RAPIDMINER_HOME` environment variable.

##### Reading ExampleSet

Once you have a connector instance, you can read a RapidMiner ExampleSet in Python by running the following line:

```python
df = connector.read_resource("//Samples/data/Iris")
```

The resulting `df` is a `pandas` `DataFrame` object, which you can use in the conventional way.

##### Writing ExampleSet

You can save any `pandas` `DataFrame` object to a RapidMiner repository (or file) with the following command:

```python
connector.write_resource(df, "//Local Repository/data/mydata")
```

where `df` is the `DataFrame` object you want to write to the repository, and `"//Local Repository/data/mydata"` is the location where you want to store it.

##### Running a process

To run a process execute the following line:

```python
[df] = connector.run_process("//Samples/processes/02_Preprocessing/01_Normalization")
```

You will get the results as `pandas` `DataFrames`. You can also define inputs, and many more. For more examples, see the [examples notebook](examples/studio_examples.ipynb)

## Server

With `Server` class, you can directly connect to a local or remote Server instance without the need for any local RapidMiner (Studio) installation. You can read data from and write data to the Server repository and you can execute processes using the scalable Job Agent architecture. In this section, we show you some examples on how to read and write repository data and run processes. For more advanced scenarios see the included [IPython notebook](examples/server_examples.ipynb) and the [documentation of the `Server` class](docs/Server.md).

The `Server` class requires a webservice backend to be installed on RapidMiner Server. This is done automatically on the first instantiation of the Server class. For that, you will be asked for a repository path where the webservice process will be installed. If you are satisfied with thet proposed default value, just hit Enter. This is not needed after the first run, unless you deliberately change the webservice name by the `webservice` parameter. This first run can also be fully automated, if you specify the `processpath` parameter besides `url`, `username` and `password`.

To create a `Server` `Connector` object, run the following code:

```python
import rapidminer
connector = rapidminer.Server("https://myserver.mycompany.com:8080", username="myrmuser")
```

where you replace `"https://myserver.mycompany.com:8080"` with the url of your Server instance and `"myrmuser"` with your username.

##### Reading ExampleSet

Once you have a connector instance, you can read a RapidMiner ExampleSet in Python by running the following line:

```python
df = rapidminer.read_resource("/home/myrmuser/data/mydata")
```

The resulting `df` is a `pandas` `DataFrame` object, which you can use in the conventional way.

##### Writing ExampleSet

You can save any `pandas` `DataFrame` object to the Server repository with the following command:

```python
connector.write_resource(df, "/home/myrmuser/data/myresult")
```

where `df` is the `DataFrame` object you want to write to the repository, and `"/home/myrmuser/data/myresult"` is the location where you want to store it.

##### Running a process

To run a process execute the following line:

```python
[df] = connector.run_process("/home/myrmsuer/process/transform_data", inputs=df)
```

You will get the results as `pandas` `DataFrames`. You can also define multiple inputs, and other parameters. For more examples, see the [examples notebook](examples/server_examples.ipynb)

## Scoring

This class allows you to easily use a deployed [Real-Time Scoring](https://docs.rapidminer.com/server/scoring-agent/) service. You only need to provide the Server url and the particular scoring service endpoint to create a class instance. After that, you can use the predict method to do scoring on a Pandas DataFrame and get the result in a Pandas DataFrame as well. For instructions on how to deploy Real-Time Scoring on Server, please refer to its documentation.

```python
sc = rapidminer.Scoring("http://myserver.mycompany.com:8090", "score-sales/score1")
prediction = sc.predict(df)
```

where the scoring endpoint is at `"score-sales/score1"` that can be applied to the dataset `df`, and the resulting `prediction` is a `pandas` `DataFrame` object. You can find the `Scoring` class [documentation here](docs/Scoring.md).


