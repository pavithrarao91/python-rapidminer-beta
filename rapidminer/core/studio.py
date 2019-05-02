# 
# This file is part of the RapidMiner Python package.
# 
# Copyright (C) 2018-2019 RapidMiner GmbH
# 
# This program is free software: you can redistribute it and/or modify it under the terms of the
# GNU Affero General Public License as published by the Free Software Foundation, either version 3
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
# even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License along with this program.
# If not, see https://www.gnu.org/licenses/.
# 
import shutil
import os
import subprocess
import tempfile
import glob
import sys
import logging
from threading import Thread
import threading
import platform
import io
import pandas
import json
try:
    import cPickle as pickle
except:
    import pickle
from .utilities import __STDOUT_ENCODING__
from .connector import Connector
from .resources import Resource
from .resources import File
from .resources import RepositoryLocation
from .utilities import GeneralException
from .utilities import __DEFAULT_ENCODING__
from .utilities import __open__

class StudioException(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)

class Studio(Connector):
    """
    Class for using a locally installed RapidMiner Studio instance. You can read from and write to the repositories defined in Studio (and use even remote repositories this way) and you can execute processes.
    """
    __CSV_SUFFIX=".csv"
    __MD_SUFFIX=".pmd"
    __TMP_OUTPUT_DIR_PREFIX= "rapidminer-scripting-output-"
    __TMP_INPUT_DIR_PREFIX="rapidminer-scripting-inputs-"
    ___EXIT_CODE_MSG="EXIT_CODE="
    __RAPIDMINER_ERROR_MSG="RAPIDMINER_ERROR_MSG="
    __RAPIDMINER_ERROR_MSG_FIRST_LINE="RAPIDMINER_ERROR_MSG_FIRST_LINE="

    def __init__(self, studio_home=None, **kwargs):
        """Initializes a new connector to a local Rapidminer Studio instance. Every command will launch a new Studio instance, executing the required operations in batch mode.

        Arguments:
        :param studio_home: path to installation directory of the Rapidminer Studio. If None, the location will be taken from the RAPIDMINER_HOME environment variable if defined, or the current directory, if RAPIDMINER_HOME is not defined.

        Possible kwargs arguments:
        :param logger: a Logger object to use. By default a very simple logger is used, with INFO level, logging to stdout.
        :param loglevel: the loglevel, as an int value. Common values are defined in the standard logging module. Only used, if logger is not defined.
        :param rm_stdout: the output stream to redirect the output of underlying Studio launches. By default the output is directed to the logger associated with this connector. Log records from Studio are labeled with new element 'key'='studio', while the logs from python with 'key'='python'.
        :param password: password for a remote repository, if its password is not saved - DOES NOT YET WORK
        """
        super(Studio, self).__init__(**kwargs)
        if studio_home is not None:
            self.studio_home = studio_home
        elif os.getenv("RAPIDMINER_HOME") is not None and not os.getenv("RAPIDMINER_HOME") == "":
            self.studio_home = os.getenv("RAPIDMINER_HOME")
        else:
            self.studio_home = os.getcwd()
        if not self.studio_home.endswith(os.path.sep):
            self.studio_home += os.path.sep
        if "rm_stdout" in kwargs:
            self.__rm_stdout__ = kwargs["rm_stdout"]
        else:
            self.__rm_stdout__ = None
        if "password" in kwargs:
            self.__password = kwargs["password"]
        else:
            self.__password = None
        self.override_python_binary = "override_python_binary" in kwargs and kwargs["override_python_binary"]
        self.__last_exception_msg__ = {} # ensures proper multithreading: this maps last exception message for every thread
        self.__last_exit_code__ = {} # ensures proper multithreading: this maps last exit code for every thread

####################
# Public functions #
####################

    def read_resource(self, input):
        """
        Reads the resource(s) from the given repository location(s) / file(s)

        :param input: the path(s) to the resource(s). If no extension is specified, the path is treated as a repository location. If file extension is specified, it is treated as a file.
        :return: the resource(s) as pandas DataFrame(s), a pickle-able python object(s) or a file-like object(s). If multiple inputs are specified, the same number of inputs will be returned, as a tuple of objects.
         """
        if not ((isinstance(input, tuple) or isinstance(input, list))):
            input = [input]
            single_input = True
        else:
            single_input = False
        output_dirs = [tempfile.mkdtemp(prefix=self.__TMP_OUTPUT_DIR_PREFIX) for _ in input]
        try:
            self.__run_rapidminer(input_files=list(input), output_files=[File(output_dir) for output_dir in output_dirs])
            output_files = []
            for output_dir in output_dirs:
                csv_files = glob.glob(output_dir + "/*.csv")
                if (len(csv_files) == 1):
                    output_files.append(csv_files[0])
                else:
                    output_files.append(glob.glob(output_dir + "/*")[0])
            result = tuple(self.__deserialize_from_file(output_file) for output_file in output_files)
            if single_input:
                return result[0]
            else:
                return result
        finally:
            for dir in output_dirs:
                shutil.rmtree(dir, ignore_errors=True)

    def write_resource(self, object, output):
        """
        Writes the pandas DataFrame(s) to RapidMiner repository location(s) / regular file(s).

        :param object: can be a pandas DataFrame, a pickle-able python object or a file-like object. Multiple items can be specified as list or tuple.
        :param output: the path(s) to the resource(s). The same number of outputs is required as the number of dataframes. If no extension is specified, the path is treated as a repository location. If file extension is specified, it is treated as a file.
        """
        if not ((isinstance(object, tuple) or isinstance(object, list))):
            object = [object]
        if not ((isinstance(output, tuple) or isinstance(output, list))):
            output = [output]

        if len(object) != len(output):
            raise ValueError("Object and output must contain the same number of values.")
        input_dirs = [tempfile.mkdtemp(prefix=self.__TMP_INPUT_DIR_PREFIX) for _ in object]
        try:
            input_files = [self.__serialize_to_file(obj, os.path.join(dir, "input0")) for (dir, obj) in zip(input_dirs, object)]
            self.__run_rapidminer(input_files=[File(f) for f in input_files], output_files=output)
        finally:
            for input_dir in input_dirs:
                shutil.rmtree(input_dir, ignore_errors=True)

    def run_process(self, path, inputs=None, **kwargs):
        """
        Runs a RapidMiner process.

        Arguments:
        :param path: path to the *.rmp RapidMiner process file.
        :param inputs: inputs used by the RapidMiner process, can be a pandas DataFrame, a pickle-able python object or a file-like object.

        Possible kwargs arguments:
        :param operator: the name of the RapidMiner operator to execute. If None (default) the whole process is executed.
        :return: the results of the RapidMiner process, as a list of pandas DataFrame objects.
        """
        if inputs is not None and not (isinstance(inputs, tuple) or isinstance(inputs, list)):
            inputs = [inputs]
        if "operator" in kwargs:
            operator = kwargs["operator"]
        else:
            operator = None
        if "macros" in kwargs:
            macros = kwargs["macros"]
        else:
            macros = {}
        output_dir = tempfile.mkdtemp(prefix=self.__TMP_OUTPUT_DIR_PREFIX)
        remove_dirs = [output_dir]
        try:
            input_files = []
            if (inputs is not None):
                input_dir = tempfile.mkdtemp(prefix=self.__TMP_INPUT_DIR_PREFIX)
                remove_dirs.append(input_dir)
                for i in range(len(inputs)):
                    input_files.append(File(self.__serialize_to_file(inputs[i], os.path.join(input_dir, "input" + str(i)))))
            return self.__run_process_with_output_dir(path, input_files, operator, output_dir, macros)
        finally:
            for dir in remove_dirs:
                shutil.rmtree(dir, ignore_errors=True)

#####################
# Private functions #
#####################

    def __extract_log_level(self, msg, threadid):
        # LogLevels: https://docs.python.org/2/library/logging.html#logging-levels
        if msg.startswith("FINEST: "):
            lglevel = logging.DEBUG
            msg = msg[8:]
        elif msg.startswith("FINER: "):
            lglevel = logging.DEBUG
            msg = msg[7:]
        elif msg.startswith("DEBUG: "):
            lglevel = logging.DEBUG
            msg = msg[7:]
        elif msg.startswith("CONFIG: "):
            lglevel = logging.DEBUG
            msg = msg[8:]
        elif msg.startswith("INFO: "):
            lglevel = logging.INFO
            msg = msg[6:]
        elif msg.startswith("WARNING: "):
            lglevel = logging.WARNING
            msg = msg[9:]
        elif msg.startswith("SEVERE: "):
            lglevel = logging.ERROR
            msg = msg[8:]
        elif msg.startswith(self.__RAPIDMINER_ERROR_MSG_FIRST_LINE):
            lglevel = logging.ERROR
            msg = msg[len(self.__RAPIDMINER_ERROR_MSG_FIRST_LINE):]
            self.__last_exception_msg__[threadid] = msg
        elif msg.startswith(self.__RAPIDMINER_ERROR_MSG):
            lglevel = logging.ERROR
            msg = msg[len(self.__RAPIDMINER_ERROR_MSG):]
        elif msg.startswith(self.___EXIT_CODE_MSG):
            lglevel = -1
            try:
                self.__last_exit_code__[threadid] = int(msg[10:])
            except ValueError:
                self.__last_exit_code__[threadid] = 0
        else:
            lglevel = logging.INFO
        return (msg, lglevel)

    def __print_to_console(self, process, close_process_stdout=False, threadid = -1):
        for line in iter(process.stdout.readline, b''):
            try:
                msg = line.decode(encoding=__STDOUT_ENCODING__, errors='ignore')
                if self.__rm_stdout__ is not None:
                    self.__rm_stdout__.write(msg)
                else:
                    (msg, lglevel) = self.__extract_log_level(msg, threadid)
                    self.log(msg, level=lglevel, source="studio")
            except UnicodeEncodeError:
                self.log("<could not decode row>", level=logging.DEBUG, source="studio")
        if close_process_stdout:
            process.stdout.close()

    def __start_printer_thread(self, process):
        t = Thread(target = self.__print_to_console, args=(process, False, threading.currentThread().ident))
        t.daemon = True
        t.start()

    def __quote_params(self, param, prefix=""):
        if platform.system() == "Windows":
            return prefix + param
        else:
            return '\"' + prefix + param + '\"'

    def __get_script_extension(self):
        if platform.system() == "Windows":
            return ".bat"
        else:
            return ".sh"

    def __needs_temp_dir(self, input_file):
        '''
        Returns true, if the given input file will need a temporary directory on the RapidMiner side.

        :param input_file: the file to be inspected. Files with .fo extension needs a temp dir. (file-object)
        :return: true, if the given input file will need a temporary directory on the RapidMiner side.
        '''
        if isinstance(input_file, Resource):
            input_file = input_file.to_string()
        return input_file.endswith(".fo")

    # TODO refactor this method to reduce its cognitive complexity from 26 to the allowed 15...
    def __run_rapidminer(self, process=None, input_files=[], output_files=[], output_dir=None, macros={}, operator=None):
        kwargs = {"stdout": subprocess.PIPE,
                  "stderr": subprocess.STDOUT,
                  "bufsize": 10}
        params = []
        params.append(self.studio_home + "scripts" + os.path.sep + "rapidminer-batch" + self.__get_script_extension())
        params.append(self.__quote_params("rmx_python_scripting:com.rapidminer.extension.pythonscripting.launcher.ExtendedCmdLauncher", prefix="-C"))
        if (process is not None):
            if not isinstance(process, Resource):
                process = RepositoryLocation(name=process)
            params.append(self.__quote_params(process.to_string(), prefix="-P"))
        for input_file in input_files:
            if not isinstance(input_file, Resource):
                input_file = RepositoryLocation(name=input_file)
            params.append(self.__quote_params(input_file.to_string(), prefix="-I"))
        for output_file in output_files:
            if not isinstance(output_file, Resource):
                output_file = RepositoryLocation(name=output_file)
            params.append(self.__quote_params(output_file.to_string(), prefix="-O"))
        if output_dir is not None:
            params.append(self.__quote_params(output_dir, prefix="-D"))
        if operator is not None:
            params.append(self.__quote_params(operator, prefix="-N"))
        if self.__password is not None:
            params.append(self.__quote_params(self.__password, prefix="-X"))
        if len(macros) > 0:
            for key in macros:
                params.append(self.__quote_params(str(key) + "=" + str(macros[key]), prefix="-M"))
        if any(self.__needs_temp_dir(input) for input in input_files):
            temp_dir = tempfile.mkdtemp(prefix=self.__TMP_OUTPUT_DIR_PREFIX)
            params.append(self.__quote_params(temp_dir, prefix="-T"))
        else:
            temp_dir = None
        if self.override_python_binary:
            params.append(self.__quote_params(sys.executable, prefix="-B"))
        threadid = threading.currentThread().ident
        if threadid in self.__last_exit_code__:
            del self.__last_exit_code__[threadid]
        if threadid in self.__last_exception_msg__:
            del self.__last_exception_msg__[threadid]
        try:
            p = subprocess.Popen(params, **kwargs)
            try:
                self.__start_printer_thread(p)
                p.wait()
                if threadid in self.__last_exit_code__ and self.__last_exit_code__[threadid] != 0:
                    if threadid in self.__last_exception_msg__:
                        raise StudioException("Error while executing studio: " + self.__last_exception_msg__[threadid])
                    else:
                        raise StudioException("Error while executing studio - unkown error.")
            finally:
                p.stdout.close()
        finally:
            if temp_dir is not None:
                shutil.rmtree(temp_dir, ignore_errors=True)

    def __run_process_with_output_dir(self, path, input_files, operator, output_dir, macros):
        self.__run_rapidminer(process=path, input_files=input_files, output_dir=output_dir, macros=macros, operator=operator)
        outputs = glob.glob(os.path.join(output_dir, "*.*"))
        outputs.sort()
        result = []
        for output in outputs:
            if not output.endswith(".pmd"):
                result.append(self.__deserialize_from_file(output))
        return result

    def __serialize_dataframe(self, df, streams):
        """
        Serializes a pandas DataFrame to CSV, using the format reuqired by RapidMiner Read CSV operator.

        :param df: the pandas DataFrame.
        :param streams: list of (text) file objects. The list should contain to objects, the first is foir the actual data (csv), the second for the metadata (pmd).
        :return:
        """
        dfc = self._copy_dataframe(df) # make a copy, as the column names may be modified
        dfc.columns = self._rename_invalid_columns(dfc.columns)
        dfc.to_csv(streams[0], index=False, encoding=__DEFAULT_ENCODING__)
        self._write_metadata(dfc, streams[1])

    def __serialize_dataframe_to_file(self, df, basename):
        """
        Serializes a pandas DataFrame to CSV, using the format reuqired by RapidMiner Read CSV operator.

        :param df: the pandas DataFrame.
        :param basename: the base filename, without extension.
        :return:
        """
        with __open__(basename + ".csv", "w") as csv_file:
            with __open__(basename + ".pmd", "w") as meta_file:
                self.__serialize_dataframe(df, [csv_file, meta_file])
        return basename + ".csv"

    def __serialize_to_file(self, object, basename):
        """
        Serializes a python object to the appropriate file.

        :param object, a python object.
        :param basename: the base filename, without extension.
        :return:
        """
        if isinstance(object, pandas.DataFrame):
            return self.__serialize_dataframe_to_file(object, basename)
        else:
            # try to write out as a file like object first
            try:
                with open(basename + ".fo", "w", encoding=object.encoding) as outf:
                    shutil.copyfileobj(object, outf)
                return basename + ".fo"
            except AttributeError:
                try:
                    with open(basename + ".fo", "wb") as outf:
                        shutil.copyfileobj(object, outf)
                    return basename + ".fo"
                except AttributeError:
                    shutil.rmtree(basename + ".fo", ignore_errors=True)
                    with open(basename + ".bin", 'wb') as dump_file:
                        pickle.dump(object, dump_file)
                    return basename + ".bin"

    def __deserialize_dataframe_from_file(self, csv_file, md_file):
        """
        Reads a csv file into a pandas Dataframe. Code --with slight modifications -- taken from wrapper.py (readExampleSet).

        :param csv_file: the csv file to read from. Must have special format (which is created by the corresponding Java
                code in the Studio part.
        :param md_file: metadata file, containing additional column type infos created by Studio.
        :return: pandas DataFrame object, with special rm_metadata attribute present (this stores the metadata).
        """
        try:
            with __open__(md_file,'r') as md_stream:
                metadata = json.load(md_stream)
            date_set = set(['date','time','date_time'])
            date_columns = []
            meta_dict={}
            #different iteration methods for python 2 and 3
            try:
                items = metadata.iteritems()
            except AttributeError:
                items = metadata.items()
            for key, value in items:
                #convert to tuple
                meta_dict[key]=(value[0],None if value[1]=="attribute" else value[1])
                #store date columns for parsing
                if value[0] in date_set:
                    date_columns.append(key)
            #read example set from csv
            try:
                with __open__(csv_file,'r') as csv_stream:
                    data = pandas.read_csv(csv_stream,index_col=None,encoding=__DEFAULT_ENCODING__,parse_dates=date_columns,infer_datetime_format=True)
            except TypeError:
                #if the argument inter_datetime_format is not allowed in the current version do without
                with __open__(csv_file,'r') as csv_stream:
                    data = pandas.read_csv(csv_stream,index_col=None,encoding=__DEFAULT_ENCODING__,parse_dates=date_columns)
            self._suppress_pandas_warning(lambda: self._set_metadata(data, meta_dict))
        except:
            #no metadata found or reading with meta data failed
            self.log("Failed to use the meta data.", level=logging.WARNING)
            with __open__(csv_file,'r') as csv_stream:
                data = pandas.read_csv(csv_stream,index_col=None,encoding=__DEFAULT_ENCODING__)
            self._suppress_pandas_warning(lambda: self._set_metadata(data, None))
        return data

    def __deserialize_from_file(self, filename):
        """
        Reads the given file. The acual method depends on the file extension

        :param filename: name of the file
        :return: an arbitrary python object (DataFrame, file object or any other python type pickled out)
        """
        extension = os.path.splitext(filename)[1]
        if(extension=='.csv'):
            md_file = os.path.splitext(filename)[0] + ".pmd"
            return self.__deserialize_dataframe_from_file(filename, md_file)
        elif extension=='.bin':
            with open(filename, 'rb') as f:
                try:
                    return pickle.load(f)
                except Exception as exc:
                    raise GeneralException("Error while trying to load pickled object:" + str(exc))
        elif extension=='.fo':
            with open(filename, 'rb') as f:
                return io.BytesIO(f.read()) # reads the file to memory
        else:
            raise ValueError("Cannot handle files with '" + str(extension) + "' extension.")
