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
import json
import logging
import sys
import threading

class Connector(object):
    """
    Base class for interacting with RapidMiner. The subclasses of this class should be used.
    """
    __id_counter__ = 0
    __lock__ = threading.Lock()


    def __init__(self, **kwargs):
        """
        Possible kwargs arguments:
        :param logger: a Logger object to use. By default a very simple logger is used, with INFO level, logging to
                        stdout.
        :param loglevel: the loglevel, as an int value. Common values are defined in the standard logging module. Only
                        used, if logger is not defined.
        """
        Connector.__lock__.acquire()
        try:
            self.__id__ = Connector.__id_counter__
            Connector.__id_counter__ = Connector.__id_counter__ + 1
        finally:
            Connector.__lock__.release()
        if "logger" in kwargs:
            self.logger = kwargs["logger"]
        else:
            if "loglevel" in kwargs:
                lglvl = kwargs["loglevel"]
            else:
                lglvl = logging.INFO
            formatter=logging.Formatter("%(asctime)s [%(levelname)s -- %(source)s]: %(message)s")
            syslog=logging.StreamHandler(sys.stdout)
            syslog.setFormatter(formatter)
            self.logger = logging.getLogger(self.__class__.__name__ + "@" + str(self.__id__))
            self.logger.setLevel(lglvl)
            self.logger.addHandler(syslog)

    def log(self, msg, level=logging.INFO, source="python"):
        """
        Logs a message with the defined log level.

        :param msg: the message to log.
        :param level: the log level as an integer.
        :param source: source of the message, default is 'python'.
        :return:
        """
        self.logger.log(msg=msg.strip(), level=level, extra={"source": source})

    def read_resource(self, input):
        """
        Reads the resource from the given repository location/file

        :param input: the path(s) to the resource(s). If no extension is specified, the path is treated as a repository location. If file extension is specified, it is treated as a file.
        :return: the resource(s) as a pandas DataFrame(s). If multiple inputs are specified, the same number of inputs will be returned, as tuple of DataFrame objects.
        """
        raise NotImplementedError("Method not implemented in base class.")

    def write_resource(self, dataframe, output):
        """
        Writes the pandas DataFrame to RapidMiner repository/regular file.

        :param dataframe: the pandas DataFrame(s). (Multiple DataFrame-s can be specified as list or tuple).
        :param output: the path(s) to the resource(s). The same number of output(s) is required as the number of dataframe(s). If no extension is specified, the path is treated as a repository location. If file extension is specified, it is treated as a file.
        """
        raise NotImplementedError("Method not implemented in base class.")


    def run_process(self, path, inputs=None, **kwargs):
        """
        Runs a RapidMiner process.

        Arguments:
        :param path: path to the *.rmp RapidMiner process file.
        :param inputs: inputs used by the RapidMiner process, as a list of pandas DataFrame objects or a single pandas DataFrame.

        Possible kwargs arguments:
        :param operator: the name of th RapidMiner operator to execute. If None (default) the whole process is executed.
        :return: the results of the RapidMiner process, as a list of pandas DataFrame objects.
        """
        raise NotImplementedError("Method not implemented in base class.")

    def _suppress_pandas_warning(self, f):
        try:
            import warnings
        except:
            f()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            f()

    def _can_convert_to_str(self, value):
        """
        Tests, if the given value can be converted to a string representation.

        Taken from the legacy wrapper.py code (isstringable).

        :param value: value to test.
        :return: True, if value can be converted to string, False otherwise.
        """
        try:
            str(value)
            return True
        except:
            return False

    def _rename_invalid_columns(self, columns):
        """
        Renames the invalid column names. Column names must be not empty and not a single number.

        Taken -- with some modification -- from the legacy wrapper.py code (checkColumNames).

        :param columns: list of DataFrame columns.
        :return:
        """
        if any(self._can_convert_to_str(value) and ((not str(value)) or str(value).isdigit()) for value in columns):
            return ['att'+str(value) if (self._can_convert_to_str(value) and ((not str(value)) or str(value).isdigit())) else str(value) for value in columns]
        else:
            return columns

    # TODO refactor this method to reduce its cognitivy complexity (44->15)
    def _write_metadata(self, data, text_file):
        """
        Writes the meta data to a stream (a file object with text type)
        uses the meta data from rm_metadata attribute if present
        otherwise deduces the type from the data and sets no special role.

        Taken -- with some modification -- from the legacy wrapper.py code (handleMetaData).

        :param data: the pandas DataFrame.
        :param text_file: the file object representing a text resource (e.g. the result of 'open("myfile.txt", "r", encoding="utf-8")')
        :return:
        """
        metadata = {}
        #check if rm_metadata attribute is present and a dictionary
        try:
            if isinstance(data.rm_metadata, dict):
                meta_isdict = True
            else:
                meta_isdict = False
                if data.rm_metadata is not None:
                    self.log("'rm_metadata' must be a dictionary.", level=logging.WARNING)
        except:
            meta_isdict = False

        for name in data.columns.values:
            try:
                meta = data.rm_metadata[name]
                if isinstance(meta, tuple) and len(meta) == 2 and meta_isdict:
                    meta_type, meta_role = meta
                else:
                    if meta_isdict and meta is not None:
                        self.log("'rm_metadata[" + name + "]' must be a tuple of length 2, e.g. data.rm_metadata['column1']=('binominal','label')", level=logging.WARNING)
                    if isinstance(meta, tuple) or isinstance(meta, list) and len(meta) > 0:
                        meta_type = meta[0]
                    else:
                        try:
                            meta_type = str(meta)
                        except:
                            meta_type = None
                    if isinstance(meta, tuple) or isinstance(meta, list) and len(meta) > 1:
                        meta_role = meta[1]
                    else:
                        meta_role = None
            except:
                meta_type = None
                meta_role = None

            if meta_role is None:
                meta_role = 'attribute'
            #choose type by dtype of the column
            if meta_type is None:
                kind_char = data.dtypes[name].kind
                if kind_char in ('i','u'):
                    meta_type = 'integer'
                elif kind_char in ('f'):
                    meta_type = 'real'
                elif kind_char in ('M'):
                    meta_type = 'date_time'
                elif kind_char in ('b'):
                    meta_type = 'binominal'
                else:
                    meta_type = 'polynomial'
            metadata[name] = [meta_type, meta_role]
        #store as json
        try:
            json.dump(metadata, text_file)
        except Exception as e:
            self.log("Failed to send meta data from Python script to RapidMiner (reason: " + str(e) + ").", level=logging.WARNING)

    def _set_metadata(self, df, metadata):
        df.rm_metadata = metadata

    def _copy_dataframe(self, df):
        """
        Returns a copy of the metadata. Handles the special 'rm_metadata' attribute as well.

        :param df: a pandas DataFrame.
        :return: copy of the pandas DataFrame.
        """
        copy = df.copy()
        if hasattr(df, "rm_metadata"):
            self._suppress_pandas_warning(lambda: self._set_metadata(copy, df.rm_metadata))
        return copy;
