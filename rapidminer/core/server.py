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
import requests
import numpy as np
import base64
import tempfile
import jwt
import json
import xml.etree.ElementTree as et
import pkg_resources
pkg_resources.require("pandas>=0.23.0")
import pandas as pd
import pickle
import os
import getpass
from time import sleep
from .connector import Connector
from .utilities import ServerException
from .utilities import check_for_error
import uuid

class Server(Connector):
    """
    Class for using a local or remote RapidMiner Server instance directly. You can read from and write to the Server repository and you can execute processes using the scalable Job Agent architecture.
    """
    __POLL_INTERVAL_SECONDS = 6
    __WEBSERVICE_PROCESS_XML = \
        """<?xml version="1.0" encoding="UTF-8"?><process version="9.3.000">
          <context>
            <input/>
            <output/>
            <macros/>
          </context>
          <operator activated="true" class="process" compatibility="9.3.000" expanded="true" name="Process">
            <process expanded="true">
              <operator activated="true" class="python_scripting:repository_service" compatibility="9.3.000" expanded="true" height="68" name="Repository Access" width="90" x="179" y="34"/>
              <connect from_port="input 1" to_op="Repository Access" to_port="file"/>
              <connect from_op="Repository Access" from_port="output" to_port="result 1"/>
              <portSpacing port="source_input 1" spacing="0"/>
              <portSpacing port="source_input 2" spacing="0"/>
              <portSpacing port="sink_result 1" spacing="0"/>
              <portSpacing port="sink_result 2" spacing="0"/>
            </process>
          </operator>
        </process>
        """
    __WEBSERVICE_DESCRIPTOR_XML = \
        """<?xml version="1.0" encoding="UTF-8"?>
        <exported-process>
            <mime-type>application/json</mime-type>
            <output-format>JSON</output-format>
            <process-entry>PROCESS_ENTRY_PATH</process-entry>
            <xslt-entry/>
            <parameter-mappings/>
            <properties>
                <name>service:Repository Service</name>
            </properties>
            <data-source-input>
                <name>service:Repository Service</name>
            </data-source-input>
        </exported-process>
        """
    
    def __init__(self, url='http://localhost:8080', username=None, **kwargs):
        """
        Initializes a new connector to a local or remote Rapidminer Server instance. It also installs the auxiliary webservice required by this library to be able to interact with the Server repository directly.

        Arguments:
        :param url: Server url path (hostname and port as well)
        :param username: user to use Server with

        Possible kwargs arguments:
        :param password: password for the username. If not provided, you will need to enter it.
        :param webservice: this API requires an auxiliary process installed as a webservice on the Server instance. This parameter specifies the name for this webservice. The webservice is automatically installed if it has not been.
        :param processpath: path in the repository where the process behind the webservice will be saved. If not specified, a user prompt asks for the path, but proposes a default value.
        :param tempfolder: repository folder on Server that can be used for storing temporary objects by run_process method. Default value is "tmp" inside the user home folder. Note that in case of certain failures, you may need to delete remaining temporary objects from this folder manually.
        :param install: boolean. If set to false, webservice installation step is completely skipped.
        """
        super(Server, self).__init__(**kwargs)
        # URL of the Rapidminer Server
        self.server_url = url
        # RapidMiner Server Username
        if username == None:
            self.username = input('Username: ')
        else:
            self.username = username
        # RapidMiner Server Password
        if "password" in kwargs:
            self.__password = kwargs["password"]
        else:
            self.__password = getpass.getpass(prompt='Password: ')
        if "webservice" in kwargs:
            self.webservice = kwargs["webservice"]
        else:
            self.webservice = "Repository Service"
        if "processpath" in kwargs:
            self.__processpath = kwargs["processpath"]
        else:
            self.__processpath = None
        if "tempfolder" in kwargs:
            self.__tempfolder = kwargs["tempfolder"]
            self.__tempfolder += "/" if not self.__tempfolder.endswith("/") else ""
        else:
            self.__tempfolder = "/home/" + self.username + "/tmp/"
        if "install" in kwargs:
            self.__install = kwargs["install"]
        else:
            self.__install = True
        
        # Connect to the RM Server
        self.__connect()
        
        # Test and install required web service if it does not exist
        if self.__install:
            self.__test_and_install()

####################
# Public functions #
####################

    def read_resource(self, input):
        """
        Reads the resource from the specified Server repository location

        :param input: the path(s) to the resource(s) inside Server repository
        :return: the resource(s) as a pandas DataFrame(s). If multiple inputs are specified, the same number of inputs will be returned, as tuple of DataFrame objects. Otherwise, the return value is a single DataFrame.
         """
        if not ((isinstance(input, tuple) or isinstance(input, list))):
            input = [input]
            single_input = True
        else:
            single_input = False
        resources = []
        for inp in input:
            post_url = self.server_url + "/api/rest/process/" + self.webservice + "?"
            r = requests.post(post_url, json={"command": "load", "path": inp}, headers=self.auth_header)
            if r.status_code != 200:
                raise ServerException("Failed to read input \"" + inp + "\", status: " + str(r.status_code))
            response = check_for_error(r)
            dataset = pd.read_json(json.dumps(response))
            resources.append(dataset)
        if single_input:
            return resources[0]
        else:
            return tuple(resources)

    def write_resource(self, dataframe, output):
        """
        Writes the pandas DataFrame to the Server repository

        :param dataframe: the pandas DataFrame(s). Multiple DataFrames can be specified as list or tuple.
        :param output: the path(s) to the resource(s) inside Server repository. The same number of outputs is required as the number of dataframes.
        """
        if not ((isinstance(dataframe, tuple) or isinstance(dataframe, list))):
            dataframe = [dataframe]
        if not ((isinstance(output, tuple) or isinstance(output, list))):
            output = [output]
            
        if len(dataframe) != len(output):
            raise ValueError("dataframe and output must contain the same number of values")
        for i in range(len(dataframe)):
            post_url = self.server_url + "/api/rest/process/" + self.webservice + "?"
            data = json.loads(dataframe[i].to_json(orient="table", index=False))
            r = requests.post(post_url, json={"command": "save", "path": output[i], "data": data}, headers=self.auth_header)
            if r.status_code != 200:
                raise ServerException("Failed to save input no. " + str(i) + ", status: " + str(r.status_code))
            if len(r.content) > 0:
                try:
                    check_for_error(r)
                except:
                    # ignore, as there is not necessarily an output
                    return

    def run_process(self, path, inputs=None, **kwargs):
        """
        Runs a RapidMiner process.

        Arguments:
        :param path: path to the *.rmp RapidMiner process file.
        :param inputs: inputs used by the RapidMiner process, as a list of pandas DataFrame objects or a single pandas DataFrame.

        Possible kwargs arguments:
        :param queue: the name of the queue to submit the process to. Default is DEFAULT
        :param macros: optional dict that sets the macros in the process context according to the key-value pairs
        :param ignore_cleanup_errors: boolean. Determines if any error during temporary data cleanup should lead to an error. Default value is True
        :return: the results of the RapidMiner process, as a list of pandas DataFrame objects.
        """
        if inputs is not None and not ((isinstance(inputs, tuple) or isinstance(inputs, list))):
            inputs = [inputs]
        if "queue" in kwargs:
            queue = kwargs["queue"]
        else:
            queue = "DEFAULT"
        if "macros" in kwargs:
            macros = kwargs["macros"]
        else:
            macros = None
        if "ignore_cleanup_errors" in kwargs:
            ignore_cleanup_errors = kwargs["ignore_cleanup_errors"]
        else:
            ignore_cleanup_errors = True

        process_xml = self.__read_process_xml(path)
        root = et.fromstring(process_xml)
        temp_resources = []
        context = {}
        try:
            if inputs != None and len(inputs) > 0:
                input_resources = [self.__tempfolder + next(tempfile._get_candidate_names()) for _ in inputs]
                temp_resources += input_resources
                self.write_resource(inputs, input_resources)
                # add input locations in process xml
                context["inputLocations"] = input_resources
            # find connected output ports, add locations to process xml
            output_resources = []
            for wire in root.find('operator').find('process').findall('connect'):
                if wire.attrib['to_port'].startswith('result '):
                    output_resources.append(self.__tempfolder + next(tempfile._get_candidate_names()))
            if len(output_resources) > 0:
                context["outputLocations"] = output_resources
            temp_resources += output_resources
            # set macros in process xml
            if macros != None:
                macros_dict = {}
                for key, value in macros.items():
                    macros_dict[key] = value
                context["macros"] = macros_dict
            r = self.__submit_process_xml(queue, process_xml, path, context)
            if r.status_code != 200:
                raise ServerException("Failed to submit process, status: " + str(r.status_code))
            jobid = r.json()["id"]
            print("Submitted process with job id:", jobid)
            self.__wait_for_job(jobid)
            res = self.read_resource(output_resources)
            if not isinstance(res, tuple):
                return [res]
            return list(res)
        finally:
            if ignore_cleanup_errors:
                try:
                    self.__delete_resource(temp_resources)
                except Exception as e:
                    strfile = "file" if len(temp_resources) == 1 else "files"
                    message = e.message if hasattr(e, 'message') else str(e)
                    print("Could not delete the following temporary " + strfile + ", error: " + message)
                    print("\n".join(t for t in temp_resources))
            else:
                self.__delete_resource(temp_resources)

    def getQueues(self):
        """
        Gets information of the available queues in the Server instance
        
        :return: a JSON array of objects representing each queue with its properties
        """
        get_url = self.server_url + "/executions/queues?"
        r = requests.get(get_url, headers=self.auth_header)
        if r.status_code != 200:
            raise ServerException("Failed to get queues, status: " + str(r.status_code))
        return r.json()

#####################
# Private functions #
#####################

    def __connect(self):
        # Encode the basic Authorization header
        userAndPass = base64.b64encode(bytes(self.username + ":" + self.__password, 'utf-8')).decode("ascii")
        headers = { 'Authorization' : 'Basic %s' %  userAndPass }

        r = requests.get(url=self.server_url + '/internal/jaxrest/tokenservice', headers=headers)
        
        # JWT idToken for the RM Server
        self.idToken = r.json()['idToken']

        # Bearer Authorization header
        self.auth_header = { 'Authorization' : 'Bearer %s' %  self.idToken }
        
        # RM Server Client Info
        self.tokenDecoded = jwt.decode(self.idToken, verify=False)
        if r.status_code == 200:
            print("Successfully connected to the Server")
        else:
            raise ServerException("Connection error, status: " + str(r.status_code))

    def __test_and_install(self):
        # test if webservice exists
        post_url = self.server_url + "/api/rest/process/" + self.webservice + "?"
        r = requests.post(post_url, json={"command": "test"}, headers=self.auth_header)
        if r.status_code == 404:
            print("Webservice is not installed, installing it with the name '" + self.webservice + "'...")
            default_webservice_path = "/home/" + self.username + "/" + self.webservice
            webservice_path = input("Please enter repository path for installing the webservice [" + default_webservice_path + "]: ") if self.__processpath == None else self.__processpath
            if webservice_path.strip() == "":
                webservice_path = default_webservice_path
            self.__install_webservice(webservice_path)
            # Re-test installed service
            r = requests.post(post_url, json={"command": "test"}, headers=self.auth_header)
            if r.status_code != 200:
                raise ServerException("Test of installed webservice failed, status: " + r.status_code)
            print("Webservice installed successfully")
        elif r.status_code == 200:
            check_for_error(r)   
        else:
            raise ServerException("Webservice test failed with unexpected error, status: " + r.status_code \
                                  + ". Make sure that the webservice with the name '" + self.webservice + ' is installed.')
    
    def __read_process_xml(self, path):
        get_url = self.server_url + "/api/rest/resources" + path
        r = requests.get(get_url, headers=self.auth_header)
        if r.status_code != 200:
            raise ServerException("Failed to get process \"" + path + "\", status: " + str(r.status_code))
        return r.text

    def __submit_process_xml(self, queue, process, location, context):
        post_url = self.server_url + "/executions/jobs?"
        body = { 
            "queueName": queue, 
            "process": base64.b64encode(bytes(process, 'UTF-8')).decode("ascii"), 
            "location": location, 
            "context": context
        }
        return requests.post(url=post_url, json=body, headers=self.auth_header)

    __JOB_STATE_ERROR = ("TIMED_OUT", "STOPPED", "ERROR")
    __JOB_STATE_SUCCESS = ("FINISHED")
    
    def __wait_for_job(self, jobid):
        while True:
            sleep(self.__POLL_INTERVAL_SECONDS)
            get_url = self.server_url + "/executions/jobs/" + jobid
            r = requests.get(get_url, headers=self.auth_header)
            if r.status_code != 200:
                raise ServerException("Error during getting job status, job id: " + jobid + ", status: " + r.status_code)
            r = r.json()
            if r["state"] in self.__JOB_STATE_ERROR:
                raise ServerException("Job finished with error state: " + r["state"] + ", " + Server.__format_job_error(r))
            elif r["state"] in self.__JOB_STATE_SUCCESS:
                return
    
    def __format_job_error(response):
        # TODO: improve
        return "Unknown error" if "error" not in response else response["error"]["type"] + ": " + response["error"]["title"] + ": " + response["error"]["message"]
    
    def __delete_resource(self, resource_paths):
        post_url = self.server_url + "/api/rest/process/" + self.webservice + "?"
        for path in resource_paths:
            r = requests.post(post_url, json={"command": "del", "path": path}, headers=self.auth_header)
            if r.status_code != 200:
                raise ServerException("Failed to delete path \"" + path + "\", status: " + str(r.status_code))
    
    def __install_webservice(self, path):
        self.__postProcess(path, self.__WEBSERVICE_PROCESS_XML)
        self.__postService(self.webservice, self.__WEBSERVICE_DESCRIPTOR_XML.replace("PROCESS_ENTRY_PATH", path))

    def __postProcess(self, path, process):
        post_url = self.server_url + "/api/rest/resources" + path
        head = self.auth_header.copy()
        head['Content-Type'] = 'application/vnd.rapidminer.rmp+xml'
        r = requests.post(post_url, headers=head, data=process)
        if r.status_code != 201:
            raise ServerException("Failed to save process to repository path '" + path + "', status: " + str(r.status_code))
        return r

    def __postService(self, serviceName, descriptor):
        post_url = self.server_url + "/api/rest/service/" + serviceName
        r = requests.post(post_url, auth=(self.username, self.__password), data=descriptor)
        if r.status_code != 200:
            raise ServerException("Failed to install webservice with the name '" + serviceName + "', status: " + str(r.status_code))
        return r
