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
import pandas as pd
import requests
import json
from .utilities import ServerException
from .utilities import check_for_error

class Scoring:
    """
    Class that allows you to use the Real-Time Scoring agent directly on a dataset.
    """

    def __init__(self, hostname, endpoint):
        self.url = hostname + "/services/" + endpoint

    def predict(self, dataframe):
        """
        Calls the Real-Time Scoring agent on the specified dataset and returns the result.
        
        :param dataframe: the pandas DataFrame.
        :return: the result as a pandas DataFrame.
        """
        df_json = dataframe.to_json(orient="table")

        headers = { 'Content-type': 'application/json' }
        r = requests.post(self.url, data=df_json, headers=headers)
        if r.status_code != 200:
            raise ServerException("Could not score data, status:", r.status_code)
        
        response = r.json()
        check_for_error(response)
        json_string = json.dumps(response["data"])
        df_out = pd.read_json(json_string)

        return df_out
