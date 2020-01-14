import oauthlib.oauth2
import requests
from requests_oauthlib import OAuth2Session
from datetime import datetime
from datetime import timedelta
import pandas as pd


class Connection:
    def __init__(self, client_id, client_secret):
        """
        Parameters
        client_id : str
            Client ID provided from your ClicData ccount
        client_secret : str
            Client secret provided from your ClicData account
        """
        self.url = "https://api.clicdata.com/"
        self.token_url = self.url + "oauth20/token"
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token, self.token_expire_time, _ = self.initialize()

    ###
    # Methods
    ###
    def initialize(self):
        """Retrieve access token for ClicData API"""
        token_url = self.url + "oauth20/token"
        token_request_body = {"grant_type": "client_credentials",
                              "client_id": self.client_id,
                              "client_secret": self.client_secret}
        token = requests.post(token_url,
                              data=token_request_body)
        status_code = token.status_code
        token_body = token.json()
        access_token = token_body.get("access_token")
        expires_in = token_body.get("expires_in")
        token_expire_time = datetime.now() + timedelta(seconds=expires_in)
        return access_token, token_expire_time, status_code

    def reinitialize(self):
        """To refresh an expired token"""
        if datetime.now() >= self.token_expire_time:
          self.access_token, self.token_expire_time, _ = self.initialize()

    def get_data(self, rec_id=None, output='df'):
        """Retrieve list of data sources or retrieve the contents of a data source
        rec_id : int
            RecId of the data you want to retrieve
        output : str
            Output format, either df or dict
        """
        self.reinitialize()
        auth_header = {"Authorization": "Bearer " + self.access_token}
        endpoint = self.url + "data"
        if rec_id is None:
            data = requests.get(endpoint, headers=auth_header)
            # return data.json()
            if output == 'df':
                return pd.DataFrame.from_dict(data.json().get('data'))
            elif output == 'dict':
                return data.json().get('data')
        else:
            i = 1
            b = True
            data = []
            while b:
                d = requests.get(f"{endpoint}/{rec_id}",
                                 headers=auth_header,
                                 params={"page": i}).json()
                b = d.get('has_more_data')
                i += 1
                data = data + d.get('data')
            if output == 'df':
                return pd.DataFrame.from_dict(data)
            elif output == 'dict':
                return data
            # return pd.DataFrame.from_dict(data.json().get('data'))

    def get_data_history(self, rec_id=None, ver_id=None, output='df'):
        """Retrieve list of data sources or retrieve the contents of a data source
        rec_id : int
            RecId of the data set you want to check history for
        ver_id : int
            Version ID of the data you want to retrieve from your data set
        output : str
            Output format, either df or dict
        """
        self.reinitialize()

        auth_header = {"Authorization": "Bearer " + self.access_token}

        if rec_id is None:
            raise Exception('Please enter a valid data clone RecId.')
        else:
            if ver_id is None:
                endpoint = self.url + f"data/{rec_id}/versions"
                data = requests.get(endpoint, headers=auth_header)
                if output == 'df':
                    df = pd.DataFrame.from_dict(data.json().get('versions'))
                    df["data_rec_id"] = rec_id
                    return df
                elif output == 'dict':
                    data_dict = data.json().get('versions')
                    for version in data_dict:
                        version.update({"data_rec_id": rec_id})
                    return data_dict
            else:
                endpoint = self.url + f"data/{rec_id}/v/{ver_id}"
                i = 1
                b = True
                data = []
                while b:
                    d = requests.get(endpoint,
                                     headers=auth_header,
                                     params={"page": i})
                    if d.status_code != 200:
                        raise Exception('Please enter a valid data RecId or Version Number for your data.')
                    b = d.json().get('has_more_data')
                    i += 1
                    data = data + d.json().get('data')
                if output == 'df':
                    return pd.DataFrame.from_dict(data)
                elif output == 'dict':
                    return data

    def create_data(self, name=None, desc="", cols=None):
        """Creates an empty custom table in ClicData
        name: str
            Name of data table created in ClicData. Must be unique to account.
        desc : str
            Long form description attached to table in ClicData.
        def: dict
            Column name as key, data type as value.
        """

        self.reinitialize()

        valid_data_types = ['text','number','datetime','date','percentage','checkbox','dropdown','id']
        pandas_convert = {
            'object':'text',
            'int64':'number',
            'float64':'number',
            'bool':'checkbox',
            'datetime64':'datetime',
            'timedelta[ns]':'text',
            'category':'text'
        }
        auth_header = {"Authorization": "Bearer " + self.access_token}

        if name is None:
            raise Exception('Please enter a name for your data set')
        elif cols is None:
            raise Exception("""Please provide a valid dictionary object containing your column names and types.\n
            {"columnName":"type",\n
            "columnName":"type"}""")
        else:
            endpoint= f"{self.url}data"
            columns = []
            for i in cols.keys():
                data_type = pandas_convert[cols[i].name]
                if data_type not in valid_data_types:
                  raise Exception(f'Column [{i}] contains an invalid data type ({data_type})'+
                                  f', please enter your data with one of the following: {valid_data_types}.')
                column_def = {"name": i,
                              "data_type":data_type}
                # num = 0
                columns.append(column_def)
            # print(columns)
            body = {
                "name":name,
                "description": desc,
                "columns": columns
            }
            post = requests.post(endpoint, headers=auth_header, json=body)
            return post

    def append_data(self, rec_id=None, data=None):
        """Append your data to an existing dataset
        rec_id : int
            id of your data in ClicData
        data : pandas.Dataframe
            df containing the data you want to append
        """

        self.reinitialize()

        auth_header = {"Authorization": "Bearer " + self.access_token,
                       "accept": "application/json"}

        if rec_id is None:
          raise Exception('Please enter a valid data clone RecId.')
        elif data is None:
          raise Exception('Please enter a data set to append')
        else:
          endpoint = self.url + f'data/{rec_id}/row'
          d = data.to_dict(orient='index')
          data_set = []
          for i in d.keys():
              row = []
              num = 0
              for ii in d[i].keys():
                  cell = {"column": list(d[i])[num],
                          "value": d[i][ii]}
                  row.append(cell)
                  num += 1
              data_set.append(row)
          body = {
              "data": data_set
          }
          post = requests.post(endpoint,
                                headers=auth_header,
                                json=body)
          return post

    def static_send_data(self, name=None, desc="", data=None):
        """ Creates a static data set in ClicData using a pandas dataframe
        name : str
            Name of data set to create, must be unique to your account
        desc : str
            Optional, long-form details about your data
        data : pandas.Dataframe
            Data to upload
        """

        self.reinitialize()
        auth_header = {"Authorization": "Bearer " + self.access_token}

        if name is None:
          raise Exception('Please enter a name for your data set.')
        elif data is None:
          raise Exception('Please enter a data.')
        else:
          columns = data.dtypes.to_dict()
          # print(columns)
          rec_id = self.create_data(name=name, desc=desc,cols=columns)
          try:
             rec_id = int(rec_id.text)
          except ValueError as e:
            raise Exception(f'There appears to be an issue with the connection. Creating data set returned:\n{rec_id.text}')
          status = self.append_data(rec_id = rec_id, data=data)
        return status.text
