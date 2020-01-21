import oauthlib.oauth2
import requests
from requests_oauthlib import OAuth2Session
from datetime import datetime
from datetime import timedelta
import pandas as pd


###
# Functions that accept the Session class as input
###
def get_data(session=None, rec_id=None, output='df'):
    """Retrieve list of data sources or retrieve the contents of a data source
    session : custom class
        Session class passed from ClicData session module
    rec_id : int
        RecId of the data you want to retrieve
    output : str
        Output format, either df or dict
    """
    endpoint = session.url + "data"
    if rec_id is None:
        # Check token for expiry
        session.reinitialize()
        data = requests.get(endpoint, headers=session.header)
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
            # print(f"{endpoint}/{rec_id}")
            # print(self.header)
            # Check token for expiry
            session.reinitialize()
            d = requests.get(f"{endpoint}/{rec_id}",
                             headers=session.header,
                             params={"page": i})
            if d.status_code == 200:
                b = d.json().get('has_more_data')
                i += 1
                data = data + d.json().get('data')
            else:
                b = False
                print(f"Ran into issues processing your request \nStatus Code: {d.status_code}\n" +
                      f"Content: {d.text}\nData processed before the error returned.")
        if output == 'df':
            return pd.DataFrame.from_dict(data)
        elif output == 'dict':
            return data
        # return pd.DataFrame.from_dict(data.json().get('data'))


def get_data_history(session=None, rec_id=None, ver_id=None, output='df'):
    """Retrieve list of data sources or retrieve the contents of a data source
    session : custom class
        Session class passed from ClicData session module
    rec_id : int
        RecId of the data set you want to check history for
    ver_id : int
        Version ID of the data you want to retrieve from your data set
    output : str
        Output format, either df or dict
    """
    if rec_id is None:
        raise Exception('Please enter a valid data clone RecId.')
    else:
        if ver_id is None:
            endpoint = session.url + f"data/{rec_id}/versions"
            session.reinitialize()
            data = requests.get(endpoint, headers=session.header)
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
            endpoint = session.url + f"data/{rec_id}/v/{ver_id}"
            i = 1
            b = True
            data = []
            while b:
                session.reinitialize()
                d = requests.get(endpoint,
                                 headers=session.header,
                                 params={"page": i})
                if d.status_code != 200:
                    raise Exception('Please enter a valid data RecId or Version Number for your data.')
                b = d.json().get('has_more_data')
                i += 1
                print(d.json())
                data = data + d.json().get('data')
            if output == 'df':
                return pd.DataFrame.from_dict(data)
            elif output == 'dict':
                return data


def create_data(session=None, name=None, desc="", cols=None):
    """Creates an empty custom table in ClicData
    session : custom class
        Session class passed from ClicData session module
    name: str
        Name of data table created in ClicData. Must be unique to account.
    desc : str
        Long form description attached to table in ClicData.
    def: dict
        Column name as key, data type as value.
    """
    valid_data_types = ['text', 'number', 'datetime', 'date', 'percentage', 'checkbox', 'dropdown', 'id']
    pandas_convert = {
        'object': 'text',
        'int64': 'number',
        'float64': 'number',
        'bool': 'checkbox',
        'datetime64': 'datetime',
        'timedelta[ns]': 'text',
        'category': 'text'
    }

    if name is None:
        raise Exception('Please enter a name for your data set')
    elif cols is None:
        raise Exception("""Please provide a valid dictionary object containing your column names and types.\n
           {"columnName":"type",\n
           "columnName":"type"}""")
    else:
        endpoint = f"{session.url}data"
        columns = []
        for i in cols.keys():
            data_type = pandas_convert[cols[i].name]
            if data_type not in valid_data_types:
                raise Exception(f'Column [{i}] contains an invalid data type ({data_type})' +
                                f', please enter your data with one of the following: {valid_data_types}.')
            column_def = {"name": i,
                          "data_type": data_type}
            # num = 0
            columns.append(column_def)
        # print(columns)
        body = {
            "name": name,
            "description": desc,
            "columns": columns
        }
        session.reinitialize()
        post = requests.post(endpoint, headers=session.header, json=body)
        return post


def append_data(session=None, rec_id=None, data=None):
    """Append your data to an existing dataset
    session : custom class
        Session class passed from ClicData session module
    rec_id : int
        id of your data in ClicData
    data : pandas.Dataframe
        df containing the data you want to append
    """
    if rec_id is None:
        raise Exception('Please enter a valid data clone RecId.')
    elif data is None:
        raise Exception('Please enter a data set to append')
    else:
        endpoint = session.url + f'data/{rec_id}/row'
        d = data.to_dict(orient='index')
        print(d)
        data_set = []
        for k, v in d.items():
            row = []
            num = 0
            for col, val in v.items():
                cell = {"column": col,
                        "value": val}
                row.append(cell)
                num += 1
            data_set.append(row)
        body = {
            "data": data_set
        }
        session.reinitialize()
        post = requests.post(endpoint,
                             headers=session.header,
                             json=body)
        return post


def static_send_data(session=None, name=None, desc="", data=None):
    """ Creates a static data set in ClicData using a pandas dataframe
    session : custom class
        Session class passed from ClicData session module
    name : str
        Name of data set to create, must be unique to your account
    desc : str
        Optional, long-form details about your data
    data : pandas.Dataframe
        Data to upload
    """
    if name is None:
        raise Exception('Please enter a name for your data set.')
    elif data is None:
        raise Exception('Please enter a data.')
    else:
        columns = data.dtypes.to_dict()
        # print(columns)
        rec_id = session.create_data(name=name, desc=desc, cols=columns)
        try:
            rec_id = int(rec_id.text)
        except ValueError as e:
            raise Exception(
                f'There appears to be an issue with the connection. Creating data set returned:\n{rec_id.text}')
        status = session.append_data(rec_id=rec_id, data=data)
    return status.text


def delete_data(session=None, rec_id=None, filters=None, multiple_rows='all'):
    """ Deletes rows from a specified data set.
    session : custom class
        Session class passed from ClicData session module
    name : str
        Name of data set to create, must be unique to your account
    desc : str
        Optional, long-form details about your data
    data : pandas.Dataframe
        Data to upload
    """

    if rec_id is None or type(rec_id) != int:
        raise Exception('Please enter a valid data clone RecId as an integer.')
    if filters is None or type(filters) != dict:
        raise Exception("Please enter a dict of column names (keys) and values to filter.")
    else:
        endpoint = f"{session.url}data/{rec_id}/row"
        find = []
        for k, v in filters.items:
            cell = {"column": k,
                    "value": v}
            find.append(cell)
        body = {
            "multiplerows": multiple_rows,
            "find": find
        }
        session.reinitialize()
        delete = requests.delete(endpoint, body=body, headers=session.header)
        return delete.text
