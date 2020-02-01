import requests
import pandas as pd


###
# Helper Functions
###

def retrieve_paginated_data(session=None, endpoint=None):
    page = 1
    has_more_data = True
    data = []
    while has_more_data:
        session.reinitialize()
        d = requests.get(endpoint,
                         headers=session.header,
                         params={"page": page})
        if d.status_code == 200:
            has_more_data = d.json().get('has_more_data')
            page += 1
            data = data + d.json().get('data')
        else:
            has_more_data = False
            print(f"Ran into issues processing your request \nStatus Code: {d.status_code}\n" +
                  f"Content: {d.text}\nData processed before the error returned")
    return data


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
    if type(rec_id) != int:
        endpoint = session.url + "data"
        # Check token for expiry
        session.reinitialize()
        data = requests.get(endpoint, headers=session.header)
        # return data.json()
        if output == 'df':
            return pd.DataFrame.from_dict(data.json().get('data'))
        elif output == 'dict':
            return data.json().get('data')
    else:
        endpoint = f"{session.url}data/{rec_id}"
        data = retrieve_paginated_data(session=session, endpoint=endpoint)
        if output == 'df':
            return pd.DataFrame.from_dict(data)
        elif output == 'dict':
            return data
        # return pd.DataFrame.from_dict(data.json().get('data'))


def get_data_history(session=None, id=None, ver_id=None, output='df'):
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
    if id is None:
        raise Exception('Please enter a valid data clone RecId.')
    else:
        if ver_id is None:
            endpoint = session.url + f"data/{id}/versions"
            # Check token for expiry
            session.reinitialize()
            data = requests.get(endpoint, headers=session.header)
            if output == 'df':
                df = pd.DataFrame.from_dict(data.json().get('versions'))
                df["data_rec_id"] = id
                return df
            elif output == 'dict':
                data_dict = data.json().get('versions')
                for version in data_dict:
                    version.update({"data_rec_id": id})
                return data_dict
        else:
            endpoint = session.url + f"data/{id}/v/{ver_id}"
            data = retrieve_paginated_data(session=session, endpoint=endpoint)
            if output == 'df':
                return pd.DataFrame.from_dict(data)
            elif output == 'dict':
                return data


def create_data(session=None, name=None, description="", cols=None):
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
    # List of valid ClicData data types to input
    valid_data_types = ['text', 'number', 'datetime', 'date', 'percentage', 'checkbox', 'dropdown', 'rec_id']
    # Dictionary containing Pandas data types and the converted type for ClicData
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
            columns.append(column_def)
        body = {
            "name": name,
            "description": description,
            "columns": columns
        }
        # Check token for expiry
        session.reinitialize()
        post = requests.post(endpoint, headers=session.header, json=body)
        return post


def append_data(session=None, rec_id=None, data=None):
    """Append your data to an existing dataset
    session : custom class
        Session class passed from ClicData session module
    rec_id : int
        rec_id of your data in ClicData
    data : pandas.Dataframe
        df containing the data you want to append
    """
    if type(rec_id) != int:
        raise Exception('Please enter a valid data clone RecId.')
    elif data is None:
        raise Exception('Please enter a data set to append')
    else:
        endpoint = session.url + f'data/{rec_id}/row'
        formatted_data = data.to_dict(orient='index')
        data_set = []
        for k, v in formatted_data.items():
            row = []
            for column, value in v.items():
                cell = {"column": column,
                        "value": value}
                row.append(cell)
            data_set.append(row)
        body = {
            "data": data_set
        }
        # Check token for expiry
        session.reinitialize()
        post = requests.post(endpoint,
                             headers=session.header,
                             json=body)
        return post


def create_and_append(session=None, name=None, description="", data=None):
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
        rec_id = create_data(session=session, name=name, description=description, cols=columns)
        try:
            rec_id = int(rec_id.text)
        except ValueError as e:
            raise Exception(
                f'There appears to be an issue with the connection. Creating data set returned:\n{rec_id.text}')
        status = append_data(session=session, rec_id=rec_id, data=data)
    return status.text


def rebuild_data(session=None, rec_id=None, method='reload'):
    """ Rebuild a data set using the specified method
        session : custom class
            Session class passed from ClicData session module
        rec_id : int
            rec_id of your data in ClicData
        method : str
            reload method to refresh the data with
        """
    valid_methods = [
        "reload",
        "recreate",
        "update",
        "updateappend",
        "append"
    ]
    if session is None:
        raise Exception("Please enter a valid Session() object.")
    if type(rec_id) != int:
        raise Exception("Please enter a valid data clone RecId as an integer.")
    if method not in valid_methods:
        raise Exception(f"Please enter a valid method: {valid_methods}")

    endpoint = f"{session.url}data/{rec_id}/{method}"
    response = requests.post(endpoint, header=session.header)
    return response.text


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

    if type(rec_id) != int:
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
        # Check token for expiry
        session.reinitialize()
        delete = requests.delete(endpoint, body=body, headers=session.header)
        return delete.text


###
# WIP ---------------------------------------------------------------------------------
def update_data(session=None, rec_id=None, data=None, filters=None, multiple_rows='all'):
    ###
    # Replace matching data in ClicData data file based on filter provided
    ###

    # Check token for expiry
    session.reinitialize()
    auth_header = {"Authorization": "Bearer " + session.access_token}

    if type(rec_id) != int:
        raise Exception('Please enter a valid data clone RecId.')
    elif data is None:
        raise Exception('Please provide data to insert')
    else:
        endpoint = f"{session.url}data/{rec_id}/row"
        if filters is not None:
            {}
        body = {
            "multiplerows": multiple_rows,
            "find": [],
            "data": []
        }