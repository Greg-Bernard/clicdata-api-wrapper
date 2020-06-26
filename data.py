from clicdata_api_wrapper.session import Session, SessionManager
import pandas as pd


class Data:

    def __init__(self, **connection_params):
        if connection_params:
            self.session = Session(**connection_params)
        else:
            self.session = SessionManager.get_session()

    def retrieve_paginated_data(
        self, 
        suffix=None
    ):
        page = 1
        has_more_data = True
        data = []
        while has_more_data:
            page_response = self.session.api_call(suffix=suffix,
                                                  request_method='get',
                                                  params={"page": page})
            if page_response.status_code == 200:
                has_more_data = page_response.json().get('has_more_data')
                page += 1
                data = data + page_response.json().get('data')
            else:
                has_more_data = False
                print(f"Ran into issues processing your request \nStatus Code: {page_response.status_code}\n" +
                      f"Content: {page_response.text}\nData processed before the error returned")
        return data

    def get_data(
        self, 
        rec_id=None, 
        name:str=None, 
        unique_key_available:bool=None, 
        refresh:bool=None, 
        output='df'
    ):
        """Retrieve list of data sources or retrieve the contents of a data source
        rec_id : int
            RecId of the data you want to retrieve
        name : str
            data set name based filter to apply
        unique_key_available : bool
            filter data sets based on whether they're using unique keys
        refresh : bool
            filter data sets based on whether they're refresh-able (non-static)
        output : str
            Output format, either df or dict
        """
        if rec_id is None:
            # Add parameter string based on input
            params = {}

            if name:
                params["name"] = str(name)

            if unique_key_available:
                params["uniquekeyavailable"] = unique_key_available

            if refresh:
                params["refresh"] = refresh

            # Use api_call to grab list of data sets
            data = self.session.api_call(
                suffix='data',
                request_method='get',
                params=params
            )

            if output == 'df':
                return pd.DataFrame.from_dict(data.json().get('data'))
            elif output == 'dict':
                return data.json().get('data')

        elif type(rec_id) != int:
            raise Exception("Please enter a valid rec_id as int.")

        else:
            suffix = f"data/{rec_id}"
            data = self.retrieve_paginated_data(suffix=suffix)
            if output == 'df':
                return pd.DataFrame.from_dict(data)
            elif output == 'dict':
                return data

    def get_data_history(
        self, 
        rec_id=None, 
        ver_id=None, 
        output='df'
    ):
        """Retrieve list of data sources or retrieve the contents of a data source
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
                suffix = f"data/{rec_id}/versions"
                data = self.session.api_call(suffix=suffix)
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
                suffix = f"data/{rec_id}/v/{ver_id}"
                data = self.retrieve_paginated_data(suffix=suffix)
                if output == 'df':
                    return pd.DataFrame.from_dict(data)
                elif output == 'dict':
                    return data

    def create_data(
        self, 
        name=None, 
        description="", 
        cols=None
    ):
        """Creates an empty custom table in ClicData
        name: str
            Name of data table created in ClicData. Must be unique to account.
        desc : str
            Long form description attached to table in ClicData.
        def: dict
            Column name as key, data type as value.
        """
        # List of valid ClicData data types to input
        valid_data_types = [
            'text', 
            'number', 
            'datetime', 
            'date', 
            'percentage', 
            'checkbox', 
            'dropdown', 
            'rec_id'
        ]
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
            suffix = "data"
            columns = []
            for column_name in cols.keys():
                data_type = pandas_convert[cols[column_name].name]
                if data_type not in valid_data_types:
                    raise Exception(f'Column [{column_name}] contains an invalid data type ({data_type})' +
                                    f', please enter your data with one of the following: {valid_data_types}.')
                column_def = {"name": column_name,
                              "data_type": data_type}
                columns.append(column_def)

            body = {
                "name": name,
                "description": description,
                "columns": columns
            }

            post = self.session.api_call(
                suffix=suffix,
                body=body,
                request_method='post'
            )  

            return post

    def append_data(
        self, 
        rec_id=None, 
        data=None
    ):
        """Append your data to an existing data set
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
            suffix = f'data/{rec_id}/row'
            formatted_data = data.to_dict(orient='index')
            data_set = []
            for row_dict in formatted_data.values():
                row = []
                for column, value in row_dict.items():
                    cell = {"column": column,
                            "value": value}
                    row.append(cell)
                data_set.append(row)
            body = {
                "data": data_set
            }
            post = self.session.api_call(
                suffix=suffix,
                body=body,
                method='post'
            )

            return post.text

    def create_and_append(
        self, name=None, 
        description="", 
        data=None
    ):
        """ Creates a static data set in ClicData using a pandas dataframe
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
            rec_id = self.create_data(
                name=name,
                description=description,
                cols=columns
            )

            try:
                rec_id = int(rec_id.text)
            except ValueError as e:
                raise Exception(
                    f'There appears to be an issue with the connection. Creating data set returned:\n{rec_id.text}\n'+
                    f'Error text: {e}'
                )

            status = self.append_data(
                rec_id=rec_id,
                data=data
            )

        return {'rec_id': rec_id, 'status': status.text}

    def rebuild_data(
        self, 
        rec_id=None, 
        method='reload'
    ):
        """ Rebuild a data set using the specified method
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

        if type(rec_id) != int:
            raise Exception("Please enter a valid data clone RecId as an integer.")
        if method not in valid_methods:
            raise Exception(f"Please enter a valid method: {valid_methods}")

        suffix = f"data/{rec_id}/{method}"
        response = self.session.api_call(suffix=suffix, request_method='post')

        return response.text

    def delete_data(
        self, 
        rec_id=None, 
        filters=None, 
        multiple_rows='all'
    ):
        """ Deletes rows from a specified data set.
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
            suffix = f"data/{rec_id}/row"
            find = []
            for k, v in filters.items:
                cell = {"column": k,
                        "value": v}
                find.append(cell)

            body = {
                "multiplerows": multiple_rows,
                "find": find
            }

            delete = self.session.api_call(
                request_method='delete',
                suffix=suffix,
                body=body
            )

            return delete.text
