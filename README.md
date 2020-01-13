# clicdata-python
A package to work with your ClicData account natively in Python.

Currently only supports the Client Credentials authentication method. See how to generate your Client ID and Client Secret here: https://app.clicdata.com/help/apidocumentation/api-auth-oauth-client

Right now this is an early alpha version with limited type checking and error handling. As development progresses more handling will be built in and more API functionality will be incorporated.

*Not officially affiliated with ClicData*


## Currently working:

### Methods

#### get_data()
* **Parameters**:
  * *(Optional)* **rec_id** : int - RecId of the data you want to retrieve
  * **output** : str - Output format, either df or dict
* **Enpoints**:
  * List Data: GET /data
  * Retrieve Data: GET /data/{id}
* **Usage**:
  * If no rec_id is provided lists all data on account, if rec_id is provided, retrieves data from specified data set.

#### get_data_history()
* **Parameters**:
  * **rec_id** : int - RecId of the data you want to retrieve
  * *(Optional)* **ver_id** : int - Version ID of the data you want to retrieve from your data set
  * **output** : str - Output format, either df or dict
* **Enpoints**:
  * List Data History: GET /data/{id}/versions
  * Retrieve Historical Data: GET /data/{id}/v/{ver}
* **Usage**:
  * If no ver_id is provided lists all stored data versions, if ver_id is provided, retrieves data from specified data set version. Must have Data History enabled on data set to use.

#### create_data()
* **Parameters**:
  * **name** : str - Name of data table created in ClicData. Must be unique to account.
  * *(Optional)* **desc** : str - Long form description attached to table in ClicData.
  * **cols** : dict - Column name as key, data type as value.
* **Enpoints**:
  * Create New Custom Table: POST /data
* **Usage**:
  * Creates and empty custom data table on your ClicData account, which can then be appended to

#### append_data()
* **Parameters**:
  * **rec_id** : int - id of your data in ClicData
  * **data** : pandas.Dataframe - df containing the data you want to append.
* **Enpoints**:
  * Append Data: POST /data/{id}/row
* **Usage**:
  * Append your data to an existing dataset
  
#### static_send_data()
* **Parameters**:
  * **name** : str - Name of data set to create, must be unique to your account
  * *(Optional)* **desc** : str - Optional, long-form details about your data
  * **data** : pandas.Dataframe - Data to upload
* **Usage**:
  * Uses create_data() to create a static data set, then uses append_data() to add the input data to it. Note: Input must be a dataframe.

## To Do:

### Planned Authentication:

* Authorization Code
* Basic Auth

### Planned Methods:

* **Data:**
  * Update Data: PUT /data/{id}/row
  * Delete Data: DELETE /data/{id}/row
* **Account:**
  * User Activity: GET /account/activity/users
  * Dashboard Activity: GET /account/activity/dashboards
* **Schedule:**
  * List Schedules: GET /schedule
  * Schedule Details: GET /schedule/{id}
  * Trigger Schedule: POST /schedule/{id}/trigger
* **Team:**
  * List Teams: GET /team
  * Create Team: POST /team
  * Update Team: PUT /team/{id}
  * Delete Team: DELETE /team/{id}
  * Add User To Team: POST /team/{id}/user
  * Remove User From Team: DELETE /team/{id}/user
  * Add/Update Team Paramter: POST /team/{id}/parameter
  * Remove Team Paramter: DELETE /team/{id}/parameter
* **User:**
  * List Users: GET /user
  * Create new User: POST /user
  * Get User Details: GET /user/{id}
  * Update User: PUT /user/{id}
  * Delete User: DELETE /user/{id}
  * Set User Password: PUT /user/{id}/password
  
  
  # Resources:
  
  * **ClicData API Documentation:** https://app.clicdata.com/help/apidocumentation/api-docs
  
