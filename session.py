import requests
from datetime import datetime
from datetime import timedelta
import base64


class Session:
    """
    Class Session used to initiate and maintain connection to ClicData API

    Class Methods:
    api_call()
        Intended to handle all calls to ClicData while using this library
    """
    # To do: re-organize variables as **kwargs
    def __init__(
        self,
        auth_method='client_credentials',
        client_id=None,
        client_secret=None,
        username=None,
        password=None,
        **kwargs
    ):
        """
        Parameters
        client_id : str
            Client ID provided from your ClicData ccount
        client_secret : str
            Client secret provided from your ClicData account
        """
        self.url = kwargs.get('url', "https://api.clicdata.com/")
        self.auth_method = auth_method

        if auth_method == 'client_credentials':
            self._client_id = client_id
            self._client_secret = client_secret
            token_type = 'Bearer '
            self.access_token, self.token_expire_time, _ = self._initialize()
            
        elif auth_method == 'basic':
            if type(client_id) != str:
                raise Exception("Please enter a valid client_id (string).")
            elif type(username) != str:
                raise Exception("Please enter a valid username (string).")
            elif type(password) != str:
                raise Exception("Please enter a valid password (string).")
            else:
                self._client_id = client_id
                up = username + ':' + password
                base64_up = base64.b64encode(up.encode('utf-8'))
                self.access_token = base64.b64encode(client_id.encode('utf-8') + base64_up)
                token_type = 'Basic '
                self.auth_method = 'basic'

        elif auth_method == 'authorization_code':
            # To be developed
            raise Exception("Authorization code is not a supported authentication method yet.")
            ###
        else:
            raise Exception("Please provide a valid authentication type. Choose from:\n"+
                            "basic, client_credentials, or authorization code")

        self.header = {
            "Authorization": token_type + self.access_token,
            "accept": "application/json"
        }

    ###
    # Methods
    ###
    def _initialize(self):
        """Retrieve access token for ClicData API
        Used for client_credentials and authorization_code
        """
        token_url = self.url + "oauth20/token"
        token_request_body = {"grant_type": "client_credentials",
                              "client_id": self._client_id,
                              "client_secret": self._client_secret}
        token = requests.post(token_url,
                              data=token_request_body)
        status_code = token.status_code
        token_body = token.json()
        access_token = token_body.get("access_token")
        expires_in = token_body.get("expires_in")
        token_expire_time = datetime.now() + timedelta(seconds=expires_in)
        return access_token, token_expire_time, status_code

    def reinitialize(self):
        """To refresh an expired token for client_credentials or authorization_code"""
        if self.auth_method != 'basic':
            if datetime.now() >= self.token_expire_time:
                self.access_token, self.token_expire_time, _ = self._initialize()
                self.header = {"Authorization": "Bearer " + self.access_token,
                               "accept": "application/json"}

    def api_call(
        self, 
        suffix=None, 
        request_method=None, 
        params=None, 
        headers=None, 
        body=None
    ):
        """
        Perform API call with provided method and additional criteria
        suffix : str
        request_method : str
            method to use (get, post, delete, put, etc...)
        params : dict
            query string parameters to add to the request
        headers : dict
            additional headers to pass in addition to authorization
        body : dict
             data to send with the request
        return: request return
        """
        # Check if token is still valid, if not, re-initialize
        self.reinitialize()
        endpoint = self.url + suffix

        # Check if additional headers are provided
        if type(headers) == dict:
            headers = self.header.update(headers)
        elif headers is not None:
            raise Exception("The header type entered is invalid. Please provide type dict.")
        else:
            headers = self.header

        # Check if any parameters are passed as a dictionary
        if not params:
            params = ''
        elif type(params) != dict:
            raise Exception("The params type entered is invalid. Please provide type dict.")

        # Check which API method is being used
        if request_method == 'get':
            response = requests.get(endpoint, params=params, headers=headers)
        elif request_method == 'post':
            response = requests.post(endpoint, params=params, headers=headers, json=body)
        elif request_method == 'delete':
            response = requests.delete(endpoint, params=params, headers=headers)
        elif request_method == 'put':
            response = requests.put(endpoint, params=params, headers=headers, json=body)
        else:
            raise Exception("Please enter a valid request_method as a string")
        return response


class SessionManager:
    """
    A class to maintain a connection for the current session.
    Designed to work with an open session in a Jupyter notebook.
    """
    __session = None

    @classmethod
    def bind_session(cls, session):
        cls.__session = session

    @classmethod
    def get_session(cls):
        if cls.__session is None:
            raise Exception("You need to create a session using SessionManager" +
                            " or pass connection parameters to your module class.")
        return cls.__session

    def __init__(self, **connection_params):
        session = Session(**connection_params)
        self.bind_session(session)



