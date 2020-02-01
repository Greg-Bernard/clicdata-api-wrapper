import oauthlib.oauth2
from requests_oauthlib import OAuth2Session
import requests
from datetime import datetime
from datetime import timedelta
import base64


class Session:
    """
    Class Session used to initiate and maintain connection to ClicData API

    Class Variables:
    url : str
        ClicData base API URL
    access_token : str
        Access token for ClicData API
    token_expire_time : datetime
        Datetime that the token expires
    """
    # To do: re-organize variables as **kwargs
    def __init__(self,
                 auth_method='client_credentials',
                 client_id=None,
                 client_secret=None,
                 username=None,
                 password=None):
        """
        Parameters
        client_id : str
            Client ID provided from your ClicData ccount
        client_secret : str
            Client secret provided from your ClicData account
        """
        self.url = "https://api.clicdata.com/"
        self.auth_method = auth_method

        if auth_method == 'client_credentials':
            self.client_id = client_id
            self.client_secret = client_secret
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
                self.client_id = client_id
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

        self.header = {"Authorization": token_type + self.access_token,
                       "accept": "application/json"}

    ###
    # Methods
    ###
    def _initialize(self):
        """Retrieve access token for ClicData API
        Used for client_credentials and authorization_code
        """
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
        """To refresh an expired token for client_credentials or authorization_code"""
        if self.auth_method != 'basic':
            if datetime.now() >= self.token_expire_time:
                self.access_token, self.token_expire_time, _ = self._initialize()
                self.header = {"Authorization": "Bearer " + self.access_token,
                               "accept": "application/json"}


