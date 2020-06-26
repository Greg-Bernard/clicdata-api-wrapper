from clicdata_api_wrapper.session import Session, SessionManager
import pandas as pd


class Account:

    def __init__(self, **connection_params):
        if connection_params:
            self.session = Session(**connection_params)
        else:
            self.session = SessionManager.get_session()

    def get_account(self, output='df'):
        """Get details on account usage and limits
        output : str
            Output format, either df or dict
        """
        suffix = "account"
        account = self.session.api_call(suffix=suffix,
                                        request_method='get')
        if output == 'df':
            return pd.DataFrame.from_dict(account.json())
        elif output == 'dict':
            return account.json()

    def get_account_activity(self, entity='users', output='df'):
        """Retrieve either dashboard or user activity
        entity : str
            Pull activity data for 'dashboards' vs 'users'
        output : str
            Output format, either df or dict
        """
        valid_entities = ['users', 'dashboards']
        if entity not in valid_entities:
            raise Exception("Please enter a valid entity: "+str(valid_entities))
        suffix = "account/activity/" + entity
        activity = self.session.api_call(suffix=suffix,
                                         request_method='get')

        if output == 'df':
            return pd.DataFrame.from_dict(activity.json())
        elif output == 'dict':
            return activity.json()
