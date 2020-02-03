import requests
import pandas as pd


###
# Functions that accept the Session class as input
###
def get_account(session=None, output='df'):
    """Get details on account usage and limits
    session : custom class
        Session class passed from ClicData session module
    output : str
        Output format, either df or dict
    """
    suffix = "account"
    account = session.api_call(suffix=suffix,
                               request_method='get')
    if output == 'df':
        return pd.DataFrame.from_dict(account.json())
    elif output == 'dict':
        return account.json()


def get_account_activity(session=None, entity='users', output='df'):
    """Retrieve either dashboard or user activity
    session : custom class
        Session class passed from ClicData session module
    entity : str
        Pull activity data for 'dashboards' vs 'users'
    output : str
        Output format, either df or dict
    """
    valid_entities = ['users', 'dashboards']
    if entity not in valid_entities:
        raise Exception("Please enter a valid entity: "+str(valid_entities))
    suffix = "account/activity/" + entity
    activity = session.api_call(suffix=suffix,
                                headers=session.header,
                                request_method='get')

    if output == 'df':
        return pd.DataFrame.from_dict(activity.json())
    elif output == 'dict':
        return activity.json()