import requests
import pandas as pd


###
# Functions that accept the Session class as input
###
def get_dashboard(session=None, thumbnail=False, name=None, output='df'):
    """Get details of all dashboards on an account
    session : custom class
        Session class passed from ClicData session module
    thumbnail : bool
        Whether to include base64 copies of dashboard thumbnails
    name : str
        Filter dashboards by name
    output : str
        Output format, either df or dict
    """
    endpoint = session.url + "dashboard"
    if name is not None:
        params = {"includethumbnail": thumbnail,
                  "name": name}
    else:
        params = {"includethumbnail": thumbnail}

    dashboards = requests.get(f"{endpoint}",
                              params=params,
                              headers=session.header)

    if output == 'df':
        return pd.DataFrame.from_dict(dashboards.json().get('dashboards'))
    elif output == 'dict':
        return dashboards.json()


def get_dashboard_thumbnail(session=None, rec_id=None):
    """Returns base64 encoded string of a thumbnail
    session : custom class
        Session class passed from ClicData session module
    rec_id : str
        Dashboard rec_id to pull
    """
    if type(rec_id) != int:
        raise Exception("Please enter a valid rec_id integer.")
    endpoint = session.url + "account/" + rec_id + "/thumbnail"
    thumbnail = requests.get(f"{endpoint}",
                             headers=session.header)
    return thumbnail.text


def get_dashboard_snapshot(session=None, rec_id=None):
    """Returns base64 encoded string of a thumbnail
    session : custom class
        Session class passed from ClicData session module
    rec_id : str
        Dashboard rec_id to pull
    """
    if type(rec_id) != int:
        raise Exception("Please enter a valid rec_id integer.")
    endpoint = session.url + "account/" + rec_id + "/snapshot"
    thumbnail = requests.get(f"{endpoint}",
                             headers=session.header)
    return thumbnail.text
