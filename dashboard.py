import requests
import pandas as pd
import base64


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
    suffix = "dashboard"
    if name is not None:
        params = {"includethumbnail": thumbnail,
                  "name": name}
    else:
        params = {"includethumbnail": thumbnail}

    dashboards = session.api_call(suffix=suffix,
                                  params=params,
                                  request_method='get')

    if output == 'df':
        return pd.DataFrame.from_dict(dashboards.json().get('dashboards'))
    elif output == 'dict':
        return dashboards.json()


def get_dashboard_thumbnail(session=None, rec_id=None, output='base64'):
    """Returns thumbnail either ase base64 encoded string or image
    session : custom class
        Session class passed from ClicData session module
    rec_id : str
        Dashboard rec_id to pull
    output : str
        Whether the function output a string or an image
    """
    if type(rec_id) != int:
        raise Exception("Please enter a valid rec_id integer.")
    suffix = "account/" + rec_id + "/thumbnail"
    thumbnail = session.api_call(suffix=suffix,
                                 request_method='get')
    if output == 'base64':
        return thumbnail.text
    elif output == 'image':
        image = base64.b64decode(thumbnail.text)
        return image
    else:
        raise Exception("Please enter a valid output type: ['base64', 'image'].")


def get_dashboard_snapshot(session=None, rec_id=None, output='base64'):
    """Returns snapshot either ase base64 encoded string or image
    session : custom class
        Session class passed from ClicData session module
    rec_id : str
        Dashboard rec_id to pull
    output : str
        Whether the function output a string or an image
    """
    if type(rec_id) != int:
        raise Exception("Please enter a valid rec_id integer.")
    suffix = "account/" + rec_id + "/snapshot"
    snapshot = session.api_call(suffix=suffix,
                                request_method='get')
    if output == 'base64':
        return snapshot.text
    elif output == 'image':
        image = base64.b64decode(snapshot.text)
        return image
    else:
        raise Exception("Please enter a valid output type: ['base64', 'image'].")
