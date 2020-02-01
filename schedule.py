import requests
import pandas as pd


def get_schedule(session=None, rec_id=None, output='df'):
    """Get details of all schedules on an account or single schedule if rec_id is passed
    session : custom class
        Session class passed from ClicData session module
    rec_id : int
        Id of your schedule in ClicData
    output : str
        Output format, either df or dict
    """
    if session is None:
        raise Exception("Please enter a valid Session() object.")
    endpoint = session.url + "schedule"
    if type(rec_id) == int:
        schedules = requests.get(f"{endpoint}/{rec_id}",
                                 headers=session.header)
    else:
        schedules = requests.get(f"{endpoint}",
                                 headers=session.header)

    if output == 'df':
        return pd.DataFrame.from_dict(schedules.json().get('schedules'))
    elif output == 'dict':
        return schedules.json().get('schedules')
    else:
        raise Exception("Please enter a valid output: ['df', 'dict'].")


def trigger_schedule(session=None, rec_id=None, output='df'):
    """Trigger a specified schedule by id
    session : custom class
        Session class passed from ClicData session module
    rec_id : int
            Id of your schedule in ClicData
    output : str
        Output format, either df or dict
    """
    if session is None:
        raise Exception("Please enter a valid Session() object.")
    if type(rec_id) == int:
        endpoint = session.url + f"schedule/{rec_id}/trigger"
        response = requests.post(f"{endpoint}",
                                 headers=session.header)
    else:
        raise Exception("Please enter a valid rec_id as an integer.")

    return response
