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
    suffix = "schedule"
    if type(rec_id) == int:
        schedules = session.api_call(suffix=f"{suffix}/{rec_id}",
                                     request_method='get')
    else:
        schedules = session.api_call(suffix=suffix,
                                     request_method='get')

    if output == 'df':
        return pd.DataFrame.from_dict(schedules.json().get('schedules'))
    elif output == 'dict':
        return schedules.json().get('schedules')
    else:
        raise Exception("Please enter a valid output: ['df', 'dict'].")


def trigger_schedule(session=None, rec_id=None):
    """Trigger a specified schedule by id
    session : custom class
        Session class passed from ClicData session module
    rec_id : int
            Id of your schedule in ClicData
    """
    if session is None:
        raise Exception("Please enter a valid Session() object.")
    if type(rec_id) == int:
        suffix = f"schedule/{rec_id}/trigger"
        response = session.api_call(suffix=suffix,
                                    request_method='get')
    else:
        raise Exception("Please enter a valid rec_id as an integer.")

    return response
