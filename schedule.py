from clicdata_api_wraper.session import Session, SessionManager
import pandas as pd


class Schedule:

    def __init__(self, **connection_params):
        if connection_params:
            self.session = Session(**connection_params)
        else:
            self.session = SessionManager.get_session()

    def get_schedule(self, rec_id=None, output='df'):
        """Get details of all schedules on an account or single schedule if rec_id is passed
        rec_id : int
            Id of your schedule in ClicData
        output : str
            Output format, either df or dict
        """
        suffix = "schedule"
        if type(rec_id) == int:
            schedules = self.session.api_call(suffix=f"{suffix}/{rec_id}",
                                              request_method='get')
        else:
            schedules = self.session.api_call(suffix=suffix,
                                              request_method='get')

        if output == 'df':
            return pd.DataFrame.from_dict(schedules.json().get('schedules'))
        elif output == 'dict':
            return schedules.json().get('schedules')
        else:
            raise Exception("Please enter a valid output: ['df', 'dict'].")

    def trigger_schedule(self, rec_id=None):
        """Trigger a specified schedule by id
        rec_id : int
                Id of your schedule in ClicData
        """
        if type(rec_id) == int:
            suffix = f"schedule/{rec_id}/trigger"
            response = self.session.api_call(suffix=suffix,
                                             request_method='get')
        else:
            raise Exception("Please enter a valid rec_id as an integer.")

        return response
