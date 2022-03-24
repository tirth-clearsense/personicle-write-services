from copy import copy
import traceback
from typing import List
import copy
from flask import session
from numpy import delete
from models.postgresdb import generate_table_class, loadSession
from models.base_schema import base_schema
from sqlalchemy import delete

EVENTS_TABLE = "personal_events"
EVENTS_SCHEMA = "event_schema.avsc"

session = loadSession()

def delete_events(event_ids : List):
    try:
        model_class = generate_table_class(EVENTS_TABLE, copy.deepcopy(base_schema[EVENTS_SCHEMA]))

        statement = delete(model_class).where(model_class.unique_event_id.in_(event_ids))

        return_values = session.execute(statement,)
        session.commit()
        return True, return_values.rowCount
    except Exception as e:
        print(traceback.format_exc(e))
        return False, 0

def delete_datastreams(datastream_table, user_id, start_time, end_time):
    pass

def update_events(events: List):
    pass
