from copy import copy
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
    model_class = generate_table_class(EVENTS_TABLE, copy.deepcopy(base_schema[EVENTS_SCHEMA]))

    statement = delete(model_class).where(model_class.unique_event_id.in_(event_ids))
    # statement = statement.on_conflict_do_nothing(index_elements=[model_class.individual_id, model_class.timestamp, model_class.source])\
    #     .returning(model_class)
    # orm_stmt = (
    #     select(model_class)
    #     .from_statement(statement)
    #     .execution_options(populate_existing=True)
    # )

    return_values = session.execute(statement,)
    session.commit()
    return True, return_values.rowCount

def delete_datastreams(datastream_table, user_id, start_time, end_time):
    pass

def update_events(events: List):
    pass
