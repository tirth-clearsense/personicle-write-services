from sqlalchemy import BigInteger, Column, Float, TIMESTAMP
from sqlalchemy.types import Integer, Numeric, String
base_schema = {
    "integer_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(Integer),
        "unit": Column(String),
        "confidence": Column(String, default=None)
        },
    "numeric_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(Float),
        "unit": Column(String),
        "confidence": Column(String, default=None)
        },
    "string_datastream_schema.avsc": {
        "individual_id": Column(String, primary_key=True),
        "timestamp": Column(TIMESTAMP, primary_key=True),
        "source": Column(String, primary_key=True),
        "value": Column(String),
        "unit": Column(String),
        "confidence": Column(String, default=None)
        },
    "event_schema.avsc": {
        "user_id": Column(String),
        "start_time": Column(TIMESTAMP),
        "end_time": Column(TIMESTAMP),
        "event_name": Column(String),
        "source": Column(String),
        "parameters": Column(String),
        "unique_event_id": Column(String, primary_key=True)
    },
     "user_datastreams_store.avsc": {
        "individual_id": Column(String, primary_key=True),
        "source": Column(String, primary_key=True),
        "datastream": Column(String, primary_key=True),
        "last_updated": Column(TIMESTAMP)
    }
}