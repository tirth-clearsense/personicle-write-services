from flask import Flask, Response, request
import requests
from config import IDENTITY_SERVICE_AUTH_URL, DATA_DICTIONARY_VERIFY_URL, EVENTHUB_CONFIG, WRITE_SERVICE_URL, WRITE_SERVICE_PORT
import json
import os
from producer import send_records_azure, send_datastreams_to_azure

app = Flask(__name__)

PROJ_LOC = os.path.dirname(__file__)

@app.route("/")
def test_application():
    return Response("Write service up")

@app.route("event/upload", methods=['POST'])
def upload_event():
    """
    Expect a well formatted event data packet
    Verify the access token in the request
    Verify the packet
    if verified then send to event hub
    else return a failure message
    """
    # authenticate the access token with okta api call
    auth_headers = {}
    auth_response = requests.get(IDENTITY_SERVICE_AUTH_URL, headers=auth_headers)
    if not json.loads(auth_response.text).get("message", False):
        return Response("Unauthorised access token", 401)

    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        event_data_packet = request.json
    else:
        return Response('Content-Type not supported!', 415)
    
    # verify the event packet by making the data dictionary api call
    data_dict_params = {"data_type": "event"}
    data_dict_response = requests.post(DATA_DICTIONARY_VERIFY_URL, json=event_data_packet, params=data_dict_params)
    if json.loads(data_dict_response.text).get("schema_check", False):
        # send the data to azure event hub
        schema_file = os.path.join(PROJ_LOC, "event_schema.avsc")
        send_records_azure.send_records_to_eventhub(schema_file, event_data_packet, EVENTHUB_CONFIG['EVENTHUB_NAME'])
        return Response("Sent {} records to database".format(len(event_data_packet)), 200)
    else:
        return Response("Incorrectly formatted data packet", 422)

@app.route("datastream/upload", methods=['POST'])
def upload_datastream():
    pass


if __name__ == "__main":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    app.run(WRITE_SERVICE_URL, port=WRITE_SERVICE_PORT, ssl_context='adhoc', debug=True)