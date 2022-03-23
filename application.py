from flask import Flask, Response, request
import requests
from config import IDENTITY_SERVER_SETTINGS, DATA_DICTIONARY_SERVER_SETTINGS, EVENTHUB_CONFIG, HOST_CONFIG
import json
import os
from producer import send_records_azure, send_datastreams_to_azure

app = Flask(__name__)

PROJ_LOC = os.path.dirname(__file__)

@app.route("/")
def test_application():
    return Response("Write service up")

@app.route("/event/upload", methods=['POST'])
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
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+":"+IDENTITY_SERVER_SETTINGS["HOST_PORT"]+'/authenticate'))
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+":"+IDENTITY_SERVER_SETTINGS["HOST_PORT"]+'/authenticate', headers=auth_headers, verify=False)
    print(auth_response.text, auth_response.status_code)
    if auth_response.status_code != requests.codes.ok or json.loads(auth_response.text).get("message", False)== False:
        return Response("Unauthorised access token", 401)

    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        event_data_packet = request.json
    else:
        return Response('Content-Type not supported!', 415)
    
    # verify the event packet by making the data dictionary api call
    data_dict_params = {"data_type": "event"}
    data_dict_response = requests.post(DATA_DICTIONARY_SERVER_SETTINGS['HOST_URL']+':'+DATA_DICTIONARY_SERVER_SETTINGS['HOST_PORT']+"/validate-data-packet", 
        json=event_data_packet, params=data_dict_params)
    if json.loads(data_dict_response.text).get("schema_check", False):
        # send the data to azure event hub
        schema_file = os.path.join(PROJ_LOC, "event_schema.avsc")
        send_records_azure.send_records_to_eventhub(schema_file, event_data_packet, EVENTHUB_CONFIG['EVENTHUB_NAME'])
        return Response("Sent {} records to database".format(len(event_data_packet)), 200)
    else:
        return Response("Incorrectly formatted data packet", 422)

@app.route("/datastream/upload", methods=['POST'])
def upload_datastream():
    # authenticate the access token with okta api call
    auth_headers = {}
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+":"+IDENTITY_SERVER_SETTINGS["HOST_PORT"]+'/authenticate'))
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+":"+IDENTITY_SERVER_SETTINGS["HOST_PORT"]+'/authenticate', headers=auth_headers, verify=False)
    print(auth_response.text, auth_response.status_code)
    if auth_response.status_code != requests.codes.ok or json.loads(auth_response.text).get("message", False)== False:
        return Response("Unauthorised access token", 401)

    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        data_packet = request.json
    else:
        return Response('Content-Type not supported!', 415)
    
    # verify the event packet by making the data dictionary api call
    data_dict_params = {"data_type": "datastream"}
    data_dict_response = requests.post(DATA_DICTIONARY_SERVER_SETTINGS['HOST_URL']+':'+DATA_DICTIONARY_SERVER_SETTINGS['HOST_PORT']+"/validate-data-packet", 
        json=data_packet, params=data_dict_params)
    if json.loads(data_dict_response.text).get("schema_check", False):
        # send the datastream to azure event hub
        send_datastreams_to_azure.datastream_producer(data_packet)
        return Response("Sent {} records to database".format(len(data_packet)), 200)
    else:
        return Response("Incorrectly formatted data packet", 422)

@app.route("/event/delete", methods=["DELETE"])
def delete_event():
    """Receive a list of event ids to be deleted in th epost request json"""
    pass

@app.route("/event/update", methods=["POST"])
def update_event():
    pass


if __name__ == "__main__":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    print("running server on {}:{}".format(HOST_CONFIG['HOST_URL'], HOST_CONFIG['HOST_PORT']))
    app.run(HOST_CONFIG['HOST_URL'], port=HOST_CONFIG['HOST_PORT'], debug=True)#, ssl_context='adhoc')