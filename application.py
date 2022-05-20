from flask import Flask, Response, jsonify, request
import requests
from config import AVRO_SCHEMA_LOC, IDENTITY_SERVER_SETTINGS, DATA_DICTIONARY_SERVER_SETTINGS, EVENTHUB_CONFIG, HOST_CONFIG
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
    Expect a well formatted event data packet (list of events)
    Verify the access token in the request
    Verify the packet
    if verified then send to event hub
    else return a failure message
    """
    # authenticate the access token with okta api call
    # get user id from okta and make sure the ids match
    auth_token = request.headers['Authorization']
    auth_headers = {"Authorization": "{}".format(auth_token)}
    print(auth_headers)
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+'/authenticate'))
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+'/authenticate', headers=auth_headers)
    print(auth_response.text, auth_response.status_code)
    if auth_response.status_code != requests.codes.ok or json.loads(auth_response.text).get("message", False)== False:
        return Response("Unauthorised access token", 401)
    try:
        user_id = json.loads(auth_response.text)['user_id']
    except KeyError as e:
        return Response("Incorrect response from auth server", 401)
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        event_data_packet = request.json
    else:
        return Response('Content-Type not supported!', 415)
    if type(event_data_packet) != type([]):
        return Response("Array of events expected", 422)
    # verify the event packet by making the data dictionary api call
    send_records = []
    send_summary = {}
    for event in event_data_packet:
        data_dict_params = {"data_type": "event"}
        data_dict_response = requests.post(DATA_DICTIONARY_SERVER_SETTINGS['HOST_URL']+"/validate-data-packet", 
            json=event, params=data_dict_params)
        print(data_dict_response.text)
        if data_dict_response.status_code == requests.codes.ok and json.loads(data_dict_response.text).get("schema_check", False):
            if user_id == event.get("individual_id", ""):
                send_summary[event['event_name']] = send_summary.get(event['event_name'], 0) + 1
                send_records.append(event)
            else:
                send_summary['incorrect_user_id'] = send_summary.get('incorrect_user_id', 0) + 1
        else:
            send_summary['incorrectly_formatted_events'] = send_summary.get('incorrectly_formatted_events', 0) + 1
            # send the data to azure event hub
    schema_file = os.path.join(AVRO_SCHEMA_LOC, "event_schema.avsc")
    if len(send_records)> 0:
        send_records_azure.send_records_to_eventhub(schema_file, send_records, EVENTHUB_CONFIG['EVENTHUB_NAME'])
    return jsonify({"message": "Sent {} records to database".format(len(send_records)),
                "summary": send_summary
    })


@app.route("/datastream/upload", methods=['POST'])
def upload_datastream():
    # authenticate the access token with okta api call
    auth_headers = {}
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+'/authenticate'))
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+'/authenticate', headers=auth_headers)
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
    data_dict_response = requests.post(DATA_DICTIONARY_SERVER_SETTINGS['HOST_URL']+"/validate-data-packet", 
        json=data_packet, params=data_dict_params)
    if json.loads(data_dict_response.text).get("schema_check", False):
        # send the datastream to azure event hub
        send_datastreams_to_azure.datastream_producer(data_packet)
        return Response("Sent {} records to database".format(len(data_packet)), 200)
    else:
        return Response("Incorrectly formatted data packet", 422)

@app.route("/event/delete", methods=["DELETE"])
def delete_event():
    """Receive a list of event ids to be deleted in the post request json"""
    pass

@app.route("/event/update", methods=["POST"])
def update_event():
    pass


if __name__ == "__main__":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    print("running server on {}:{}".format(HOST_CONFIG['HOST_URL'], HOST_CONFIG['HOST_PORT']))
    app.run(HOST_CONFIG['HOST_URL'], port=HOST_CONFIG['HOST_PORT'])#, ssl_context='adhoc')
