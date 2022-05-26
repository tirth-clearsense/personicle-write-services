from flask import Flask, Response, jsonify, request
import requests
from config import AVRO_SCHEMA_LOC, IDENTITY_SERVER_SETTINGS, DATA_DICTIONARY_SERVER_SETTINGS, EVENTHUB_CONFIG, HOST_CONFIG,DELETE_USER, PERSONICLE_SCHEMA_API
import json
import os
from producer import send_records_azure, send_datastreams_to_azure
from flask_parameter_validation import ValidateParameters,Query
from typing import List, Optional
from sqlalchemy import delete,select
from db_controllers import session
from models.postgresdb import generate_table_class
from models.base_schema import base_schema
from copy import copy
import copy
from sqlalchemy import func
from datetime import datetime
import inflect
import logging

p = inflect.engine()

app = Flask(__name__)

PROJ_LOC = os.path.dirname(__file__)
EVENTS_TABLE = "personal_events"
EVENTS_SCHEMA = "event_schema.avsc"
logging.basicConfig(filename='test.log', level=logging.DEBUG)
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
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+":"+IDENTITY_SERVER_SETTINGS["HOST_PORT"]+'/authenticate'))
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+":"+IDENTITY_SERVER_SETTINGS["HOST_PORT"]+'/authenticate', headers=auth_headers, verify=False)
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
        data_dict_response = requests.post(DATA_DICTIONARY_SERVER_SETTINGS['HOST_URL']+':'+DATA_DICTIONARY_SERVER_SETTINGS['HOST_PORT']+"/validate-data-packet", 
            json=event, params=data_dict_params, verify=False)
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
@app.route("/datastream/delete", methods=["DELETE"])
@ValidateParameters()
def delete_datastream(user_id: str=Query(), stream_name: str=Query(), start_time: Optional[str]=Query(), end_time: Optional[str]=Query()):
    auth_token = request.headers['Authorization']
    auth_headers = {"Authorization": "{}".format(auth_token)}
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+"/auth/authenticate"))
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+"/auth/authenticate", headers=auth_headers)
    print(auth_response.text, auth_response.status_code)
    if auth_response.status_code == 401:
        return Response("Unauthorized", 401)
    try:
        count = 0
        u_id = auth_response.json()['user_id']
        stream_names = list(map(lambda e: e.lower(),request.args['stream_name'].split(";") ))
        for stream in stream_names:
            params = {'data_type': 'datastream','stream_name': stream}
            schema_response = requests.get(PERSONICLE_SCHEMA_API['MATCH_DICTIONARY_ENDPOINT']+"/match-data-dictionary",params=params)
            if not schema_response.status_code == 200:
                return jsonify({"error": "One of the stream name not found "}),400
            stream_information = schema_response.json()

            start_time_query = request.args.get('start_time',None)
            end_time_query = request.args.get('end_time',None)
            try:
                start_time_object = datetime.strptime(start_time_query,'%Y-%m-%d %H:%M:%S') if start_time_query is not None else None
                end_time_object = datetime.strptime(end_time_query,'%Y-%m-%d %H:%M:%S') if end_time_query is not None else None
            except ValueError:
                    return Response("start_time and end_time should be in %Y-%m-%d %H:%M:%S format", 400)

            table_name = stream_information['TableName']
            model_class = generate_table_class(table_name, copy.deepcopy(base_schema[stream_information['base_schema']]))
            if start_time_query is not None:
            
                if end_time_query is not None:
                    query = model_class.__table__.delete().where( (model_class.individual_id == u_id)  & 
                    (model_class.timestamp.between(start_time_object,end_time_object)))
                else : 
                    query = model_class.__table__.delete().where( (model_class.individual_id == u_id) & (model_class.timestamp >= start_time_object ))

            elif start_time_query is None and end_time_query is not None:
                    query = model_class.__table__.delete().where( (model_class.individual_id == u_id) & (model_class.timestamp <= end_time_object ))
            else:
                # no start and end time specified, delete all events specified
                query = model_class.__table__.delete().where( (model_class.individual_id == u_id) )
            result = session.execute(query)
            count+=result.rowcount
            session.commit()
        
        if count > 0:
            return jsonify({"message": f"Deleted {count} {p.plural('datastream'),count}"}),200
        else:
            return jsonify({"message": f" One or more data stream not deleted. No such data stream found"}), 400
    except requests.exceptions.RequestException as e:
        print(e)


@app.route("/event/delete", methods=["DELETE"])
@ValidateParameters()
def delete_event(user_id: str=Query(), event_type: Optional[str]=Query(), event_id: Optional[str]=Query(), start_time: Optional[str]=Query(), end_time: Optional[str]=Query()):
    logging.info("inside delete_event function")
    auth_token = request.headers['Authorization']
    print("in /event/delete")
    auth_headers = {"Authorization": "{}".format(auth_token)}
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+"/auth/authenticate"))
    logging.info("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+"/auth/authenticate"))
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+"/auth/authenticate", headers=auth_headers)
    print(auth_response.text, auth_response.status_code)
    if auth_response.status_code == 401:
        return Response("Unauthorized", 401)
    args = request.args
    # print(args)
    delete__header = request.headers.get('DELETE_DATA', None)
    
    try:
        if event_type is None and event_id is None and delete__header is None:
            return jsonify({"error": "event_id or event_type should be specified"}),400
        elif event_type is not None and event_id is not None:
            return jsonify({"error": "Either event_id or event_type should be included but not both"}),400
           
        u_id = auth_response.json()['user_id']
        event_name = list(map(lambda e: e.lower(),request.args['event_type'].split(";") )) if event_type is not None else None
        event_ids = request.args['event_id'].split(";") if event_id is not None else None

        start_time_query = request.args.get('start_time',None)
        end_time_query = request.args.get('end_time',None)
        
        model_class = generate_table_class(EVENTS_TABLE, copy.deepcopy(base_schema[EVENTS_SCHEMA]))
        if delete__header == DELETE_USER['DELETE_USER_TOKEN']:
            delete_all_user_info_query =  model_class.__table__.delete().where( (model_class.user_id == u_id))
            result = session.execute(delete_all_user_info_query)
            total_deleted_events = result.rowcount
            if total_deleted_events != 0:
                session.commit()
                return jsonify({"message": f"Deleted {total_deleted_events}  {p.plural('event'),total_deleted_events}" }),200
            else:
                session.rollback()
                return jsonify({"message": f"No events found/deleted"}), 400

        if event_ids:
            query = model_class.__table__.delete().where( (model_class.user_id == u_id) & (model_class.unique_event_id.in_(event_ids)))
            result = session.execute(query)
            total_events = result.rowcount
            if total_events != 0:
                session.commit()
                return jsonify({"message": f"Deleted {total_events} {event_ids} {p.plural('event'),total_events}" }),200
            else:
                session.rollback()
                return jsonify({"message": f"No such {event_ids} found"}), 400
       
        try:
                start_time_object = datetime.strptime(start_time_query,'%Y-%m-%d %H:%M:%S') if start_time_query is not None else None
                end_time_object = datetime.strptime(end_time_query,'%Y-%m-%d %H:%M:%S') if end_time_query is not None else None
        except ValueError:
                return Response("start_time and end_time should be in %Y-%m-%d %H:%M:%S format", 400)
        if start_time_query is not None:
           
            if end_time_query is not None:
                query = model_class.__table__.delete().where( (model_class.user_id == u_id)  & 
                (model_class.start_time.between(start_time_object,end_time_object)) & (func.lower(model_class.event_name).in_(event_name)))
            else : 
                # end time is none
                # delete all events after start time
                query = model_class.__table__.delete().where( (model_class.user_id == u_id) & (func.lower(model_class.event_name).in_(event_name)) & 
                (model_class.start_time >= start_time_object ))

        elif start_time_query is None and end_time_query is not None:
             query = model_class.__table__.delete().where( (model_class.user_id == u_id) & (func.lower(model_class.event_name).in_(event_name)) & 
                (model_class.end_time <= end_time_object ))
        else:
            # no start and end time specified, delete all events specified
            query = model_class.__table__.delete().where( (model_class.user_id == u_id) & (func.lower(model_class.event_name).in_(event_name)) )

        result = session.execute(query)
        total_events = result.rowcount
        if total_events != 0:
            session.commit()
            return jsonify({"message": f"Deleted {total_events} {event_name} {p.plural('event'),total_events}"}),200
        else:
            session.rollback()
            return jsonify({"message": f"No such {event_name} found"}), 400
    except requests.exceptions.RequestException as e:
        print(e) 
    """Receive a list of event ids to be deleted in the post request json"""
    

@app.route("/event/update", methods=["POST"])
def update_event():
    pass

@app.route('/account/delete', methods=["DELETE"])
@ValidateParameters()
def delete_account():
    auth_token = request.headers['Authorization']
    auth_headers = {"Authorization": "{}".format(auth_token),
                    "DELETE_DATA": DELETE_USER['DELETE_USER_TOKEN']
                   }
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+"/auth/authenticate", headers=auth_headers)
    
    if auth_response.status_code == 401:
        return jsonify({"error": "Unauthorized"}), 401
    u_id = auth_response.json()['user_id']
    logging.info("here")
    # delete_user_events = requests.delete(f"http://127.0.0.1:5000/event/delete?user_id={u_id}",headers=auth_headers)
    delete_user_events = requests.delete(f"{HOST_CONFIG['STAGING_URL']}/event/delete?user_id={u_id}",headers=auth_headers)
    logging.info("after delete user events")
    logging.info(delete_user_events.json())
    delete_user_headers = {"Authorization": DELETE_USER['DELETE_USER_TOKEN']}
    deactivate_user_account = requests.delete(f"{DELETE_USER['API_ENDPOINT']}/{u_id}?sendEmail=true",headers=delete_user_headers)
    logging.info(deactivate_user_account.json())
    logging.info("deactivate user account")
    if delete_user_events.status_code == 200 and deactivate_user_account.status_code == 204:
        return delete_user_account(u_id,delete_user_headers,events_found=True)
    elif delete_user_events.status_code != 200 and  deactivate_user_account.status_code == 204:
        return delete_user_account(u_id,delete_user_headers)
    return jsonify({"error": f"Unable to delete {u_id} or user data"}), 400

def delete_user_account(u_id,delete_user_headers,events_found=None):
    logging.info("inside delete user account")
    delete_user_account = requests.delete(f"{DELETE_USER['API_ENDPOINT']}/{u_id}?sendEmail=true",headers=delete_user_headers)
    if events_found:
        if delete_user_account.status_code == 204:
                return jsonify({"message": f"User {u_id} deleted and all data deleted"}), 200
        return jsonify({"message": f"User {u_id} deactivated and all data deleted"}), 200
    else :
        if delete_user_account.status_code == 204:
                return jsonify({"message": f"User {u_id} DELETED. No data found to be deleted"}), 200
        return jsonify({"message": f"User {u_id} DEACTIVATED. No data found to be deleted"}), 200

if __name__ == "__main__":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    print("running server on {}:{}".format(HOST_CONFIG['HOST_URL'], HOST_CONFIG['HOST_PORT']))
    app.run(HOST_CONFIG['HOST_URL'], port=HOST_CONFIG['HOST_PORT'], debug=True)#, ssl_context='adhoc')
    # app.run(debug=True)
