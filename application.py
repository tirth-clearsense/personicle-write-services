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
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['PERSONICLE_AUTH_API_ENDPOINT']))
    print("event received")
    print(request.json)
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['PERSONICLE_AUTH_API_ENDPOINT'], headers=auth_headers)
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
    print(event_data_packet)
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
    auth_token = request.headers['Authorization']
    auth_headers = {"Authorization": "{}".format(auth_token)}
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['HOST_URL']+'/authenticate'))
    print(auth_headers)
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['HOST_URL']+'/authenticate', headers=auth_headers)
    
    print(auth_response.text, auth_response.status_code)
    if auth_response.status_code != requests.codes.ok or json.loads(auth_response.text).get("message", False)== False:
        return Response("Unauthorised access token", 401)

    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        data_packet = request.json
        print(data_packet)
    else:
        return Response('Content-Type not supported!', 415)
    
    # verify the event packet by making the data dictionary api call
    data_dict_params = {"data_type": "datastream"}

    # data_dict_response = requests.post(DATA_DICTIONARY_SERVER_SETTINGS['HOST_URL']+"/validate-data-packet", 
    #     json=data_packet, params=data_dict_params)
    data_dict_response = requests.post("http://127.0.0.1:5005/validate-data-packet", 
        json=data_packet, params=data_dict_params)
    # print(json.loads(data_dict_response.text))
    print(data_dict_response)
    if json.loads(data_dict_response.text).get("schema_check", False):
        print("hola")
        # send the datastream to azure event hub
        send_datastreams_to_azure.datastream_producer(data_packet)
        return Response("Sent {} records to database".format(len(data_packet)), 200)
    else:
        return Response("Incorrectly formatted data packet", 422)

def authenticate(request):
    auth_token = request.headers.get('Authorization',None)
    auth_headers = {"Authorization": "{}".format(auth_token)}
    print("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['PERSONICLE_AUTH_API_ENDPOINT']))
    logging.info("sending request to: {}".format(IDENTITY_SERVER_SETTINGS['PERSONICLE_AUTH_API_ENDPOINT']))
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['PERSONICLE_AUTH_API_ENDPOINT'], headers=auth_headers)
    print(auth_response.text, auth_response.status_code)
    return auth_response

def delete_datastreams_func(request,auth_response,delete_header):
    try:
        count = 0
        u_id = auth_response.json()['user_id']
        # if request.args.get('user_id') != u_id:
        #     return jsonify({"error":"user_id does not match access token. Unauthorized"}),401
        stream_source = request.args.get('source')
        # u_id=request.args.get('user_id')
        streams = request.args.get('stream_name',None)
        if streams is not None:
            stream_names = list(map(lambda e: e.lower(),streams.split(";") )) 
        else:
            stream_names = None
        # delete account request
        if stream_names is None and (delete_header == DELETE_USER['DELETE_USER_TOKEN']):
            model_class_user_datastreams = generate_table_class("user_datastreams", copy.deepcopy(base_schema['user_datastreams_store.avsc']))
            stmt = select(model_class_user_datastreams).where((model_class_user_datastreams.individual_id ==u_id) )
            all_user_datastreams = session.execute(stmt)
            user_data_streams = []
            for datastream_obj in all_user_datastreams.scalars():
               user_data_streams.append(datastream_obj.datastream)
            #    print(datastream_obj.datastream)
            # delete metadata
            query =  model_class_user_datastreams.__table__.delete().where(model_class_user_datastreams.individual_id == u_id) 
            session.execute(query)
            session.commit()
            if len(user_data_streams) > 0:
                records = 0
                count = 0
                for user_data_stream in user_data_streams:
                     params = {'data_type': 'datastream','stream_name': user_data_stream}
                     schema_response = requests.get(PERSONICLE_SCHEMA_API['MATCH_DICTIONARY_ENDPOINT']+"/match-data-dictionary",params=params).json()
                     table_name = schema_response['TableName']
                     model_class = generate_table_class(table_name, copy.deepcopy(base_schema[schema_response['base_schema']]))
                    #  print(schema_response)
                     query = model_class.__table__.delete().where(model_class.individual_id == u_id) 
                     res = session.execute(query)
                     count+=1
                     records+=res.rowcount
                     session.commit()
                
                if count > 0 and records > 0:
                    return jsonify({"message": f"Deleted all {count} datastreams for {u_id}. Deleted {records} records"}),200
            else:
                return jsonify({"message": f"No datastream for user {u_id} exists"}),400

        
        for stream in stream_names or []:
            params = {'data_type': 'datastream','stream_name': stream}
            schema_response = requests.get(PERSONICLE_SCHEMA_API['MATCH_DICTIONARY_ENDPOINT']+"/match-data-dictionary",params=params)
            if not schema_response.status_code == 200:
                return jsonify({"error": f"Stream {stream} is not valid"}),400
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
                    (model_class.timestamp.between(start_time_object,end_time_object)) & (model_class.source == stream_source))
                else : 
                    query = model_class.__table__.delete().where( (model_class.individual_id == u_id) & (model_class.timestamp >= start_time_object ) 
                    & (model_class.source == stream_source))

            elif start_time_query is None and end_time_query is not None:
                    query = model_class.__table__.delete().where( (model_class.individual_id == u_id) & (model_class.timestamp <= end_time_object ) 
                    & (model_class.source == stream_source))
            else:
                # both start and end time is none, delete specified datastreams
                 query = model_class.__table__.delete().where( (model_class.individual_id == u_id) & (model_class.source == stream_source))
            result = session.execute(query)
            count+=result.rowcount
            session.commit()

       
        if count > 0:
            return jsonify({"message": f"Deleted {count} {p.plural('datastream'),count}"}),200
        else:
            return jsonify({"message": f" One or more data stream not deleted. No such data stream found"}), 400
    except requests.exceptions.RequestException as e:
        print(e)


@app.route("/datastream/delete", methods=["DELETE"])
@ValidateParameters()
def delete_datastream(user_id: str=Query(), stream_name: str=Query(), source: str=Query(),start_time: Optional[str]=Query(), end_time: Optional[str]=Query()):
    auth_response = authenticate(request)
    if auth_response.status_code == 401:
        return Response("Unauthorized", 401)
    return delete_datastreams_func(request,auth_response=None)


def delete_events_func(request,auth_response, event_type,event_id,delete_header):
    try:
        if event_type is None and event_id is None and delete_header is None:
            return jsonify({"error": "event_id or event_type should be specified"}),400
        elif event_type is not None and event_id is not None:
            return jsonify({"error": "Either event_id or event_type should be included but not both"}),400
           
        u_id = auth_response.json()['user_id']
        event_name = list(map(lambda e: e.lower(),request.args['event_type'].split(";") )) if event_type is not None else None
        event_ids = request.args['event_id'].split(";") if event_id is not None else None

        start_time_query = request.args.get('start_time',None)
        end_time_query = request.args.get('end_time',None)
        
        model_class = generate_table_class(EVENTS_TABLE, copy.deepcopy(base_schema[EVENTS_SCHEMA]))
        if delete_header == DELETE_USER['DELETE_USER_TOKEN']:
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


@app.route("/event/delete", methods=["DELETE"])
@ValidateParameters()
def delete_event(user_id: str=Query(), event_type: Optional[str]=Query(), event_id: Optional[str]=Query(), start_time: Optional[str]=Query(), end_time: Optional[str]=Query()):
    auth_response = authenticate(request)
    if auth_response.status_code == 401:
        return Response("Unauthorized", 401)
    delete_header = request.headers.get('DELETE_DATA', None)
    return delete_events_func(request,auth_response,event_type,event_id,delete_header)
   
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
    auth_response = requests.get(IDENTITY_SERVER_SETTINGS['PERSONICLE_AUTH_API_ENDPOINT'], headers=auth_headers)

    if auth_response.status_code == 401:
        return jsonify({"error": "Unauthorized"}), 401
    u_id = auth_response.json()['user_id']
    delete_user_events = delete_events_func(request,auth_response,None,None,DELETE_USER['DELETE_USER_TOKEN'])
    delete_user_datastreams = delete_datastreams_func(request,auth_response,DELETE_USER['DELETE_USER_TOKEN'])

    logging.info(delete_user_events)
    logging.info(delete_user_datastreams)
    print(delete_user_datastreams)
    delete_user_headers = {"Authorization": DELETE_USER['DELETE_USER_TOKEN']}
    deactivate_user_account = requests.delete(f"{DELETE_USER['API_ENDPOINT']}/{u_id}?sendEmail=true",headers=delete_user_headers)
   
    logging.info("deactivate user account")
    if delete_user_events[1] == 200 and delete_user_datastreams[1]==200 and deactivate_user_account.status_code == 204:
        return delete_user_account(u_id,delete_user_headers,events_found=True,datastreams_found=True)
    elif delete_user_events[1] != 200 and delete_user_datastreams[1]!=200 and deactivate_user_account.status_code == 204:
        return delete_user_account(u_id,delete_user_headers,events_found=False,datastreams_found=False)
    elif delete_user_events[1] != 200 and delete_user_datastreams[1]==200 and deactivate_user_account.status_code == 204:
         return delete_user_account(u_id,delete_user_headers,events_found=False,datastreams_found=True)
    elif delete_user_events[1] == 200 and delete_user_datastreams[1]!=200 and deactivate_user_account.status_code == 204:
        return delete_user_account(u_id,delete_user_headers,events_found=True,datastreams_found=False)
    return jsonify({"error": f"Unable to delete {u_id} or user data"}), 400

def delete_user_account(u_id,delete_user_headers,events_found=None,datastreams_found=None):
    logging.info("inside delete user account")
    delete_user_account = requests.delete(f"{DELETE_USER['API_ENDPOINT']}/{u_id}?sendEmail=true",headers=delete_user_headers)
    if events_found:
        if datastreams_found and delete_user_account.status_code == 204:
                return jsonify({"message": f"User {u_id} DELETED and all events and datastreams deleted."}), 200
        elif datastreams_found==False and delete_user_account.status_code == 204:
                return jsonify({"message": f"User {u_id} DELETED and all events deleted. No datastreams found to be deleted."}), 200
        return jsonify({"message": f"User {u_id} DEACTIVATED and all events and datastreams deleted."}), 200
    else :
        if datastreams_found and delete_user_account.status_code == 204:
                return jsonify({"message": f"User {u_id} DELETED. All datastreams deleted. No events found."}), 200
        elif datastreams_found==False and delete_user_account.status_code == 204:
                return jsonify({"message": f"User {u_id} DELETED. No events or datastreams found to be deleted."}), 200       
        return jsonify({"message": f"User {u_id} DEACTIVATED. No data found to be deleted."}), 200

if __name__ == "__main__":
    # os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    print("running server on {}:{}".format(HOST_CONFIG['HOST_URL'], HOST_CONFIG['HOST_PORT']))

    app.run(HOST_CONFIG['HOST_URL'], port=HOST_CONFIG['HOST_PORT'], debug=True, ssl_context='adhoc')
#     app.run()
   
