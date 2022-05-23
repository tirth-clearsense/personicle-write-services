import os
import json
import pathlib
import configparser

__file_path = os.path.abspath(__file__)
__dir_path = os.path.dirname(__file_path)

PROJ_LOC=pathlib.Path(__dir_path)
AVRO_SCHEMA_LOC=os.path.join(PROJ_LOC, "avro_modules")

# Database url format
# dialect+driver://username:password@host:port/database
# postgresql+pg8000://dbuser:kx%25jj5%2Fg@pghost10/appdb

# IDENTITY_SERVICE_AUTH_URL, DATA_DICTIONARY_VERIFY_URL, EVENTHUB_CONFIG, WRITE_SERVICE_URL, WRITE_SERVICE_PORT
# DB_CREDENTIALS
if int(os.environ.get("INGESTION_PROD", '0')) != 1:
    print("in the dev environment")
    print("environment variables: {}".format(list(os.environ.keys())))

    __app_config = configparser.ConfigParser()
    __app_config.read(os.path.join(PROJ_LOC,'config.ini'))

    DATA_DICTIONARY_SERVER_SETTINGS = __app_config['DATA_DICTIONARY_SERVER']
    IDENTITY_SERVER_SETTINGS = __app_config['IDENTITY_SERVER']
    EVENTHUB_CONFIG = __app_config['EVENTHUB']
    PERSONICLE_SCHEMA_API = __app_config['PERSONICLE_SCHEMA_API']
    HOST_CONFIG = __app_config['DATA_WRITE_SERVICE']
    DB_CREDENTIALS = __app_config['CREDENTIALS_DATABASE']

else:
    print("in the prod environment")
    try:
        DATA_DICTIONARY_SERVER_SETTINGS = {
            'HOST_URL': os.environ.get('DATA_DICTIONARY_HOST', "0.0.0.0"),
            'HOST_PORT': os.environ.get('DATA_DICTIONARY_PORT', 5001)
        }

        IDENTITY_SERVER_SETTINGS = {
            'HOST_URL': os.environ.get('IDENTITY_SERVER_HOST', "0.0.0.0"),
            'HOST_PORT': os.environ.get('IDENTITY_SERVER_PORT', 5002)
        }

        EVENTHUB_CONFIG = {
            'CONNECTION_STRING': os.environ['EVENTHUB_CONNECTION_STRING'],
            'EVENTHUB_NAME': os.environ['EVENTHUB_NAME'],
            'SCHEMA_REGISTRY_FQNS': os.environ['EVENTHUB_SCHEMA_REGISTRY_FQNS'],
            'SCHEMA_REGISTRY_GROUP': os.environ['EVENTHUB_SCHEMA_REGISTRY_GROUP'],
            'DATASTREAM_EVENTHUB_CONNECTION_STRING': os.environ['DATASTREAM_EVENTHUB_CONNECTION_STRING'],
            'DATASTREAM_EVENTHUB_NAME': os.environ['DATASTREAM_EVENTHUB_NAME']
        }

        HOST_CONFIG = {
            'HOST_URL': os.environ.get('DATA_WRITE_SERVER_HOST', "0.0.0.0"),
            'HOST_PORT': os.environ.get('DATA_WRITE_SERVER_PORT', 5004)
        }

        DB_CREDENTIALS = {
            "USERNAME": os.environ['CREDENTIALS_DB_USER'],
            "PASSWORD": os.environ['CREDENTIALS_DB_PASSWORD'],
            "HOST": os.environ['CREDENTIALS_DB_HOST'],
            "NAME": os.environ['CREDENTIALS_DB_NAME']
        }
    except KeyError as e:
        print("failed to create configs for the application")
        print("missing configuration {} from environment variables".format(e))
        raise e