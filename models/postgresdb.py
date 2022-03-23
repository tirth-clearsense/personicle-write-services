from asyncio.log import logger
import logging
import traceback
from flask import session
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from config import DB_CREDENTIALS as database
# from configparser import ConfigParser

# config_object = ConfigParser()
# config_object.read("config.ini")
# database = config_object["CREDENTIALS_DATABASE"]

logger = logging.getLogger(__name__)

engine = create_engine("postgresql://{username}:{password}@{dbhost}/{dbname}".format(username=database['USERNAME'], password=database['PASSWORD'],
                                                                                                        dbhost=database['HOST'], dbname=database['NAME']),
                        pool_pre_ping=True)


Base = declarative_base(engine)

TABLE_MODELS = {}

def generate_table_class(table_name: str, base_schema: dict):
    if table_name in TABLE_MODELS:
        return TABLE_MODELS[table_name]
    try:
        base_schema['__tablename__'] = table_name
        base_schema['__table_args__'] = {'extend_existing': True}
        generated_model = type(table_name, (Base, ), base_schema)
        generated_model.__table__.create(bind=engine, checkfirst=True)
        TABLE_MODELS[table_name] = generated_model
    except Exception as e:
        logger.error(traceback.format_exc())
        generated_model = None
    return generated_model

# class Heartrate(Base):
#     __table__ = Base.metadata.tables['heartrate']

#     def __repr__(self):
#         return '''<Heartrate(individual_id='{0}', timestamp='{1}', source='{2}', value='{3}', unit='{4}', confidence='{5}')>'''.format(self.individual_id,
#         self.timestamp, self.source, self.value, self.unit, self.confidence)

# class HeartIntensityMinutes(Base):
#     __table__ = Base.metadata.tables['heart_intensity_minutes']

    # add string __repr__(self) here for this data type

# class ActiveMinutes(Base):
#     __table__ = Base.metadata.tables['active_minutes']

    # add string __repr__(self) here for this data type

    

def loadSession():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

    