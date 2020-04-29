import os
import json
import uuid
from datetime import datetime

from cassandra.cluster import Cluster
from cassandra.cqlengine import (
    columns,
    ValidationError,
)
from cassandra.cqlengine.connection import register_connection, set_default_connection
from cassandra.cqlengine.models import Model
from cassandra.cqlengine.management import sync_table, create_keyspace_simple, drop_keyspace
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

__all__ = ['Cassandra']


class userModel(Model):
    __keyspace__ = 'streamkeyspace'
    __table_name__ = 'user'

    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    username = columns.Text(required=True)
    email = columns.Text(required=True)
    password= columns.Text(required=True)
    created_time = columns.DateTime(default=datetime.utcnow)

class fileModel(Model):
    __keyspace__ = 'streamkeyspace'
    __table_name__ = 'file'
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    name = columns.Text(required=True)
    content_length = columns.BigInt(required=True)
    userid = columns.UUID(primary_key=True,required=True)

class fileChunk(Model):
    __keyspace__ = 'streamkeyspace'
    __table_name__ = 'file_chunk'
    chunk_id = columns.BigInt(primary_key=True,required=True)
    file_id = columns.UUID(primary_key=True,required=True)
    content = columns.Blob(required=True)

    

class Cassandra:
    session = None
    cluster = None

    def __init__(self):
        self.connect()
        self.key_space = "streamkeyspace"

    def connect(self):
        cloud_config = {
        'secure_connect_bundle': 'cassandra-config.zip'
        }
        auth_provider = PlainTextAuthProvider(username='stream', password='1111111')

        self.cluster = Cluster(
            connect_timeout=10000,
            cloud=cloud_config,
            auth_provider=auth_provider
            # executor_threads=int(os.getenv('CASSANDRA_EXECUTOR_THREADS')),
            # protocol_version=int(os.getenv('CASSANDRA_PROTOCOL_VERSION')),
        )
        self.cluster.default_timeout=10000

        self.session = self.cluster.connect()

        register_connection(str(self.session), session=self.session)
        set_default_connection(str(self.session))

    def sync_table(self):
        return sync_table(userModel)

    def write(self, modelClass,**data):
        try:
            model_data=modelClass.create(**data)
            print('Cassandra Data: {}'.format(dict(model_data)))
            return model_data
        except ValidationError as e:
            print(e)
        return True

    def disconnect(self):
        self.cluster.shutdown()
