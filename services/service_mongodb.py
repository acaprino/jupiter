from typing import Optional, List

from pymongo import MongoClient
import asyncio

from pymongo.errors import ConnectionFailure

from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin


class MongoDBService(LoggingMixin):
    def __init__(self, config: ConfigReader, host: str, port: int, username: str, password: str, db_name: str):
        super().__init__(config)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self._lock = asyncio.Lock()
        self.loop = asyncio.get_event_loop()
        self.config = config
        self.agent = "MongoDB-Service"

    @exception_handler
    async def _run_blocking(self, func, *args, **kwargs):
        return await self.loop.run_in_executor(
            None,
            lambda: func(*args, **kwargs)
        )

    def _connect(self):
        self.info(f"Connecting to MongoDB at {self.host}:{self.port}...")

        # Costruzione della URI con autenticazione
        uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        self.client = MongoClient(uri)

        try:
            # Verifica la connessione ottenendo le informazioni del server
            self.client.server_info()
        except Exception as e:
            self.error(f"Connection failed", exec_info=e)
            raise e

        # Selezione (e creazione implicita) del database
        self.db = self.client[self.db_name]

        self.info("MongoDB connection established.")

    def _disconnect(self):
        self.info("Disconnecting from MongoDB...")
        self.client.close()
        self.client = None
        self.db = None
        self.info("MongoDB disconnected.")

    def _upsert(self, collection: str, id_object: any, payload: any) -> Optional[dict]:
        try:
            db = self.client[self.db_name]
            coll = db[collection]

            upsert_operation = {"$set": payload}
            result = coll.update_one(id_object, upsert_operation, upsert=True)

            if result.upserted_id:
                return {"ids": [result.upserted_id]}
            else:
                if isinstance(id_object, dict) and "_id" in id_object:
                    return {"ids": [id_object["_id"]]}
                else:
                    doc = coll.find_one(id_object)
                    if doc and "_id" in doc:
                        return {"ids": [doc["_id"]]}
                    else:
                        return {"ids": []}
        except Exception as e:
            self.error(f"An error occurred while updating the document: {e}")
            return None

    def _find_one(self, collection: str, id_object: any) -> Optional[dict]:
        db = self.client[self.db_name]
        collection = db[collection]
        try:
            document = collection.find_one(id_object)
            return document
        except Exception as e:
            self.error(f"An error occurred while retrieving the document: {e}")
            return None

    def _find_many(self, collection: str, filter: dict) -> Optional[List]:
        db = self.client[self.db_name]
        collection = db[collection]
        try:
            documents = list(collection.find(filter))
            return documents
        except Exception as e:
            self.error(f"An error occurred while retrieving documents with filter {filter}: {e}")
            return None

    def _create_index(self, collection: str, index_field: str, unique: bool = False):
        """
        Creates an index on the specified collection if it does not already exist.

        :param collection: Name of the collection
        :param index_field: Field to index
        :param unique: Whether the index should enforce uniqueness
        """
        db = self.client[self.db_name]
        coll = db[collection]

        # Retrieve information about existing indexes
        indexes = coll.index_information()

        # Check if an index on the specified field with the required unique property already exists
        for index_name, index_details in indexes.items():
            # Skip the default _id index
            if index_name == '_id_':
                continue
            if index_details.get("key") == [(index_field, 1)] and index_details.get("unique", False) == unique:
                self.info(f"Index on field '{index_field}' with unique={unique} already exists. No action taken.")
                return

        # If the index does not exist, create it
        try:
            coll.create_index(index_field, unique=unique)
            self.info(f"Index created on field '{index_field}' with unique={unique}.")
        except Exception as e:
            self.error(f"Error occurred while creating the index", exec_info=e)

    def _test_connection(self) -> bool:
        """
        Tests the connection to MongoDB by executing a ping command.
        Returns True if the connection is successful, otherwise False.
        """
        try:
            # The admin database is always present
            self.client.admin.command('ping')
            print("Successfully connected to MongoDB.")
            return True
        except ConnectionFailure as e:
            self.error(f"Failed to connect to MongoDB", exec_info=e)
            return False
        except Exception as e:
            self.error(f"Error during MongoDB connection test", exec_info=e)
            return False

    @exception_handler
    async def connect(self):
        await self._run_blocking(self._connect)

    @exception_handler
    async def disconnect(self):
        await self._run_blocking(self._disconnect)

    @exception_handler
    async def upsert(self, collection: str, id_object: any, payload: any) -> Optional[dict]:
        return await self._run_blocking(self._upsert, collection, id_object, payload)

    @exception_handler
    async def find_one(self, collection: str, id_object: any):
        return await self._run_blocking(self._find_one, collection, id_object)

    @exception_handler
    async def find_many(self, collection: str, filter: dict):
        return await self._run_blocking(self._find_many, collection, filter)

    @exception_handler
    async def create_index(self, collection: str, index_field: str, unique: bool = False):
        await self._run_blocking(self._create_index, collection, index_field, unique)

    @exception_handler
    async def test_connection(self) -> bool:
        return await self._run_blocking(self._test_connection)
