import asyncio

from typing import Optional, List
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin


class MongoDBService(LoggingMixin):
    def __init__(
            self,
            config: ConfigReader,
            host: str,
            port: int,
            username: str,
            password: str,
            db_name: str,
            is_cluster: bool = False
    ):
        """
        Initialize the MongoDBService with parameters for both standalone and cluster connections.

        :param config: Configuration reader instance.
        :param host: Hostname for MongoDB or the cluster.
        :param port: Port for MongoDB (ignored if is_cluster=True).
        :param username: MongoDB username.
        :param password: MongoDB password.
        :param db_name: Database name to connect to.
        :param is_cluster: Boolean flag indicating if the connection is to a MongoDB cluster.
        """
        super().__init__(config)
        self.config = config
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.is_cluster = is_cluster  # Flag to indicate connection mode
        self._lock = asyncio.Lock()
        self.loop = asyncio.get_event_loop()
        self.agent = "MongoDB-Service"

    @exception_handler
    async def _run_blocking(self, func, *args, **kwargs):
        """
        Runs a blocking function in an executor to prevent blocking the event loop.
        """
        return await self.loop.run_in_executor(None, lambda: func(*args, **kwargs))

    def _connect(self):
        """
        Connects to MongoDB using either standalone or cluster configuration.
        If connecting to a cluster, uses the mongodb+srv scheme.
        """
        if self.is_cluster:
            self.info("Connecting to MongoDB Cluster using is_cluster flag...")
            # Construct the URI with mongodb+srv scheme for cluster connections.
            uri = (f"mongodb+srv://{self.username}:{self.password}@"
                   f"{self.host}/{self.db_name}?retryWrites=true&w=majority")
        else:
            self.info(f"Connecting to MongoDB at {self.host}:{self.port}...")
            uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.db_name}"

        self.client = MongoClient(uri)

        try:
            # Verify the connection by attempting to retrieve server information.
            self.client.server_info()
        except Exception as e:
            self.error("Connection failed", exec_info=e)
            raise e

        # Select (and implicitly create, if needed) the database.
        self.db = self.client[self.db_name]
        self.info("MongoDB connection established.")

    def _disconnect(self):
        """
        Disconnects from MongoDB by closing the client connection.
        """
        self.info("Disconnecting from MongoDB...")
        self.client.close()
        self.client = None
        self.db = None
        self.info("MongoDB disconnected.")

    def _upsert(self, collection: str, id_object: dict, payload: any) -> Optional[dict]:
        """
        Performs an upsert (update-insert) operation on the specified collection.

        :param collection: The name of the collection.
        :param id_object: Filter to identify the document.
        :param payload: Document data to set/update.
        :return: A dictionary with the upserted document id(s) or None if an error occurred.
        """
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
            self.error(f"An error occurred while updating the document: {e}", exec_info=e)
            return None

    def _find_one(self, collection: str, id_object: any) -> Optional[dict]:
        """
        Retrieves a single document from the specified collection based on the provided filter.

        :param collection: The name of the collection.
        :param id_object: Filter to identify the document.
        :return: The document if found, otherwise None.
        """
        db = self.client[self.db_name]
        coll = db[collection]
        try:
            document = coll.find_one(id_object)
            return document
        except Exception as e:
            self.error(f"An error occurred while retrieving the document: {e}", exec_info=e)
            return None

    def _find_many(self, collection: str, filter: dict) -> Optional[List]:
        """
        Retrieves multiple documents from the specified collection that match the filter.

        :param collection: The name of the collection.
        :param filter: Dictionary containing the filter criteria.
        :return: A list of matching documents or None if an error occurred.
        """
        db = self.client[self.db_name]
        coll = db[collection]
        try:
            documents = list(coll.find(filter))
            return documents
        except Exception as e:
            self.error(f"An error occurred while retrieving documents with filter {filter}: {e}", exec_info=e)
            return None

    def _create_index(self, collection: str, index_field: str, unique: bool = False):
        """
        Creates an index on the specified collection field if it does not already exist.

        :param collection: The name of the collection.
        :param index_field: The field name to index.
        :param unique: A flag to enforce unique values in the index.
        """
        db = self.client[self.db_name]
        coll = db[collection]

        # Retrieve existing index information.
        indexes = coll.index_information()

        # Check if an index on the specified field with the desired uniqueness already exists.
        for index_name, index_details in indexes.items():
            if index_name == '_id_':
                continue  # Skip the default _id index.
            if index_details.get("key") == [(index_field, 1)] and index_details.get("unique", False) == unique:
                self.info(f"Index on field '{index_field}' with unique={unique} already exists. No action taken.")
                return

        # Create the index if it does not exist.
        try:
            coll.create_index(index_field, unique=unique)
            self.info(f"Index created on field '{index_field}' with unique={unique}.")
        except Exception as e:
            self.error("Error occurred while creating the index", exec_info=e)

    def _test_connection(self) -> bool:
        """
        Tests the connection to MongoDB by issuing a ping command.

        :return: True if the connection is successful, otherwise False.
        """
        try:
            # The admin database is always present.
            self.client.admin.command('ping')
            print("Successfully connected to MongoDB.")
            return True
        except ConnectionFailure as e:
            self.error("Failed to connect to MongoDB", exec_info=e)
            return False
        except Exception as e:
            self.error("Error during MongoDB connection test", exec_info=e)
            return False

    @exception_handler
    async def connect(self):
        """
        Asynchronously connects to MongoDB.
        """
        await self._run_blocking(self._connect)

    @exception_handler
    async def disconnect(self):
        """
        Asynchronously disconnects from MongoDB.
        """
        await self._run_blocking(self._disconnect)

    @exception_handler
    async def upsert(self, collection: str, id_object: dict, payload: any) -> Optional[dict]:
        """
        Asynchronously performs an upsert operation on a document in the given collection.
        """
        return await self._run_blocking(self._upsert, collection, id_object, payload)

    @exception_handler
    async def find_one(self, collection: str, id_object: any):
        """
        Asynchronously retrieves a single document from the given collection.
        """
        return await self._run_blocking(self._find_one, collection, id_object)

    @exception_handler
    async def find_many(self, collection: str, filter: dict):
        """
        Asynchronously retrieves multiple documents from the given collection.
        """
        return await self._run_blocking(self._find_many, collection, filter)

    @exception_handler
    async def create_index(self, collection: str, index_field: str, unique: bool = False):
        """
        Asynchronously creates an index on the specified field in the given collection.
        """
        await self._run_blocking(self._create_index, collection, index_field, unique)

    @exception_handler
    async def test_connection(self) -> bool:
        """
        Asynchronously tests the connection to MongoDB.
        """
        return await self._run_blocking(self._test_connection)
