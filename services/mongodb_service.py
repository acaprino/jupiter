from pymongo import MongoClient
import asyncio

from pymongo.errors import ConnectionFailure

from misc_utils.bot_logger import BotLogger
from misc_utils.error_handler import exception_handler


class MongoDB:
    def __init__(self, bot_name: str, host: str, port: int, db_name: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self._lock = asyncio.Lock()
        self.loop = asyncio.get_event_loop()
        self.logger = BotLogger.get_logger(bot_name)

    @exception_handler
    async def _run_blocking(self, func, *args, **kwargs):
        return await self.loop.run_in_executor(
            None,
            lambda: func(*args, **kwargs)
        )

    def _connect(self):
        self.logger.info(f"Connecting to MongoDB at {self.host}:{self.port}...")
        self.client = MongoClient(self.host, self.port)
        self.db = self.client[self.db_name]
        self.logger.info("MongoDB connection established.")

    def _disconnect(self):
        self.logger.info("Disconnecting from MongoDB...")
        self.client.close()
        self.client = None
        self.db = None
        self.logger.info("MongoDB disconnected.")

    def _upsert(self, collection: str, id_object: any, payload: any):
        db = self.client[self.db_name]
        collection = db[collection]

        upsert_operation = {
            "$set": payload
        }
        try:
            result = collection.update_one(id_object, upsert_operation, upsert=True)
            return result.upserted_id if result.upserted_id else result.modified_count
        except Exception as e:
            self.logger.error(f"An error occurred while updating the document: {e}")
            return None

    def _find_one(self, collection: str, id_object: any):
        db = self.client[self.db_name]
        collection = db[collection]
        try:
            document = collection.find_one(id_object)
            return document
        except Exception as e:
            self.logger.error(f"An error occurred while retrieving the document: {e}")
            return None

    def _test_connection(self):
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
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Error during MongoDB connection test: {e}")
            return False

    @exception_handler
    async def connect(self):
        await self._run_blocking(self._connect)

    @exception_handler
    async def disconnect(self):
        await self._run_blocking(self._disconnect)

    @exception_handler
    async def upsert(self, collection: str, id_object: any, payload: any):
        return await self._run_blocking(self.upsert, collection, id_object, payload)

    @exception_handler
    async def find_one(self, collection: str, id_object: any):
        return await self._run_blocking(self.find_one, collection, id_object)

    @exception_handler
    async def test_connection(self):
        return await self._run_blocking(self._test_connection)
