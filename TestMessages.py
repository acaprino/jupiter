import asyncio
import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor

from apscheduler.util import undefined
from win32timezone import utcnow

from agents.agent_market_state_notifier import MarketStateNotifierAgent
from brokers.broker_proxy import Broker
from brokers.mt5_broker import MT5Broker
from dto.QueueMessage import QueueMessage
from misc_utils.config import ConfigReader, TelegramConfiguration, TradingConfiguration
from misc_utils.enums import RabbitExchange, Timeframe, TradingDirection
from misc_utils.utils_functions import to_serializable
from services.service_rabbitmq import RabbitMQService

config_file = '.\\configs\\middleware.json'

config = ConfigReader.load_config(config_file_param=config_file)

telegram_config_1 = TelegramConfiguration(
    token="7849409736:AAFRAiS4g7e7hCRsuSeyiHwNEajLKv07qhA",
    chat_ids=['98954367']
)
telegram_config_2 = TelegramConfiguration(
    token="7937879836:AAF4Tdn10cnOiS7d0Kh78OKcXUJEcg9vyKk",
    chat_ids=['98954367']
)
agent = "TEST_AGENT"
id = 'xyz0123'
risk_percent = 0.1

trading_configuration_1 = TradingConfiguration(
    bot_name="TEST",
    agent=agent,
    symbol='EURUSD',
    timeframe=Timeframe.H1,
    trading_direction=TradingDirection.LONG,
    risk_percent=risk_percent,
    telegram_config=telegram_config_1
)

trading_configuration_2 = TradingConfiguration(
    bot_name="TEST",
    agent=agent,
    symbol='EURUSD',
    timeframe=Timeframe.M30,
    trading_direction=TradingDirection.LONG,
    risk_percent=risk_percent,
    telegram_config=telegram_config_1
)
trading_configuration_3 = TradingConfiguration(
    bot_name="TEST",
    agent=agent,
    symbol='EURUSD',
    timeframe=Timeframe.H1,
    trading_direction=TradingDirection.SHORT,
    risk_percent=risk_percent,
    telegram_config=telegram_config_1
)


async def test_market_status_change_notification():
    market_state_notifier = MarketStateNotifierAgent(config=config, trading_configs=[trading_configuration_1, trading_configuration_2, trading_configuration_3])
    await market_state_notifier.routine_start()
    close_time = undefined
    open_time = utcnow().timestamp()
    # await market_state_notifier.on_market_status_change(symbol='EURUSD', is_open=True, closing_time=close_time, opening_time=open_time, initializing=True)
    await market_state_notifier.routine_stop()


def log_rabbit_message(exchange: str,
                       routing_key: str,
                       sender: str,
                       recipient: str,
                       trading_configuration: any,
                       payload: any,
                       message_id: str,
                       direction: str):
    print(
        f"exchange={exchange}, routing_key={routing_key}, sender={sender}, recipient={recipient}, "
        f"trading_configuration={trading_configuration}, payload={payload}, message_id={message_id}, direction={direction}"
    )


async def start():
    await init()
    await test_registration()
    await test_market_status_change_notification()


async def init():
    RabbitMQService(
        config=config,
        user=config.get_rabbitmq_username(),
        password=config.get_rabbitmq_password(),
        rabbitmq_host=config.get_rabbitmq_host(),
        port=config.get_rabbitmq_port(),
        loop=loop
    )
    RabbitMQService.register_hook(log_rabbit_message)

    await RabbitMQService.start()

    await Broker().initialize(
        MT5Broker,
        config=config,
        connection={
            'account': config.get_broker_account(),
            'password': config.get_broker_password(),
            'server': config.get_broker_server(),
            'path': config.get_broker_mt5_path()
        }
    )
    await Broker().startup()


async def test_registration():
    # Creiamo un Future che attenderà il callback
    registration_future = asyncio.Future()

    async def on_client_registration_ack(routing_key: str, message: QueueMessage):
        print(f"Client with id {id} successfully registered, calling registration callback.")
        # Segnaliamo il completamento del Future
        registration_future.set_result(True)

    await RabbitMQService.register_listener(
        exchange_name=RabbitExchange.REGISTRATION_ACK.name,
        callback=on_client_registration_ack,
        routing_key=id,
        exchange_type=RabbitExchange.REGISTRATION_ACK.exchange_type
    )

    registration_payload = to_serializable(telegram_config_1)
    registration_payload["routine_id"] = id

    client_registration_message = QueueMessage(
        sender="TEST_AGENT",
        payload=registration_payload,
        recipient="middleware",
        trading_configuration=to_serializable(trading_configuration_1)
    )

    await RabbitMQService.publish_message(
        exchange_name=RabbitExchange.REGISTRATION.name,
        exchange_type=RabbitExchange.REGISTRATION.exchange_type,
        routing_key=RabbitExchange.REGISTRATION.routing_key,
        message=client_registration_message
    )

    # Attende il completamento del Future (ossia il callback)
    await registration_future
    print("Registration callback received, continuing execution...")


loop = asyncio.get_event_loop()
executor = ThreadPoolExecutor(max_workers=100)
loop.set_default_executor(executor)

if __name__ == "__main__":
    try:
        loop.run_until_complete(start())
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
