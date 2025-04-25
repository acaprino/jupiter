import asyncio
import random
import threading

from collections import defaultdict
from typing import List
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, MenuButtonType
from aiogram.exceptions import TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command
from aiogram.types import BotCommand, BotCommandScopeChat, MenuButtonCommands, BotCommandScopeAllPrivateChats
from aiohttp import ClientConnectionError
from misc_utils.config import ConfigReader
from misc_utils.error_handler import exception_handler
from misc_utils.logger_mixing import LoggingMixin
from services.api_telegram import TelegramAPIManager


class TelegramService(LoggingMixin):
    _instances = {}
    _lock = threading.Lock()

    def __new__(cls, config: ConfigReader, token, *args, **kwargs):
        with cls._lock:
            if token not in cls._instances:
                cls._instances[token] = super(TelegramService, cls).__new__(cls)
            return cls._instances[token]

    def __init__(self, config: ConfigReader, token: str):
        if hasattr(self, '_initialized') and self._initialized:
            return
        super().__init__(config)
        self.agent = "TelegramService"
        self.config = config

        self.token = token
        self.bot = Bot(
            token=self.token,
            default=DefaultBotProperties(parse_mode=ParseMode.HTML)
        )
        self.dispatcher = Dispatcher()
        self.router = Router()
        self.dispatcher.include_router(self.router)
        self._initialized = True
        self._is_running = False

        # Initialize the global API manager
        self.api_manager = TelegramAPIManager(self.config)

        # Initialize commands storage
        self.global_commands = []  # Lista per i comandi globali
        self.chat_commands = defaultdict(list)  # Dizionario per comandi specifici per chat

    @exception_handler
    async def start(self):
        if self._is_running:
            self.info(f"Bot {self.agent} is already running.")
            return

        self._is_running = True
        self.info(f"Bot {self.agent} started.")

        # Initialize the global API manager if not already initialized
        await self.api_manager.initialize()

        # Start polling
        asyncio.create_task(self._polling())

    @exception_handler
    async def stop(self):
        if not self._is_running:
            self.info(f"Bot {self.agent} is not running.")
            return

        self._is_running = False

        await self.dispatcher.stop_polling()
        await self.bot.session.close()

        # Optionally, shutdown the global API manager if needed
        # await self.api_manager.shutdown()

        self.info(f"Bot {self.agent} stopped.")

    @exception_handler
    async def _polling(self):
        await asyncio.sleep(random.uniform(0.5, 2.0))  # Prevent simultaneous polling
        while self._is_running:
            try:
                await self.dispatcher.start_polling(self.bot)
            except TelegramRetryAfter as e:
                wait_time = e.retry_after
                self.warning(f"Rate limit exceeded. Retrying after {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            except TelegramServerError as e:
                self.error(f"Server error. Retrying in 5 seconds...", exc_info=e)
                await asyncio.sleep(5)
            except ClientConnectionError as e:
                self.error(f"Connection error. Retrying in 5 seconds...", exc_info=e)
                await asyncio.sleep(5)
            except Exception as e:
                self.critical(f"Unexpected error during polling", exc_info=e)
                break  # Exit the loop on unexpected exceptions
        self.info("Polling stopped.")

    @exception_handler
    async def send_message(self, chat_id, text, reply_markup=None):
        """
        Enqueues the send_message API call to the global API manager.
        """
        text_log = text.replace("\n", " \\n ")
        self.info(f"Sending message to chat {chat_id}: {text_log}")
        await self.api_manager.enqueue(
            self.bot.send_message,
            self.agent,
            chat_id,
            text,
            reply_markup=reply_markup
        )

    def add_command_handler(self, command: str, handler):
        self.router.message.register(handler, Command(commands=[command]))
        self.info(f"Added command handler for '{command}'.")

    def add_callback_query_handler(self, handler, filters=None):
        """
        Register a callback query handler with an optional filter.

        Args:
            handler: The function that will handle the callbacks
            filters: An optional filter to determine which callbacks to handle
        """
        if filters:
            self.router.callback_query.register(handler, filters)
        else:
            self.router.callback_query.register(handler)

        self.info("Added callback query handler with filters." if filters else "Added callback query handler.")

    @exception_handler
    async def reset_bot_commands(self, chat_id: str = None):
        """
        Resetta i comandi del bot.

        Args:
            chat_id (str, optional): ID della chat per cui resettare i comandi.
                                    Se None, resetta i comandi globali.
        """
        if chat_id is None:
            # Reset global commands
            self.global_commands = []
            await self._update_bot_commands()
            self.info("Reset global bot commands")
        else:
            # Reset commands for a specific chat
            if chat_id in self.chat_commands:
                self.chat_commands[chat_id] = []

                try:
                    # Clear commands for this specific chat
                    scope = BotCommandScopeChat(chat_id=chat_id)
                    await self.api_manager.enqueue(
                        self.bot.set_my_commands,
                        self.agent,
                        commands=[],
                        scope=scope
                    )
                    self.info(f"Reset bot commands for chat {chat_id}")
                except Exception as e:
                    self.error(f"Error resetting commands for chat {chat_id}: {str(e)}", exc_info=e)

    @exception_handler
    async def reset_all_chat_commands(self):
        """Resetta tutti i comandi specifici per chat mantenendo i comandi globali."""
        for chat_id in list(self.chat_commands.keys()):
            await self.reset_bot_commands(chat_id=chat_id)

        # Reinitialize the dictionary
        self.chat_commands = defaultdict(list)
        self.info("Reset all chat-specific commands")

    async def _update_bot_commands(self, chat_id: str = None):
        """
        Funzione dedicata all'aggiornamento dei comandi del bot.

        Args:
            chat_id (str, optional): ID della chat per cui aggiornare i comandi.
                                   Se None, aggiorna i comandi globali.
        """
        try:
            if chat_id is None:
                # Update global commands
                await self.api_manager.enqueue(
                    self.bot.set_my_commands,
                    self.agent,
                    commands=self.global_commands,  # Usa global_commands invece di chat_commands
                    scope=BotCommandScopeAllPrivateChats()
                )

                # Set command menu button
                await self.api_manager.enqueue(
                    self.bot.set_chat_menu_button,
                    self.agent,
                    menu_button=MenuButtonCommands(type=MenuButtonType.COMMANDS)
                )
                command_names = [cmd.command for cmd in self.global_commands]
                self.info(f"Updated global bot commands: {', '.join(command_names) if command_names else 'None'}")
            else:
                # Update commands for a specific chat
                if chat_id in self.chat_commands:
                    scope = BotCommandScopeChat(chat_id=chat_id)
                    await self.api_manager.enqueue(
                        self.bot.set_my_commands,
                        self.agent,
                        commands=self.chat_commands[chat_id],
                        scope=scope
                    )
                    command_names = [cmd.command for cmd in self.chat_commands[chat_id]]
                    self.info(f"Updated bot commands for chat {chat_id}: {', '.join(command_names) if command_names else 'None'}")
        except Exception as e:
            target = f"chat {chat_id}" if chat_id else "global scope"
            self.error(f"Error while updating commands for {target}: {str(e)}", exc_info=e)

    @exception_handler
    async def register_command(self, command: str, handler, description: str = "", chat_ids: List[str] = None):
        """
        Registra un comando bot con gestione degli errori migliorata e supporto per scope basato sui chat_ids.

        Args:
            command (str): Il comando da registrare (senza /)
            handler: La funzione di callback che gestirà il comando
            description (str): La descrizione del comando mostrata nel menu
            chat_ids (List[str], optional): Lista di chat_id dove il comando sarà visibile.
                                           Se None, il comando sarà globale.
        """
        try:
            # Register handler for the command (this works for all chats)
            self.router.message.register(handler, Command(commands=[command]))

            # Create new command
            new_command = BotCommand(command=command, description=description)

            # If no chat_ids specified, make it a global command
            if chat_ids is None:
                # Remove command from global list if it existed before
                self.global_commands = [
                    cmd for cmd in self.global_commands
                    if cmd.command != new_command.command
                ]
                self.global_commands.append(new_command)

                # Update bot commands globally
                await self._update_bot_commands()
                self.info(f"Command /{command} successfully registered globally")
            else:
                # For scoped commands
                for chat_id in chat_ids:
                    try:
                        # Update our local tracking of commands per chat
                        self.chat_commands[chat_id] = [
                            cmd for cmd in self.chat_commands[chat_id]
                            if cmd.command != new_command.command
                        ]
                        self.chat_commands[chat_id].append(new_command)

                        # Update commands for this specific chat
                        scope = BotCommandScopeChat(chat_id=chat_id)
                        await self.api_manager.enqueue(
                            self.bot.set_my_commands,
                            self.agent,
                            commands=self.chat_commands[chat_id],
                            scope=scope
                        )
                        self.info(f"Command /{command} successfully registered for chat {chat_id}")
                    except Exception as chat_e:
                        self.error(f"Error setting command /{command} for chat {chat_id}: {str(chat_e)}", exc_info=chat_e)

        except Exception as e:
            self.error(f"Error registering command /{command}: {str(e)}", exc_info=e)
            raise
