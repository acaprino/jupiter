import json
from typing import List, Optional, Dict, Any

from misc_utils.enums import Mode, Timeframe, TradingDirection
from misc_utils.utils_functions import to_serializable, string_to_enum


class MessageMetaInf:
    def __init__(self,
                 agent_name: Optional[str] = None,
                 routine_id: Optional[str] = None,
                 mode: Optional[Mode] = None,
                 bot_name: Optional[str] = None,
                 instance_name: Optional[str] = None,
                 symbol: Optional[str] = None,
                 timeframe: Optional[Timeframe] = None,
                 direction: Optional[TradingDirection] = None,
                 ui_token: Optional[str] = None,
                 ui_users: Optional[List[str]] = None):
        self.agent_name = agent_name
        self.routine_id = routine_id
        self.mode = mode
        self.bot_name = bot_name
        self.instance_name = instance_name
        self.symbol = symbol
        self.timeframe = timeframe
        self.direction = direction
        self.ui_token = ui_token
        self.ui_users = ui_users

    def get_agent_name(self) -> Optional[str]:
        return self.agent_name

    def get_routine_id(self) -> Optional[str]:
        return self.routine_id

    def get_mode(self) -> Optional[Mode]:
        return self.mode

    def get_bot_name(self) -> Optional[str]:
        return self.bot_name

    def get_instance_name(self) -> Optional[str]:
        return self.instance_name

    def get_symbol(self) -> Optional[str]:
        return self.symbol

    def get_timeframe(self) -> Optional[Timeframe]:
        return self.timeframe

    def get_direction(self) -> Optional[TradingDirection]:
        return self.direction

    def get_ui_token(self) -> Optional[str]:
        return self.ui_token

    def get_ui_users(self) -> Optional[List[str]]:
        return self.ui_users

    def serialize(self) -> Dict[str, Any]:
        """
        Converte l'istanza in un dizionario.
        Per gli enum, se il valore non è None, viene restituito il valore del membro (name);
        altrimenti, rimane None.
        """
        return {
            "agent_name": self.agent_name,
            "routine_id": self.routine_id,
            "mode": self.mode.name if self.mode is not None and hasattr(self.mode, "name") else None,
            "bot_name": self.bot_name,
            "instance_name": self.instance_name,
            "symbol": self.symbol,
            "timeframe": self.timeframe.name if self.timeframe is not None and hasattr(self.timeframe, "name") else None,
            "direction": self.direction.name if self.direction is not None and hasattr(self.direction, "name") else None,
            "ui_token": self.ui_token,
            "ui_users": self.ui_users
        }

    def to_json(self) -> str:
        """
        Serializza l'istanza in una stringa JSON, utilizzando il dizionario restituito da serialize.
        """
        return json.dumps(self.serialize(), default=lambda obj: to_serializable(obj))

    @staticmethod
    def from_json(data: Dict[str, Any]) -> "MessageMetaInf":
        """
        Crea un'istanza di MessageMetaInf a partire da un dizionario.
        Se i campi relativi agli enum sono presenti e non None, viene effettuata la conversione tramite string_to_enum;
        altrimenti si passa None.
        """
        return MessageMetaInf(
            agent_name=data.get("agent_name"),
            routine_id=data.get("routine_id"),
            mode=string_to_enum(Mode, data["mode"]) if data.get("mode") is not None else None,
            bot_name=data.get("bot_name"),
            instance_name=data.get("instance_name"),
            symbol=data.get("symbol"),
            timeframe=string_to_enum(Timeframe, data["timeframe"]) if data.get("timeframe") is not None else None,
            direction=string_to_enum(TradingDirection, data["direction"]) if data.get("direction") is not None else None,
            ui_token=data.get("ui_token"),
            ui_users=data.get("ui_users")
        )
