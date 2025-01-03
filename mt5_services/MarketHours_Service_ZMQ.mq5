//+------------------------------------------------------------------+
//|                                                      ProjectName |
//|                                      Copyright 2020, CompanyName |
//|                                       http://www.companyname.net |
//+------------------------------------------------------------------+
#include <Zmq/Zmq.mqh>
#property service

// Definizione dei flag ZeroMQ
#define ZMQ_DONTWAIT 1
#define ZMQ_SNDMORE 2
#define ZMQ_ROUTER 6

//+------------------------------------------------------------------+
//|                                                                  |
//+------------------------------------------------------------------+
void OnStart()
  {
// Inizializza il contesto ZeroMQ
   Context context();
   int port = 5556;
// Crea un socket ROUTER
   Socket router(context, ZMQ_ROUTER);
   router.bind("tcp://127.0.0.1:" + port);

   Print("Servizio ZMQ Router avviato sulla porta " + port);

   while(true)
     {
      ZmqMsg identity;    // Identità del client
      ZmqMsg request;     // Messaggio di richiesta

      // Riceve l'identità del client
      if(!router.recv(identity, ZMQ_DONTWAIT))
        {
         // Nessuna richiesta ricevuta, pausa per evitare uso eccessivo della CPU
         Sleep(100);
         continue;
        }

      string client_id = identity.getData();
      PrintFormat("Richiesta ricevuta da %s", client_id);

      // Riceve il messaggio
      if(!router.recv(request))
        {
         Print("Errore nella ricezione del messaggio");
         continue;
        }


      string received = request.getData();
      PrintFormat("Richiesta ricevuta da %s: %s", client_id, received);

      MqlDateTime dt_utc= {};
      MqlDateTime dt_server= {};
      datetime    time_utc=TimeGMT(dt_utc);
      datetime    time_server =TimeTradeServer(dt_server);
      int         difference  = int((time_server-time_utc) / 3600.0);

      string json_content = GetSymbolSessionData(received);

      router.sendMore(client_id);
      router.send(json_content);

      PrintFormat("Risposta inviata a %s: %s", client_id, json_content);
     }
  }
//+------------------------------------------------------------------+
//+------------------------------------------------------------------+
//| Get trading session data for a single symbol in JSON format      |
//+------------------------------------------------------------------+
string GetSymbolSessionData(string symbol)
  {
   int SESSION_INDEX = 0;
   string json = "    {\n";
   json += "      \"symbol\": \"" + symbol + "\",\n";
   json += "      \"sessions\": [\n";
   for(int i = MONDAY; i <= FRIDAY; i++)
     {
      datetime date_from, date_to;
      if(!SymbolInfoSessionTrade(symbol, (ENUM_DAY_OF_WEEK)i, SESSION_INDEX, date_from, date_to))
        {
         Print("SymbolInfoSessionTrade() failed for symbol ", symbol, ". Error ", GetLastError());
         continue;
        }

      // Convert ENUM_DAY_OF_WEEK to readable string
      string week_day = DayOfWeekToString((ENUM_DAY_OF_WEEK)i);

      json += "        {\n";
      json += "          \"day\": \"" + week_day + "\",\n";
      json += "          \"start_time\": \"" +  TimeToString(date_from, TIME_MINUTES)  + "\",\n";
      json += "          \"end_time\": \"" +  TimeToString(date_to, TIME_MINUTES)  + "\"\n";
      json += "        }";

      if(i < FRIDAY)
         json += ",\n";
      else
         json += "\n";
     }

   json += "      ]\n";
   json += "    }";
   return json;
  }

//+------------------------------------------------------------------+
//| Convert ENUM_DAY_OF_WEEK to human-readable weekday string         |
//+------------------------------------------------------------------+
string DayOfWeekToString(ENUM_DAY_OF_WEEK day_of_week)
  {
   switch(day_of_week)
     {
      case MONDAY:
         return "Monday";
      case TUESDAY:
         return "Tuesday";
      case WEDNESDAY:
         return "Wednesday";
      case THURSDAY:
         return "Thursday";
      case FRIDAY:
         return "Friday";
      case SATURDAY:
         return "Saturday";
      case SUNDAY:
         return "Sunday";
      default:
         return "Unknown";  // Should not occur for valid input
     }
  }
//+------------------------------------------------------------------+
