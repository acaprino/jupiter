///+------------------------------------------------------------------+
//|                        Economic Calendar Service                 |
//|                                      Copyright 2024, CompanyName |
//|                                       http://www.companyname.net |
//+------------------------------------------------------------------+
#include <Zmq/Zmq.mqh>
#property service

// Definizione dei flag ZeroMQ
#define ZMQ_DONTWAIT 1
#define ZMQ_SNDMORE 2
#define ZMQ_ROUTER 6

//+------------------------------------------------------------------+
//| Service Entry Point                                              |
//+------------------------------------------------------------------+
void OnStart()
  {
// Inizializza il contesto ZeroMQ

   Context context();
   int port = 5557;

// Crea un socket ROUTER
   Print("Creazione del socket ROUTER...");
   Socket router(context, ZMQ_ROUTER);
   router.bind("tcp://127.0.0.1:" + port);

   PrintFormat("Servizio ZMQ Router avviato sulla porta %d", port);

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
      PrintFormat("Messaggio ricevuto da %s: %s", client_id, received);

      string params[];
      int count = StringSplit(received, ':', params);
      PrintFormat("Numero di parametri ricevuti: %d", count);

      // Estrai i dati dalla stringa
      string command = params[0];               // Comando: LIST_IDS o GET_EVENT
      string country_code = params[1];          // Codice del paese
      long start_unix = (long)StringToInteger(params[2]); // Data di inizio in formato UNIX
      long end_unix = (long)StringToInteger(params[3]);   // Data di fine in formato UNIX

      datetime start_datetime = (datetime)start_unix;
      datetime end_datetime = (datetime)end_unix;

      PrintFormat("Comando ricevuto: %s", command);
      PrintFormat("Codice paese ricevuto: %s", country_code);

      if(command == "LIST_IDS")
        {
         Print("Esecuzione del comando LIST_IDS...");


         // Richiesta per ottenere solo gli ID degli eventi

         string ids_json = GetEventIds(start_datetime, end_datetime, country_code) + "  ";


         router.sendMore(client_id);
         router.send(ids_json);
         PrintFormat("Lista ID inviata a %s: %s", client_id, ids_json);
        }
      else
         if(command == "GET_EVENT")
           {
            Print("Esecuzione del comando GET_EVENT...");

            // Richiesta per ottenere i dettagli di un singolo evento
            int event_id = StringToInteger(params[4]);
            string event_json = GetEventDetails(event_id, country_code, start_datetime, end_datetime) + "  ";
            PrintFormat("Lunghezza della stringa JSON: %d", StringLen(event_json));

            router.sendMore(client_id);
            router.send(event_json);
            PrintFormat("Dettagli evento inviati a %s: %s", client_id, event_json);
           }
         else
           {
            // Comando non valido
            PrintFormat("Comando non valido ricevuto: %s", command);
            router.sendMore(client_id);
            router.send("{\"error\": \"Invalid command\"}");
            Print("Errore: comando non valido");
           }

     }
  }

//+------------------------------------------------------------------+
//| Funzione per ottenere la lista degli ID degli eventi             |
//+------------------------------------------------------------------+
string GetEventIds(datetime start_time, datetime end_time, string country_code)
  {
   MqlCalendarCountry countries[];
   int countries_count = CalendarCountries(countries);

   bool country_found = false;
   int event_ids[];

   for(int i = 0; i < countries_count; i++)
     {
      if(countries[i].code != country_code)
         continue;

      country_found = true;

      MqlCalendarEvent events[];
      int event_count = CalendarEventByCountry(countries[i].code, events);

      if(event_count <= 0)
         continue;

      for(int j = 0; j < event_count; j++)
        {
         if(events[j].time_mode == CALENDAR_TIMEMODE_TENTATIVE || events[j].time_mode == CALENDAR_TIMEMODE_NOTIME)
            continue;

         MqlCalendarValue values[];
         int value_count = CalendarValueHistoryByEvent(events[j].id, values, start_time, end_time);

         if(value_count <= 0)
            continue;

         ArrayResize(event_ids, ArraySize(event_ids) + 1);
         event_ids[ArraySize(event_ids) - 1] = events[j].id;
        }
     }

   if(!country_found)
     {
      Print("Errore: il codice del paese specificato non è stato trovato: ", country_code);
      return "{\"error\": \"Country code not found\"}";
     }

   return SerializeEventIdsToJson(event_ids);
  }
//+------------------------------------------------------------------+
//| Function to obtain the details of a single event                 |
//+------------------------------------------------------------------+
string GetEventDetails(int event_id, string country_code, datetime start, datetime end)
  {
// Get the list of available countries in the calendar
   MqlCalendarCountry countries[];
   int countries_count = CalendarCountries(countries);

// Variable to store the found country instance
   MqlCalendarCountry found_country;

// Search for the country with the specified code
   bool country_found = false;  // Flag to verify if the country was found
   for(int i = 0; i < countries_count; i++)
     {
      if(countries[i].code == country_code)
        {
         found_country = countries[i]; // Store the country instance
         country_found = true;
         break; // Exit the loop once found
        }
     }

// If the country was not found, return an error
   if(!country_found)
      return "{\"error\": \"Country code not found\"}";

// Variable to store the event details
   MqlCalendarEvent event;

// Get the event details using CalendarEventById
   if(!CalendarEventById(event_id, event))
     {
      return "{\"error\": \"Event details not found\"}";
     }

// Array to store the event values
   MqlCalendarValue values[];
   int values_count = CalendarValueHistory(values, start, end, country_code);

// Check if values are retrieved successfully
   if(values_count <= 0)
     {
      return "{\"error\": \"Failed to retrieve event values\"}";
     }

// Variable to store the event value
   MqlCalendarValue value;
   bool value_found = false;

   ArrayPrint(values);
// Search for the value with the matching event_id
   for(int i = 0; i < ArraySize(values); i++)
     {
      PrintFormat("Check: %d == %d", event_id, values[i].event_id);
      if(values[i].event_id == event_id)
        {
         value = values[i];
         value_found = true;
         break;
        }
     }

// If the value was not found, return an error
   if(!value_found)
      return "{\"error\": \"Event value not found\"}";

// Create an instance of your custom Event structure or class
   Event myEvent;

// Populate the details of the event
   PopulateEvent(myEvent, found_country, event, value);

// Serialize the event to JSON format and return it
   return SerializeEventToJson(myEvent);
  }
//+------------------------------------------------------------------+
//| Serializza un array di ID degli eventi in formato JSON           |
//+------------------------------------------------------------------+
string SerializeEventIdsToJson(const int &event_ids[])
  {
   string jsonArray = "["; // Inizio array JSON
   for(int i = 0; i < ArraySize(event_ids); i++)
     {
      if(i > 0)
         jsonArray += ",";
      jsonArray += "\"" + IntegerToString(event_ids[i]) + "\"";
     }
   jsonArray += "]"; // Fine array JSON
   return jsonArray;
  }
//+------------------------------------------------------------------+
//| Event structure definition                                       |
//+------------------------------------------------------------------+
struct Event
  {
   int               country_id;
   string            country_name;
   string            country_code;
   string            country_currency;
   string            country_currency_symbol;
   string            country_url_name;
   int               event_id;
   int               event_type;
   int               event_sector;
   int               event_frequency;
   int               event_time_mode;
   int               event_unit;
   int               event_importance;
   int               event_multiplier;
   int               event_digits;
   string            event_source_url;
   string            event_code;
   string            event_name;
   datetime          event_time;
   int               event_period;
   int               event_revision;
   double            actual_value;
   double            prev_value;
   double            revised_prev_value;
   double            forecast_value;
   int               impact_type;
  };

//+------------------------------------------------------------------+
//| Popola una struttura Event con i dati                            |
//+------------------------------------------------------------------+
void PopulateEvent(Event &myEvent, MqlCalendarCountry &country, MqlCalendarEvent &event, MqlCalendarValue &value)
  {
   myEvent.country_id            = country.id;
   myEvent.country_name          = country.name;
   myEvent.country_code          = country.code;
   myEvent.country_currency      = country.currency;
   myEvent.country_currency_symbol = country.currency_symbol;
   myEvent.country_url_name      = country.url_name;
   myEvent.event_id              = event.id;
   myEvent.event_type            = event.type;
   myEvent.event_sector          = event.sector;
   myEvent.event_frequency       = event.frequency;
   myEvent.event_time_mode       = event.time_mode;
   myEvent.event_unit            = event.unit;
   myEvent.event_importance      = event.importance;
   myEvent.event_multiplier      = event.multiplier;
   myEvent.event_digits          = event.digits;
   myEvent.event_source_url      = event.source_url;
   myEvent.event_code            = event.event_code;
   myEvent.event_name            = event.name;
   myEvent.event_time            = value.time;
   myEvent.event_period          = value.period;
   myEvent.event_revision        = value.revision;
   myEvent.actual_value          = value.actual_value;
   myEvent.prev_value            = value.prev_value;
   myEvent.revised_prev_value    = value.revised_prev_value;
   myEvent.forecast_value        = value.forecast_value;
   myEvent.impact_type           = value.impact_type;
  }

//+------------------------------------------------------------------+
//| Serializza un evento in formato JSON                             |
//+------------------------------------------------------------------+
string SerializeEventToJson(const Event &e)
  {
   string json = "{";
   json += "\"country_code\":\"" + EscapeDoubleQuotes(e.country_code) + "\",";
   json += "\"event_id\":" + IntegerToString(e.event_id) + ",";
   json += "\"event_type\":" + IntegerToString(e.event_type) + ",";
   json += "\"event_importance\":" + IntegerToString(e.event_importance) + ",";
   json += "\"event_source_url\":\"" + EscapeDoubleQuotes(e.event_source_url) + "\",";
   json += "\"event_code\":\"" + EscapeDoubleQuotes(e.event_code) + "\",";
   json += "\"event_name\":\"" + EscapeDoubleQuotes(e.event_name) + "\",";
   json += "\"event_time\":\"" + TimeToString(e.event_time, TIME_DATE | TIME_MINUTES) + "\"";
   json += "}";

   return json;
  }


//+------------------------------------------------------------------+
//| Utility to escape double quotes for JSON compatibility           |
//+------------------------------------------------------------------+
string EscapeDoubleQuotes(string text)
  {
   StringReplace(text, "\"", "\\\"");
   return text;
  }

//+------------------------------------------------------------------+
