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
   int port = 5555;
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

      string json_content = "{\n"
                            "    \"time_utc\": \"" + (string(time_utc)) + "\",\n"
                            "    \"time_server\": \"" + (string(time_server)) + "\",\n"
                            "    \"time_utc_unix\": " + IntegerToString((long)time_utc) + ",\n"
                            "    \"time_server_unix\": " + IntegerToString((long)time_server) + ",\n"
                            "    \"time_difference\": " + IntegerToString(difference) + "\n\n"
                            "}";

      router.sendMore(client_id);
      router.send(json_content);

      PrintFormat("Risposta inviata a %s: %s", client_id, json_content);
     }
  }
//+------------------------------------------------------------------+
