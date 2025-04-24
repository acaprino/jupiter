# Strategy 1 - Adrastea

| Versione                  | 1.1                                                        |
| ------------------------- | ---------------------------------------------------------- |
| Data ultimo aggiornamento | 09/03/2024                                                 |
| Autori                    | Gregorio Graziano, Benedetta Basile Gigante, Alfio Caprino |

---
## Descrizione della Strategia di Trading

La strategia di trading esaminata si basa su un'analisi sequenziale e condizionale dei dati di mercato, utilizzando un insieme di indicatori tecnici per determinare i punti ottimali di ingresso e uscita dal mercato. Gli indicatori chiave includono due versioni dell'indicatore SuperTrend (fast e slow) e l'oscillatore Stocastico (%K e %D). Inoltre, si applica la trasformazione delle candele OHLC (Open, High, Low, Close) in candele Heikin Ashi per una rappresentazione più fluida delle tendenze del mercato.
### Asset e Timeframe
- **Asset da negoziazione**: La strategia è versatile, applicabile a Forex, Indici, Materie prime, Criptovalute e Metalli preziosi (Bullion), escludendo la categoria 'Shares'.
- **Timeframe per il segnale di ingresso**: Ideale per l'attivazione dei segnali di ingresso sui timeframe di 30 minuti, 1 ora, 4 ore e 1 giorno, con quest'ultimo meno convenzionale per un sistema di trading automatico, ma comunque funzionale.

### Sequenza e Regressione delle Condizioni

La logica di trading segue un approccio sequenziale per la verifica delle condizioni, identificate come C1, C2, C3 e C4, con particolari requisiti di sequenza e persistenza:

- **Sequenza**: Le condizioni vengono valutate in ordine, partendo da C1 fino a C4. Le prime tre condizioni (C1-C3) devono essere soddisfatte in candele consecutive, mentre C4 può verificarsi nella stessa candela di C3.
- **Persistenza**: C1 e C3 devono rimanere valide durante l'intero processo di verifica per generare un segnale di ingresso. Se queste condizioni non sono più soddisfatte a causa di nuovi dati di mercato, si effettua una regressione al punto di verifica precedente. C2 e C4, invece, necessitano di essere soddisfatte solo una volta senza requisiti di persistenza.
### Indicatori e Calcoli

#### SuperTrend (ST)

L'indicatore SuperTrend è impiegato in due varianti, "slow" (che mira a identificare le tendenze di lungo periodo) e “fast” (che utilizza parametri differenti per una maggiore reattività), con parametri di periodo e moltiplicatore specifici. La formula generale dell'indicatore SuperTrend è: $\text{ST} = \frac{(\text{High} + \text{Low})}{2} + (M \times \text{ATR}(P))$ dove $\text{ATR}(P)$ rappresenta l'Average True Range su un periodo $P$, offrendo una misura della volatilità di mercato.

**Parametri**

| **Indicatore**  | **Nome Parametro** | **Valore Parametro** | 
| --------------- | ------------------ | -------------------- |
| SuperTrend Slow | periodo            | 40                   | 
| SuperTrend Slow | moltiplicatore     | 3                    | 

| **Indicatore**  | **Nome Parametro** | **Valore Parametro** |
| --------------- | ------------------ | -------------------- |
| SuperTrend Fast | periodo            | 10                   |
| SuperTrend Fast | moltiplicatore     | 1                    |
    
#### Oscillatore Stocastico
    
Utilizzato per determinare le condizioni di ipercomprato e ipervenduto, calcolando i valori %K e %D secondo le seguenti formule: $\%K = \left( \frac{C - L_n}{H_n - L_n} \right) \times 100$ $\%D = \text{SMA}(\%K, d)$ con $C$ come prezzo di chiusura corrente, $L_n$ come il prezzo minimo e $H_n$ come il prezzo massimo delle ultime $n$ sessioni.

**Parametri**

|**Nome Parametro**|**Valore Parametro**|
|---|---|
|k|24|
|d|5|
|smooth|3|

#### Candele Heikin Ashi
    
Le candele Heikin Ashi sono calcolate per fornire una visione più fluida delle tendenze del mercato.

**Formule** 

- $HA\_Open_i = \frac{HA\_Open_{i-1} + HA\_Close_{i-1}}{2}$
- $HA\_Close = \frac{Open + High + Low + Close}{4}$
- $HA\_High = \max(High, HA\_Open, HA\_Close)$
- $HA\_Open = \frac{HA\_Open_{prev} + HA\_Close_{prev}}{2}$

### Logica di Trading

1. **Verifica delle Condizioni per Ogni Nuova Candela**
    
    Ad ogni nuova candela, la strategia verifica che le condizioni di trading precedentemente stabilite rimangano valide con l'aggiornamento dei dati di mercato, garantendo una risposta coerente alle dinamiche correnti.
    
2. **Verifica Sequenziale e Dinamica delle Condizioni**
    
    Le condizioni sono esaminate in sequenza. Se le condizioni iniziali non sono più soddisfatte durante l'analisi, la strategia riprende dall'ultima condizione valida, assicurando un'interpretazione metodica e dinamica dei segnali di trading.
    
#### Logica di Trading Dettagliata

La strategia di trading è guidata da una serie di condizioni che devono essere soddisfatte per determinare un segnale di ingresso positivo.

1. **Condizione 1 (C1)**: Valutazione della tendenza attraverso il SuperTrend slow.
    - **Per posizioni Long**: $Close_{HA} \geq ST_{slow, prev}$
    - **Per posizioni Short**: $Close_{HA} < ST_{slow, prev}$
2. **Condizione 2 (C2)**: Conferma della tendenza con il SuperTrend fast.
    - **Per posizioni Long**: $Close_{HA} \leq ST_{fast, cur}$
    - **Per posizioni Short**: $Close_{HA} > ST_{fast, cur}$
3. **Condizione 3 (C3)**: Verifica dell'andamento con il SuperTrend fast della candela precedente.
    - **Per posizioni Long**: $Close_{HA} \geq ST_{fast, prev}$
    - **Per posizioni Short**: $Close_{HA} < ST_{fast, prev}$
4. **Condizione 4 (C4)**: Valutazione finale attraverso l'oscillatore Stocastico per stabilire il momento più favorevole per l'ingresso.
    - **Per posizioni Long**: $\%K > \%D$ e $\%D < 50$
    - **Per posizioni Short**: $\%K < \%D$ e $\%D > 50$

### Take Profit e Stop Loss

#### Stop Loss (SL)

- **Per posizioni Long**:  
  Lo **Stop Loss** per una posizione Long viene calcolato sottraendo una percentuale (*Delta*) del valore del **SuperTrend slow** al prezzo di ingresso.  

  **Formula**:  
  $SL_{Long} = Price_{entry} - (ST_{slow\_cur} \times \Delta)$
</br>
- **Per posizioni Short**:  
  Lo **Stop Loss** per una posizione Short viene calcolato aggiungendo una percentuale (*Delta*) del valore del **SuperTrend slow** al prezzo di ingresso.  

  **Formula**:  
  $SL_{Short} = Price_{entry} + (ST_{slow\_cur} \times \Delta)$

  
#### Take Profit (TP)

- **Per posizioni Long**:  
  Il **Take Profit** per una posizione Long viene calcolato aggiungendo 2 volte l'**ATR a 5 periodi** al prezzo di ingresso.  

  **Formula**:  
  $TP_{Long} = Price_{entry} + (2 \times ATR_{5})$
</br>
- **Per posizioni Short**:  
  Il **Take Profit** per una posizione Short viene calcolato sottraendo 2 volte l'**ATR a 2 periodi** dal prezzo di ingresso.  

  **Formula**:  
  $TP_{Short} = Price_{entry} - (2 \times ATR_{2})$
### Money Management

La gestione del capitale è un aspetto cruciale della strategia, che mira a ottimizzare il rendimento riducendo i rischi. Ecco i principi fondamentali del nostro approccio di money management:

- **Investimento per Posizione**: Non si deve allocare più del 20% del capitale totale in una singola posizione di trading. Questo limita l'esposizione del nostro portafoglio e ci permette di distribuire il rischio su più operazioni.
- **Rischio per Operazione**: Ogni operazione implica un rischio calcolato tra l'1% e il 2% del capitale totale. Questo approccio consente di assorbire serie di perdite senza compromettere significativamente il capitale di partenza.

Esempio:

- Capitale totale: 10.000 €
- Capitale investito per posizione: 2.000 € (20% del capitale totale)

# Technical Annex

## ⚙️ Logics and functions of the engine

### 🏗️ Architecture diagram
![[Senza nome 1.jpg]]

### 👋 Introduction
This section provides a detailed explanation of the architecture, functionality, and data flow of the bot engine, highlighting the components and their roles in the infrastructure. The goal is to deliver a clear and comprehensive guide for understanding, maintaining, or extending the system.

#### 🎯 System Objectives

*   Identify trading opportunities in real-time (Generators).
*   Allow user confirmation/rejection via Telegram (Managers).
*   Automate trading operations (Sentinels).
*   Notify users of updates on trading activities.

## 🧩 Core Components

Jupiter's architecture is based on distinct components that communicate via AMQP queues, coordinated by the Middleware.

### 💡 1. Generator

A **Generator** is responsible for analyzing real-time market data (using a specific strategy) and producing trading signals. Each Generator instance is typically associated with a specific trading configuration (symbol, timeframe, direction) and a Telegram bot for user interaction regarding the generated signals.

#### 🔑 Key Roles

*   Analyze market data according to the implemented strategy logic.
*   Generate initial **Opportunity Signals** when strategy conditions are met and send them to the **Middleware**.
*   Generate **Entry Signals** (when the final strategy conditions are met for a potentially confirmed opportunity) and send them directly to the corresponding **Sentinel**.
*   (Optional, via `EconomicEventsEmitterAgent`) Publish relevant **Economic Events** to the **Middleware**.

#### 📤 Messages Produced (Sent to Middleware/Sentinel)

*   **Registration Request:** Sent to the Middleware upon startup to announce its configuration and associated Telegram bot details.
*   **Opportunity Signal:** Sent to the Middleware when a potential trading operation is identified. Contains a `signal_id` for future reference.
*   **Entry Signal:** Sent directly to the corresponding Sentinel (via AMQP queue, bypassing Middleware confirmation logic) when the conditions to enter the market are met *after* the opportunity has been (potentially) confirmed. Contains the `signal_id` of the original opportunity.
*   **Economic Event:** (Optional) Sent to the Middleware.

### 🛡️ 2. Sentinel

A **Sentinel** acts on confirmed trading signals and market events, interacting with the broker to place and manage orders. Each Sentinel instance is associated with a specific configuration (including a unique *magic number* for its orders) and a Telegram bot for notifications related to order execution and position status.

#### 🔑 Key Roles

*   Receive **Signal Confirmations** (approved/rejected) from the **Middleware**.
*   Receive **Entry Signals** from the corresponding **Generator**.
*   Place buy/sell orders (typically BUY\_STOP/SELL\_STOP) with the **Broker** upon receiving an Entry Signal for a previously approved opportunity.
*   Monitor the status of open positions.
*   (Via `EconomicEventsManagerAgent`) Receive **Economic Events** from the **Middleware** and potentially close positions at risk.
*   Send notifications about order execution status (e.g., order placed, filled, closed by SL/TP, errors) to the **Middleware**.
*   Respond to user commands (e.g., `/list_open_positions`, `/emergency_close`) received via the **Middleware**.

#### 📤 Messages Produced (Sent to Middleware)

*   **Registration Request:** Sent to the Middleware upon startup.
*   **Execution/Status Notifications:** Sent to the Middleware to inform the user (e.g., order filled, position closed, placement error).
*   **Command Responses:** (Implicitly via User-Specific Notifications) Sent to the Middleware in response to commands like `/list_open_positions`.

### 🧠 3. Middleware

The **Middleware** is the core of the architecture, serving as the central communication hub. It coordinates interactions between Generators, Sentinels, Telegram Bots, and other services (like the MongoDB database for signal persistence). It dynamically manages subscriptions to AMQP exchanges and queues based on registered agents.

#### 🔑 Key Roles

*   **Agent Registration:** Maintains a registry of active **Generators** and **Sentinels**, mapping each instance (`routine_id`) to its configuration (symbol, timeframe, direction, instance, mode) and associated Telegram bot/users.
*   **Signal Flow Management:**
    *   Receives **Opportunity Signals** from Generators.
    *   Retrieves signal details from the database (MongoDB).
    *   Forwards the opportunity to the appropriate users via the correct **Telegram Bot**, presenting buttons for approval (`/approve`) or rejection (`/reject`).
    *   Receives the user's decision from Telegram.
    *   Saves the confirmed/rejected status of the signal in the database.
    *   Distributes **Signal Confirmations** (the updated status) to the relevant **Sentinels**.
*   **Notification Routing:** Receives notifications (execution status from Sentinels, market status, economic events, generic messages from agents) and forwards them to the correct **Telegram Bots** and users, handling both User-Specific (based on `routine_id`) and Broadcast (based on `instance_name`, `symbol`, etc.) notifications.
*   **Command Handling:** Receives commands from **Telegram Bots** (e.g., `/emergency_close`, `/list_open_positions`) and routes them to the appropriate **Sentinels**.
*   **Persistence Interaction:** Communicates with MongoDB (via `SignalPersistenceService`) to save, update, and retrieve the state of signals.

#### 📥 Messages Handled (Received from Agents/Bots)

*   **Registration Requests:** From Generators and Sentinels.
*   **Opportunity Signals:** From Generators.
*   **Signal Confirmations/Rejections:** (Implicitly via interactions with Telegram Bots).
*   **Execution/Status Notifications:** From Sentinels.
*   **User Commands:** From Telegram Bots.
*   **Broadcast Notifications:** From specialized Notifier agents.
*   **Economic Events:** From Generators (`EconomicEventsEmitterAgent`).

### 📱 4. Telegram Bot (User Interface)

Serves as the primary user interface for interacting with the Jupiter system. Each bot is associated with one or more specific configurations managed by Generator or Sentinel instances.

#### 🔑 Key Roles

*   Display notifications received from the **Middleware** to the configured users (`chat_ids`).
*   Present interactive messages (e.g., Opportunity Signals) with inline buttons for actions (e.g., Approve/Reject).
*   Receive textual commands (e.g., `/list_open_positions`, `/emergency_close`) from users and forward them to the **Middleware**.

#### ✨ Main Functions

*   Notify users about **trading signal opportunities**.
*   Receive **confirmation/rejection commands** for signals.
*   Notify users about the **status of order execution** (filled, closed, errors).
*   Notify users about **market events** (status changes, economic news).
*   Receive and process **user commands**.

### 🏗️ Infrastructure

#### 🐇 AMQP
AMQP is used for asynchronous messaging between components. Messages are organized through exchanges and queues with dynamic routing.

#### 🏦 Broker
All generators and sentinels share a single broker account to place orders. Communication with the broker is handled through sentinels.

#### 🌊 Data Flows

| Section                    | Flow Step                  | Purpose                                                                 | Exchange                   | Exchange Type | Routing Key                                   | Sender                                      | Receiver                               |
| :------------------------- | :------------------------- | :---------------------------------------------------------------------- | :----------------------------------- | :------------ | :---------------------------------------------------- | :----------------------------------------------------- | :---------------------------------------------------- |
| **Agent Registration**     | 1.1 Request                | Agent announces itself to Middleware with config and UI.                | `jupiter_system`        | Direct        | `middleware.registration`                             | Generator/Sentinel                            | Middleware                                            |
| **Agent Registration**     | 1.2 Confirmation (ACK)     | Middleware confirms registration to the agent.                          | `jupiter_system`        | Direct        | `system.registration_ack.{routine_id}`                | Middleware                                             | Agent                                                 |
| **Trading Signal Management** | 2.1 Opportunity            | Generator notifies a trading opportunity.                               | `jupiter_events`        | Topic         | `event.signal.opportunity.{symbol}.{tf}.{dir}`        | Generator                                         | Middleware                                            |
| **Trading Signal Management** | 2.2 Confirmation (from Middleware) | Middleware forwards user decision to the Sentinel.                      | `jupiter_events`        | Topic         | `event.signal.confirmation.{symbol}.{tf}.{dir}`       | Middleware                                             | Sentinel                                         |
| **Trading Signal Management** | 2.3 Entry                  | Generator instructs the Sentinel to enter (if confirmed).               | `jupiter_events`        | Topic         | `event.signal.enter.{symbol}.{tf}.{dir}`              | Generator                                         | Sentinel                                         |
| **User Notifications**     | 3.1 User-Specific          | Send message to users of a specific routine.                            | `jupiter_notifications` | Topic         | `notification.user.{routine_id}`                      | Generator/Sentinel                            | Middleware                                            |
| **User Notifications**     | 3.2 Broadcast              | Send message to all interested users (by instance/symbol).              | `jupiter_notifications` | Topic         | `notification.broadcast.{instance}.{symbol}[.*]`      | Generator/Sentinel                            | Middleware                                            |
| **User Commands**          | 4.1 Emergency Close        | Middleware forwards user request to close positions.                      | `jupiter_commands`      | Topic         | `command.emergency_close.{symbol}.{tf}.{dir}`         | Middleware                                             | Sentinel                                         |
| **User Commands**          | 4.2 List Positions         | Middleware forwards user request to list positions.                     | `jupiter_commands`      | Topic         | `command.list_open_positions.{routine_id}`            | Middleware                                             | Sentinel                                         |
| **Economic Events**        | 5.1 Event Emission         | Generator notifies an upcoming economic event.                          | `jupiter_events`        | Topic         | `event.economic`                                      | Generator                                         | Middleware                                            |

---

##### 📄 JSON Examples

###### 📄 JSON Example 1.1: Agent Registration - Request (Agent -> Middleware)

```json
{
  "sender": "SEN_EURUSD.M30.LONG",
  "recipient": "middleware",
  "meta_inf": {
    "agent_name": "SEN_EURUSD.M30.LONG",
    "routine_id": "19Eg8ia0",
    "mode": "SENTINEL",
    "bot_name": "prod",
    "instance_name": "EXECUTOR_ALFIO",
    "symbol": "EURUSD",
    "timeframe": "M30",
    "direction": "LONG",
    "ui_token": "7977947056:AAFG-LT1K90BYl6GEnOLfGtsiXfUI67VXmk",
    "ui_users": ["98954367", "839440351", "701602405"]
  },
  "payload": {
    "token": "7977947056:AAFG-LT1K90BYl6GEnOLfGtsiXfUI67VXmk",
    "chat_ids": ["98954367", "839440351", "701602405"],
    "routine_id": "19Eg8ia0",
    "mode": "SENTINEL"
  },
  "timestamp": 1744963185,
  "message_id": "NUGQM4H_OXsZDXZ7WgH4"
}
```

###### 📄 JSON Example 1.2: Agent Registration - Confirmation (Middleware -> Agent)

```json
{
  "sender": "middleware",
  "recipient": "SEN_EURUSD.M30.LONG",
  "meta_inf": {
    "agent_name": "SEN_EURUSD.M30.LONG",
    "routine_id": "19Eg8ia0",
    "mode": "SENTINEL",
    "bot_name": "prod",
    "instance_name": "EXECUTOR_ALFIO",
    "symbol": "EURUSD",
    "timeframe": "M30",
    "direction": "LONG",
    "ui_token": "7977947056:AAFG-LT1K90BYl6GEnOLfGtsiXfUI67VXmk",
    "ui_users": ["98954367", "839440351", "701602405"]
  },
  "payload": {
    "token": "7977947056:AAFG-LT1K90BYl6GEnOLfGtsiXfUI67VXmk",
    "chat_ids": ["98954367", "839440351", "701602405"],
    "routine_id": "19Eg8ia0",
    "mode": "SENTINEL",
    "success": true
  },
  "timestamp": 1744963188,
  "message_id": "CO40OGfljyLMwRrTg0ap"
}
```

###### 📄 JSON Example 2.1: Signal Management - Opportunity (Generator -> Middleware)
```json
{
  "sender": "GEN_EURUSD.H1.LONG_abcdef12",
  "recipient": "middleware",
  "meta_inf": {
    "agent_name": "GEN_EURUSD.H1.LONG_abcdef12",
    "routine_id": "abcdef12",
    "mode": "GENERATOR",
    "bot_name": "prod",
    "instance_name": "MAIN_GENERATOR",
    "symbol": "EURUSD",
    "timeframe": "H1",
    "direction": "LONG",
    "ui_token": null,
    "ui_users": null
  },
  "payload": {
    "signal_id": "sig_EURUSD_H1_L_1678887000"
  },
  "timestamp": 1678887000,
  "message_id": "sigOppGhj789"
}
```

###### 📄 JSON Example 2.2: Signal Management - Confirmation (Middleware -> Sentinel)

```json
{
  "sender": "middleware",
  "recipient": "SENTINEL_EURUSD.H1.LONG_opqrst99",
  "meta_inf": {
    "agent_name": "middleware",
    "routine_id": null,
    "mode": "MIDDLEWARE",
    "bot_name": "prod",
    "instance_name": "MAIN_MIDDLEWARE",
    "symbol": "EURUSD",
    "timeframe": "H1",
    "direction": "LONG",
    "ui_token": null,
    "ui_users": null
  },
  "payload": {
    "signal_id": "sig_EURUSD_H1_L_1678887000"
  },
  "timestamp": 1678887060,
  "message_id": "sigConfKlm123"
}
```

###### 📄 JSON Example 2.3: Signal Management - Entry (Generator -> Sentinel)

```json
{
  "sender": "GEN_EURUSD.H1.LONG_abcdef12",
  "recipient": "SENTINEL_EURUSD.H1.LONG_opqrst99",
  "meta_inf": {
    "agent_name": "GEN_EURUSD.H1.LONG_abcdef12",
    "routine_id": "abcdef12",
    "mode": "GENERATOR",
    "bot_name": "prod",
    "instance_name": "MAIN_GENERATOR",
    "symbol": "EURUSD",
    "timeframe": "H1",
    "direction": "LONG",
    "ui_token": null,
    "ui_users": null
  },
  "payload": {
    "signal_id": "sig_EURUSD_H1_L_1678887000"
  },
  "timestamp": 1678887120,
  "message_id": "sigEnterNop456"
}
```

###### 📄 JSON Example 3.1: User Notifications - User-Specific (Agent -> Middleware)

```json
{
  "sender": "SENTINEL_EURUSD.H1.LONG_opqrst99",
  "recipient": "middleware",
  "meta_inf": {
    "agent_name": "SENTINEL_EURUSD.H1.LONG_opqrst99",
    "routine_id": "opqrst99",
    "mode": "SENTINEL",
    "bot_name": "prod",
    "instance_name": "EXECUTOR_ALFIO",
    "symbol": "EURUSD",
    "timeframe": "H1",
    "direction": "LONG",
    "ui_token": null,
    "ui_users": null
  },
  "payload": {
    "message": "✅ Order placed successfully with ID 12345."
  },
  "timestamp": 1678887200,
  "message_id": "notifUserQrs789"
}
```

###### 📄 JSON Example 3.2: User Notifications - Broadcast (Market State) (Notifier -> Middleware)

```json
{
  "sender": "Market state notifier agent",
  "recipient": "middleware",
  "meta_inf": {
    "agent_name": "Market state notifier agent",
    "routine_id": null,
    "mode": null,
    "bot_name": "prod",
    "instance_name": "EXECUTOR_ALFIO",
    "symbol": "USDJPY",
    "timeframe": null,
    "direction": null,
    "ui_token": null,
    "ui_users": null
  },
  "payload": {
    "message": "⏰🟢 Market for USDJPY has just <b>opened</b> on broker. Resuming trading activities."
  },
  "timestamp": 1744963222,
  "message_id": "VsvzOv9Coeb3V47TlPXu"
}
```

###### 📄 JSON Example 4.1: User Commands - Emergency Close (Middleware -> Sentinel)

```json
{
  "sender": "middleware",
  "recipient": "SENTINEL_EURUSD.H1.LONG_opqrst99",
  "meta_inf": {
    "agent_name": "middleware",
    "routine_id": null,
    "mode": "MIDDLEWARE",
    "bot_name": "prod",
    "instance_name": "MAIN_MIDDLEWARE",
    "symbol": "EURUSD",
    "timeframe": "H1",
    "direction": "LONG",
    "ui_token": null,
    "ui_users": null
  },
  "payload": {},
  "timestamp": 1678887400,
  "message_id": "cmdCloseZab678"
}
```

###### 📄 JSON Example 4.2: User Commands - List Positions (Middleware -> Sentinel)

```json
{
  "sender": "middleware",
  "recipient": "SENTINEL_EURUSD.H1.LONG_opqrst99",
  "meta_inf": {
    "agent_name": "middleware",
    "routine_id": "opqrst99",
    "mode": "MIDDLEWARE",
    "bot_name": "prod",
    "instance_name": "MAIN_MIDDLEWARE",
    "symbol": "EURUSD",
    "timeframe": "H1",
    "direction": "LONG",
    "ui_token": "7977947056:AAFG-LT1K90BYl6GEnOLfGtsiXfUI67VXmk",
    "ui_users": null
  },
  "payload": {},
  "timestamp": 1678887500,
  "message_id": "cmdListCde901"
}
```

###### 📄 JSON Example 5.1: Economic Events - Emission (Generator -> Middleware -> Sentinel)

```json
{
  "sender": "Economic events manager agent",
  "recipient": "middleware",
  "meta_inf": {
    "agent_name": "Economic events manager agent",
    "routine_id": "gen_eco_event_id_1",
    "mode": "GENERATOR",
    "bot_name": "prod",
    "instance_name": "MAIN_GENERATOR",
    "symbol": null,
    "timeframe": null,
    "direction": null,
    "ui_token": null,
    "ui_users": null
  },
  "payload": {
    "event_id": "calendar_evt_42",
    "name": "US Non-Farm Payrolls",
    "country": "US",
    "description": "Monthly change in employment",
    "time": 1678890000,
    "importance": "HIGH",
    "source_url": "https://example.com/calendar/event/42",
    "is_holiday": false
  },
  "timestamp": 1678887600,
  "message_id": "ecoEvtFgh234"
}
```

## 🛠️ Environment configuration

### 🐇 AMQP

*   Download and install RabbitMQ from <https://www.rabbitmq.com/docs/download>
*   Start the "RabbitMQ Command Prompt (sbin dir)" command console
*   Execute the command "rabbitmq-plugins enable rabbitmq\_management"
*   Create a new user for remote access:
    *   rabbitmqctl add\_user myuser mypassword
    *   rabbitmqctl set\_permissions -p / myuser ".\*" ".\*" ".\*"
    *   rabbitmqctl set\_user\_tags myuser administrator
*   Restart the RabbitMQ service
*   Connect to [http://{host}:15672/](http://localhost:15672/) and log in with "myuser/mypassword"

**NB.** If an error related to cookies occurs, align the file in "%HOMEPATH%\\.erlang.cookie" with the one in "C:\\Windows\\System32\\config\\systemprofile\\.erlang.cookie"

### 🍃 MongoDB

TBD

### 🐍 Python bot

1.  **Download and install Meta Trader 5:**
    *   Download the latest version of Meta Trader 5 for Windows [here](https://download.mql5.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe).
    *   Run the installer and follow the on-screen instructions.
2.  **Enable Algo Trading in Meta Trader 5:**
    *   Open Meta Trader 5.
    *   Navigate to **Tools** → **Options** → **Expert Advisors**.
    *   Check the box for **Enable algorithmic trading**.
3.  **Install Python 3.11.7:**
    *   Download Python 3.11.7 for Windows [here](https://www.python.org/ftp/python/3.11.7/python-3.11.7-amd64.exe).
    *   During installation, ensure you add Python to the PATH environment variable.
4.  **Install Git:**
    *   Download Git for Windows [here](https://git-scm.com/download/win).
    *   Run the installer and complete the setup.
5.  **Clone the bot repository and install the virtual environment and its dependencies**

```bash
git clone https://github.com/acaprino/Jupiter.git
python.exe -m pip install --upgrade pip
cd Jupiter
pip install virtualenv
virtualenv venv
venv\Scripts\activate.bat
pip cache purge
pip install -r requirements.txt
```

### ✈️ Telegram

6.  Search for the Telegram bot you want to connect to through the bot.
7.  Click on START to connect with the bot.
8.  Send a message containing the text “@getidsbot”.
9.  Copy the value from the “id” field and paste it into the Telegram configuration section of the config.json file. Place it under the “chat\_ids” field, within a JSON array.


### 📈 MetaTrader5
1.  Copy the content of the mql-zmq folder into the MQL5 folder of the workspace (File > Open Data Folder), e.g., C:\\Users\\Administrator\\AppData\\Roaming\\MetaQuotes\\Terminal\\D0E8209F77C8CF37AD8BF550E51FF075\\MQL5
2.  Configure the settings as follows from Tools > Options:

### 🖥️ VPS

It is recommended to restart the VPS at least once a week, better once a day if the system allows. To do this, simply configure a restart task on Windows Server as follows:

1.  Open Task Scheduler and create a new task
2.  Set the trigger for daily execution at the desired time (e.g., at 00:15:00)
3.  As the action, set the execution of the command:
> 	shutdown.exe /r /t 0
---
1.  Open Task Scheduler and create a new task
2.  Set the trigger for daily execution at the desired time (e.g., at 00:14:50)
3.  As the action, set the execution of the command:
>	C:\\Users\\Administrator\\Desktop\\jupiter\\bin\\shutdown.bat
4.  Set "Start in" with the working directory "C:\\Users\\Administrator\\Desktop\\jupiter\\bin"
5.  In the "General" tab, select the option "Run whether user is logged on or not"
---
1.  Open Task Scheduler and create a new task
2.  Set the trigger for execution at Windows startup ("At startup")
3.  As the action, set the execution of the command:
>	C:\\Users\\Administrator\\Desktop\\jupiter\\bin\\startup.bat
4.  Set "Start in" with the working directory "C:\\Users\\Administrator\\Desktop\\jupiter\\bin"
5.  In the "General" tab, select the option "Run whether user is logged on or not"

This way, every time the VPS restarts, the bot will be started in the background (without a UI).