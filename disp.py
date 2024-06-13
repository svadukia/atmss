import asyncio
import threading
from dhanhq import marketfeed  # Ensure you have the dhanhq module installed
import json
import pandas as pd
import logging
from datetime import datetime
import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

ltp_dict = {}
subscribed_symbols = set()
latest_sums = {}
latest_sums_lock = threading.Lock()
current_date = datetime.now().strftime('%Y-%m-%d')

# Product and expiry details
pr = "NATURALGAS"
ex = "21 JUN"
c_pr = "CRUDEOIL"
c_ex = "14 JUN"
g_pr = "GOLD"
g_ex = "25 JUL"
s_pr = "SILVER"
s_ex = "26 JUN"

# Read access token from a file
def read_access_token(filepath):
    try:
        with open(filepath, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
        return None

# Custom method to handle the subscription process correctly
async def custom_subscribe_symbols(feed, feed_request_code, new_symbols):
    new_symbols_flattened = [tuple(symbol) for symbol in new_symbols if isinstance(symbol, tuple) and len(symbol) == 2]
    unique_symbols_set = set(feed.instruments)
    unique_symbols_set.update(new_symbols_flattened)
    feed.instruments = list(unique_symbols_set)

    symbols_to_subscribe = [symbol for symbol in new_symbols_flattened if symbol not in subscribed_symbols]

    if feed.ws and not feed.ws.closed and symbols_to_subscribe:
        subscription_packet = feed.create_subscription_packet(symbols_to_subscribe, feed_request_code)
        await feed.ws.send(subscription_packet)
        subscribed_symbols.update(symbols_to_subscribe)
        logging.info(f"Subscribed to new symbols: {symbols_to_subscribe}")

# Async function to handle connection events
async def on_connect(instance):
    logging.info("Connected to websocket")

# Async function to handle connection closures and attempt reconnection
async def on_close(instance):
    logging.warning("Connection has been closed. Attempting to reconnect...")
    await reconnect(instance)

# Function to handle reconnection logic
async def reconnect(instance):
    while True:
        try:
            await asyncio.sleep(5)  # Wait 5 seconds before attempting to reconnect
            await instance.connect()
            logging.info("Reconnected to websocket")
            break
        except Exception as e:
            logging.error(f"Reconnection attempt failed: {e}")
            continue

# Helper functions to get ATM strikes
def get_atm_ng(price):
    Atm_strike = round(price / 5) * 5
    atm1 = f'{pr} {ex} {Atm_strike} CALL'
    atm2 = f'{pr} {ex} {Atm_strike} PUT'
    logging.info(f"Generated ATM NG strikes: {atm1}, {atm2}")
    return [atm1, atm2]

def get_atm_crude(price):
    Atm_strike = round(price / 50) * 50
    atm1 = f'{c_pr} {c_ex} {Atm_strike} CALL'
    atm2 = f'{c_pr} {c_ex} {Atm_strike} PUT'
    logging.info(f"Generated ATM Crude strikes: {atm1}, {atm2}")
    return [atm1, atm2]

def get_atm_gold(price):
    Atm_strike = round(price / 1000) * 1000
    atm1 = f'{g_pr} {g_ex} {Atm_strike} CALL'
    atm2 = f'{g_pr} {g_ex} {Atm_strike} PUT'
    logging.info(f"Generated ATM Gold strikes: {atm1}, {atm2}")
    return [atm1, atm2]

def get_atm_silver(price):
    Atm_strike = round(price / 1000) * 1000
    atm1 = f'{s_pr} {s_ex} {Atm_strike} CALL'
    atm2 = f'{s_pr} {s_ex} {Atm_strike} PUT'
    logging.info(f"Generated ATM Silver strikes: {atm1}, {atm2}")
    return [atm1, atm2]

# Function to get token list from CSV
def get_token(strikes):
    try:
        url = 'https://raw.githubusercontent.com/svadukia/atmss/main/api-scrip-master.csv'

        sym_df = pd.read_csv(url, low_memory=False)
        
        filtered_df = sym_df[sym_df['SEM_CUSTOM_SYMBOL'].isin(strikes) & (sym_df['SEM_EXM_EXCH_ID'] == 'MCX')]
        tokens = filtered_df['SEM_SMST_SECURITY_ID'].tolist()
        ng_for = [(5, str(token)) for token in tokens]
        logging.info(f"Filtered tokens for strikes {strikes}: {ng_for}")

        mdf = filtered_df[['SEM_TRADING_SYMBOL','SEM_SMST_SECURITY_ID', 'SEM_STRIKE_PRICE', 'SEM_OPTION_TYPE']].rename(
            columns={'SEM_TRADING_SYMBOL':'Symbol','SEM_SMST_SECURITY_ID': 'Token', 'SEM_STRIKE_PRICE': 'Strike', 'SEM_OPTION_TYPE': 'type'})
        return ng_for, mdf
    except Exception as e:
        logging.error(f"Error in get_token: {e}")
        return [], pd.DataFrame()

# Function to sum LTPs of given symbols
def get_atm_sum(symbols, mdf, name):
    global latest_sums

    total_ltp = 0.0
    sums = {}
    if 'name' not in latest_sums:
            latest_sums['name'] = {}
    if name not in latest_sums['name']:
        latest_sums['name'][name] = {'strike': {}, 'total': 0.0, 'time': ''}

    for _, row in mdf.iterrows():
        token = row['Token']
        strike = row['Strike']
        option_type = row['type']

        if strike not in latest_sums['name'][name]['strike']:
            latest_sums['name'][name]['strike'][strike] = {}

        if token in ltp_dict:
            latest_sums['name'][name]['strike'][strike][option_type] = ltp_dict[token]
            total_ltp += ltp_dict[token]

    latest_sums['name'][name]['total'] = round(total_ltp, 2)
    latest_sums['name'][name]['time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    logging.info(f"Updated latest sums for {name}: {latest_sums['name'][name]}")

    return total_ltp

# Async function to handle incoming messages
async def on_message(instance, message):
    try:
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message

        message_type = data.get('type', 'Unknown')
        if message_type == 'Ticker Data':
            security_id = data.get('security_id')
            ltp = float(data.get('LTP', '0.0'))
            ltp_dict[security_id] = ltp

            if security_id == 428649:
                strikes = get_atm_ng(ltp)
                ng_sym, ng_df = get_token(strikes)
                await custom_subscribe_symbols(instance, marketfeed.Ticker, ng_sym)
                get_atm_sum(ng_sym, ng_df, pr)

            if security_id == 427034:
                strikes = get_atm_crude(ltp)
                cr_sym, cr_df = get_token(strikes)
                await custom_subscribe_symbols(instance, marketfeed.Ticker, cr_sym)
                get_atm_sum(cr_sym, cr_df, c_pr)

            if security_id == 426266:
                strikes = get_atm_gold(ltp)
                go_sym, go_df = get_token(strikes)
                await custom_subscribe_symbols(instance, marketfeed.Ticker, go_sym)
                get_atm_sum(go_sym, go_df, g_pr)
            if security_id == 258633:
                strikes = get_atm_silver(ltp)
                sl_sym, sl_df = get_token(strikes)
                await custom_subscribe_symbols(instance, marketfeed.Ticker, sl_sym)
                get_atm_sum(sl_sym, sl_df, s_pr)
    except Exception as e:
        logging.error(f"Error in on_message: {e}")

async def connect_to_feed(feed):
    """
    Attempt to connect to the feed and handle reconnections on disconnections.
    """
    attempt_count = 0
    while not shutdown_event.is_set():
        try:
            await feed.connect()
            logging.info("Connected to the WebSocket feed.")
            # Handle incoming messages
             # You might need to implement this based on your actual WebSocket library
            attempt_count = 0  # Reset attempts after a successful connection
        except Exception as e:
            logging.error(f"Connection failed: {e}")
            attempt_count += 1
            wait_time = min(30, 2 ** attempt_count)  # Exponential backoff capped at 30 seconds
            logging.info(f"Reconnecting in {wait_time} seconds...")
            await asyncio.sleep(wait_time)

# Main function to run the feed
async def main_feed():
    access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzIwNjg0NzU3LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMzAyNzcxNSJ9.IaPsFUjWY5JWG_S9KLv3KvZMgXtUwiEd4YstrTb3uy3W77qvkM5chIR0QcMuG2vTZ7aQ0n5cb6ASG_VC8gB08Q'
    if not access_token:
        logging.error("Failed to read access token. Exiting.")
        return

    client_id = '1103027715'
    instruments = [(5, '258633'), (5, '426266'), (5, '428649'),(5,'427034')]
    subscription_code = marketfeed.Ticker

    feed = marketfeed.DhanFeed(client_id, access_token, instruments, subscription_code,
                               on_connect=on_connect, on_message=on_message, on_close=on_close)
    await connect_to_feed(feed)

# Handle graceful shutdown
shutdown_event = threading.Event()

# Run feed in a separate thread
def run_feed_in_background(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main_feed())

# Start the WebSocket feed in a separate thread
feed_loop = asyncio.new_event_loop()
feed_thread = threading.Thread(target=run_feed_in_background, args=(feed_loop,))
feed_thread.start()

# Dash app setup
app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("Commodity ATM Data"),
    dash_table.DataTable(
        id='atm-table',
        columns=[
            {'name': 'Commodity', 'id': 'commodity'},
            {'name': 'Strike', 'id': 'strike'},
            {'name': 'CE LTP', 'id': 'ce_ltp'},
            {'name': 'PE LTP', 'id': 'pe_ltp'},
            {'name': 'Sum', 'id': 'sum'}
        ],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center'}
    ),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  # Update every second
        n_intervals=0
    )
])

@app.callback(
    Output('atm-table', 'data'),
    [Input('interval-component', 'n_intervals')]
)
def update_table(n):
    global latest_sums
    
    na = latest_sums
    print(na)
    table_data = []
    with latest_sums_lock:
        for commodity, details in latest_sums.get('name', {}).items():
            strikes = details.get('strike', {})
            total = details.get('total', 'N/A')
            for strike, values in strikes.items():
                ce_ltp = values.get('CE', 'N/A')
                pe_ltp = values.get('PE', 'N/A')
                table_data.append({
                    'commodity': commodity,
                    'strike': strike,
                    'ce_ltp': ce_ltp,
                    'pe_ltp': pe_ltp,
                    'sum': total
                })
    return table_data

if __name__ == '__main__':
    app.run_server(debug=True,host= '0.0.0.0', port=8058)
