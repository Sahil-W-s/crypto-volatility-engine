import requests
from datetime import datetime, timezone
from supabase import create_client, Client

# Hardcoded for local testing ONLY.
SUPABASE_URL = "SUPABASE_URL"
SUPABASE_KEY = "SUPABASE_KEY"

# Initialize connection
print("Attempting to connect to Supabase...")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Connection established.")


def fetch_and_store_crypto_data(symbol="ETHUSDT", interval="1h"):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": 2}

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    last_closed_candle = data[0]
    timestamp_ms = last_closed_candle[0]
    candle_time = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc).isoformat()

    open_price = float(last_closed_candle[1])
    high_price = float(last_closed_candle[2])
    low_price = float(last_closed_candle[3])
    close_price = float(last_closed_candle[4])
    volume = float(last_closed_candle[5])

    try:
        supabase.table("crypto_ohlcv").insert({
            "symbol": symbol,
            "candle_timestamp": candle_time,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "volume": volume
        }).execute()

        print(f"SUCCESS: Logged {symbol} candle for {candle_time} | Close: ${close_price}")
    except Exception as e:
        print(f"Database Insert Error (Likely duplicate or table missing): {e}")


if __name__ == "__main__":
    symbols = ["ETHUSDT", "BTCUSDT"]
    for sym in symbols:
        fetch_and_store_crypto_data(symbol=sym)