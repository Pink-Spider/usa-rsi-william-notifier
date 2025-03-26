from yahooquery import Ticker
import pandas as pd
from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# 윌리엄 %R 계산 함수
def calculate_williams_r(data, period=14):
  high = data['high'].rolling(window=period).max()
  low = data['low'].rolling(window=period).min()
  close = data['close']
  williams_r = -100 * ((high - close) / (high - low))
  return williams_r

# RSI 계산 함수
def calculate_rsi(data, period=14):
  delta = data['close'].diff()
  gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
  loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
  rs = gain / loss
  rsi = 100 - (100 / (1 + rs))
  return rsi

def backtest_strategy(tickers, start_date, end_date, initial_cash=1000, buy_threshold=-80, sell_threshold=-20):
  results = []
  year_returns = {}
  total_initial_cash = len(tickers) * initial_cash
  total_final_value = 0

  tickers_data = Ticker(tickers)

  for ticker in tickers:
    print(f"Processing {ticker}...")
    try:
      df = tickers_data.history(start=start_date, end=end_date, interval='1d')
    except Exception as e:
      print(f"Error downloading {ticker}: {e}")
      continue

    if isinstance(df, pd.DataFrame):
      df = df[df.index.get_level_values(0) == ticker].copy()
    else:
      print(f"No data for {ticker}. Skipping...")
      continue

    if df.empty:
      print(f"No data for {ticker}. Skipping...")
      continue

    df.reset_index(inplace=True)
    df.set_index('date', inplace=True)

    df['Williams %R'] = calculate_williams_r(df)
    df['RSI'] = calculate_rsi(df)

    buy_signals = (df['Williams %R'] < buy_threshold) & (df['RSI'] < 40)
    sell_signals = (df['Williams %R'] > sell_threshold) & (df['RSI'] > 70)

    cash = initial_cash
    position = 0
    year_initial_balance = {}
    year_final_balance = {}

    for i in range(len(df)):
      current_year = df.index[i].year
      close_price = df['close'].iloc[i].item()

      if current_year not in year_initial_balance:
        year_initial_balance[current_year] = cash + (position * close_price if position > 0 else 0)

      if buy_signals.iloc[i] and cash > 0:
        position = cash / close_price
        cash = 0
        print(f"{df.index[i].strftime('%Y-%m-%d')} BUY {ticker} at {close_price:.2f}")

      if sell_signals.iloc[i] and position > 0:
        cash = position * close_price
        position = 0
        print(f"{df.index[i].strftime('%Y-%m-%d')} SELL {ticker} at {close_price:.2f}")

      year_final_balance[current_year] = cash + (position * close_price if position > 0 else 0)

    final_value = cash + (position * df['close'].iloc[-1].item() if position > 0 else 0)
    profit = final_value - initial_cash
    total_final_value += final_value

    results.append({
      'Ticker': ticker,
      'Initial Cash': initial_cash,
      'Final Value': final_value,
      'Profit': profit,
      'Profit (%)': (profit / initial_cash) * 100
    })

    for year in year_initial_balance:
      year_profit = year_final_balance[year] - year_initial_balance[year]
      year_return = (year_profit / year_initial_balance[year]) * 100
      if year not in year_returns:
        year_returns[year] = []
      year_returns[year].append(year_return)

  results_df = pd.DataFrame(results)
  total_profit = total_final_value - total_initial_cash
  total_return_rate = (total_profit / total_initial_cash) * 100

  start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
  end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
  investment_period_years = (end_date_dt - start_date_dt).days / 365.25
  annualized_return = ((1 + total_return_rate / 100) ** (1 / investment_period_years) - 1) * 100

  year_avg_returns = {year: sum(returns) / len(returns) for year, returns in year_returns.items()}

  print("\n=== Overall Performance ===")
  print(f"Total Initial Cash: {total_initial_cash:_} USD")
  print(f"Total Final Value: {total_final_value:_} USD")
  print(f"Total Profit: {total_profit:_} USD")
  print(f"Total Return Rate: {total_return_rate:.2f}%")
  print(f"Annualized Return: {annualized_return:.2f}%")
  print("\n=== Yearly Returns ===")
  for year, avg_return in year_avg_returns.items():
    print(f"{year}: {avg_return:.2f}%")

  return results_df, total_profit, total_return_rate, annualized_return, year_avg_returns


# 실행
tickers = ['AAPL', 'NVDA', 'MSFT', 'GOOG', 'AMZN',
           'TSLA', 'AVGO', 'LLY', 'WMT', 'JPM',
           'XOM', 'ORCL', 'NFLX', 'BAC']

start_date = "2023-01-01"
end_date = "2025-01-05"
initial_cash = 1000

results, total_profit, total_return_rate, annualized_return, year_avg_returns = backtest_strategy(
    tickers, start_date, end_date, initial_cash)

print("\n=== Backtest Results ===")
print(results)

print("\n=== Final Summary ===")
print(f"Total Profit: {total_profit:.2f} USD")
print(f"Overall Return: {total_return_rate:.2f}%")
print(f"Annualized Return: {annualized_return:.2f}%")
