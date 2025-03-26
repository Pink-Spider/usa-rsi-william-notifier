import asyncio
import time
import pandas as pd
from yahooquery import Ticker

from logger.logger import logger
from message.telegram_message import send_telegram_message
from tech_indicator.indicator import calculate_rsi, calculate_williams_r, generate_signals
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# 주식 데이터 실시간 모니터링
async def monitor_stocks():
  tickers = ['AAPL', 'NVDA', 'MSFT', 'GOOG', 'AMZN',
             'TSLA', 'AVGO', 'LLY', 'WMT', 'JPM',
             'XOM', 'ORCL', 'NFLX', 'BAC']
  period = 14
  check_interval = 600  # 10분
  heartbeat_interval = 600  # 10분
  last_alert = {}

  logger.info("Trading bot with RSI and Williams %R started.")
  await send_telegram_message("Trading bot with RSI and Williams %R started.")

  while True:
    try:
      # 한 번에 모든 종목 가져오기
      tickers_obj = Ticker(tickers)
      df = tickers_obj.history(period='3mo', interval='1d')

      if df.empty:
        logger.warning("No data returned for any ticker.")
        await asyncio.sleep(check_interval)
        continue

      # 종목별로 데이터 분리
      for stock_ticker in tickers:
        try:
          stock_data = df[df.index.get_level_values(0) == stock_ticker].copy()

          if stock_data.empty:
            logger.warning(f"No data available for {stock_ticker}.")
            continue

          # 인덱스 정리
          stock_data.reset_index(inplace=True)
          stock_data.set_index('date', inplace=True)

          # 지표 계산
          stock_data['Williams %R'] = calculate_williams_r(stock_data, period)
          stock_data['RSI'] = calculate_rsi(stock_data, period)

          # 데이터 유효성 확인
          if stock_data[['Williams %R', 'RSI']].isna().all(axis=None):
            logger.warning(f"{stock_ticker}: Indicator data is not valid.")
            continue

          # 신호 생성
          buy_signals, sell_signals = generate_signals(
              stock_data['Williams %R'], stock_data['RSI']
          )

          latest_date = stock_data.index[-1]
          williams_r_value = stock_data.loc[latest_date, 'Williams %R']
          rsi_value = stock_data.loc[latest_date, 'RSI']
          close_price = stock_data.loc[latest_date, 'close']

          # 매수 알림
          if buy_signals.iloc[-1] and last_alert.get(stock_ticker) != 'buy':
            message = (
              f"[Buy Signal] {stock_ticker} - {latest_date.strftime('%Y-%m-%d')}:\n"
              f"Williams %R: {williams_r_value:.2f}, RSI: {rsi_value:.2f}\n"
              f"Price: ${close_price:.2f}"
            )
            await send_telegram_message(message)
            logger.info(message)
            last_alert[stock_ticker] = 'buy'

          # 매도 알림
          if sell_signals.iloc[-1] and last_alert.get(stock_ticker) != 'sell':
            message = (
              f"[Sell Signal] {stock_ticker} - {latest_date.strftime('%Y-%m-%d')}:\n"
              f"Williams %R: {williams_r_value:.2f}, RSI: {rsi_value:.2f}\n"
              f"Price: ${close_price:.2f}"
            )
            await send_telegram_message(message)
            logger.info(message)
            last_alert[stock_ticker] = 'sell'

        except Exception as e:
          logger.error(f"Error processing {stock_ticker}: {e}")

    except Exception as e:
      logger.error(f"Error fetching data: {e}")

    logger.info("Data check completed. Waiting for 10 minutes...")
    # Heartbeat 메시지 전송
    try:
      await send_telegram_message("✅ Heartbeat: Monitoring is running.")
      logger.info("Heartbeat message sent.")
    except Exception as e:
      logger.error(f"Failed to send heartbeat: {e}")

    await asyncio.sleep(check_interval)


# 비동기 루프 실행
if __name__ == '__main__':
  asyncio.run(monitor_stocks())
