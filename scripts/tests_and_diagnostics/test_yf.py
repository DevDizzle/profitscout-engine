import yfinance as yf
print(yf.download("SPY", period="1mo", progress=False))
