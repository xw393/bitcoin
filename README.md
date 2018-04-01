## Cryptocurrency Scraper

- crypto_scraper.py is used to download price from CoinMarketCap. Cryptos whose market cap are in top 200 will be downloaded.  

- .\btc_vol\btc_trading_vol_scraper.py is to download minute-level bitcoin trading volume data reported by 10 largest exchanges.  
    Bitcoin trading volume provides strong signals on its price changes. The website only provide last 24 hours' data, so this
    scraper could download data every 30 minutes and store them into database automatically.
