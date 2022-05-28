from binance import Client, ThreadedWebsocketManager, ThreadedDepthCacheManager
from time import time, sleep
import sqlite3
import os
from dotenv import load_dotenv



def closePosition(client, symbol, ammount):
    if ammount < 0:
        ammount = -1 * ammount
        client.futures_create_order(symbol=symbol,type="MARKET",side="BUY",quantity=ammount)
    else:
        client.futures_create_order(symbol=symbol,type="MARKET",side="SELL",quantity=ammount)
        
load_dotenv()


binance_api_key = os.getenv('BINANCE_API_KEY')
binance_api_secret = os.getenv('BINANCE_API_SECRET')

client = Client(binance_api_key, binance_api_secret)
info = client.futures_exchange_info()

con = sqlite3.connect(os.getenv('SQL_LITE_NAME'))

# create a table inside the database
cur = con.cursor()

expected_profit_percent = float(os.getenv('EXPECTED_PROFITS_PERCENT'))

cur.execute('''  CREATE TABLE IF NOT EXISTS pairs (long text, short text, position_size float, long_price float, short_price float, status text, profit text )''')
operation = input("Do you want to add a new pair or just check the status of existing pairs and see if there is any pair that needs to be closed (a/q): ")

if(operation == 'a'):
    amount = input('enter the amount for each position in $: ')
    long_position = input('enter the long position symbol: ')
    short_position = input('enter the short position symbol: ')
    
    # create a long position on futures account using binance sdk
    # get the long price and the short price
    
    long_price = client.futures_mark_price(symbol=long_position)['markPrice']
    short_price = client.futures_mark_price(symbol=short_position)['markPrice']
    long_position_info = None
    short_position_info = None
    
    for i in range(0, len(info['symbols'])):
        if(info['symbols'][i]['symbol'] == long_position):
            long_position_info = info['symbols'][i]
            break
        
    for i in range(0, len(info['symbols'])):
        if(info['symbols'][i]['symbol'] == short_position):
            short_position_info = info['symbols'][i]
            break
    
        
    long_amount = float(amount) / float(long_price)
    short_amount = float(amount) / float(short_price)
    
    # round the amount for long and short as pricePrecision in position info
    long_amount = round(long_amount, long_position_info['pricePrecision'])
    short_amount = round(short_amount, short_position_info['pricePrecision'])
    
    print('long price: ', long_price)
    print('short price: ', short_price)
    
    print('long amount: ', long_amount)
    print('short amount: ', short_amount)
    
    
    long_position_response = client.futures_create_order(symbol=long_position, type='MARKET', side='BUY', quantity=long_amount)
    short_position_response = client.futures_create_order(symbol=short_position, type='MARKET', side='SELL', quantity=short_amount)
    
    print('long position response: ', long_position_response)
    print('short position response: ', short_position_response)
    
    # save the pair to database
    cur.execute('''INSERT INTO pairs (long, short, position_size, long_price, short_price, status, profit) VALUES (?, ?, ?, ?, ?, ?, ?)''', (long_position, short_position, amount, long_price, short_price, 'open', '0'))
    print('position created')
    con.commit()

    

while(True):
    try:
        # get all pairs from database where status is open
        cur.execute('''SELECT * FROM pairs WHERE status = ?''', ('open',))
        future_account = client.futures_account()
        
        
        # loop over all pairs from database
        for row in cur.fetchall():
            # display start of a new pair

            unrealized_profits_for_long_position = None
            long_position_amount = None
            unrealized_profits_for_short_position = None
            short_position_amount = None

            # maybe bad choice but I rather use the CPU than send a request to the api.
            for position in future_account['positions']:
                if(position['symbol'] == row[0]):
                    unrealized_profits_for_long_position = position['unrealizedProfit']
                    long_position_amount = position['positionAmt']
                    break
            
            for position in future_account['positions']:
                if(position['symbol'] == row[1]):
                    unrealized_profits_for_short_position = position['unrealizedProfit']
                    short_position_amount = position['positionAmt']
                    break
            
            # now lets convert the unrealized profits into a float
            # then check if the results is greater or equal to the expected profit specefied in the configs
            unrealized_profits_for_long_position = float(unrealized_profits_for_long_position)
            unrealized_profits_for_short_position = float(unrealized_profits_for_short_position)

            # then add them together 
            unrealized_profits = unrealized_profits_for_long_position + unrealized_profits_for_short_position
            
            # display the pair long and short position
            print('long position: ', row[0], ' amount: ', long_position_amount, ' unrealized profit: ', unrealized_profits_for_long_position)
            print('short position: ', row[1], ' amount: ', short_position_amount, ' unrealized profit: ', unrealized_profits_for_short_position)
        
            # devide them by the amount of the position
            unrealized_profits = unrealized_profits / float(row[2])
            
            # check if the result is greater or equal to the expected profit
            if(unrealized_profits >= expected_profit_percent):
                # close long and short positions and update the status in the database
                closePosition(client, row[0], long_position_amount)
                closePosition(client, row[1], short_position_amount)
                cur.execute('''UPDATE pairs SET status = ?, profit = ? WHERE long = ? AND short = ?''', ('closed', unrealized_profits, row[0], row[1]))
                con.commit()

                print('position closed')
    except:
        print('error')
    sleep(300) # sleep for five minutes so binance doesn't get angry at me because am being abusive a lot
        

con.close()
