
import requests
import json
import sqlite3
from pandas import DataFrame
import datetime
import numpy as np

def get_data_and_sort(e,e2,URL):
    pay = gen_pay(e,e2)
    res1 = requests.request("POST", URL, headers=headers, data=pay)
    res1 = json.loads(res1.text)["result"]["transfers"]
    pay = gen_pay(e,"3")
    res2 = requests.request("POST", URL, headers=headers, data=pay)
    res2 = json.loads(res2.text)["result"]["transfers"]

    trans = res1 + res2
    arr = np.array(trans)
    np_arr = np.array([int(i["blockNum"], 16) for i in trans])
    np_arr = np_arr.argsort(kind = "stable")[::-1]
    return arr[np_arr]

URL = ""
def gen_pay(address: str, type: str, page: str = ""):

    if type == "1":
        return json.dumps({
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenBalances",
        "params": [
            f"{address}",
            "erc20"
        ]})
    elif type == "2":
        return json.dumps({
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
            "fromBlock": "0x0",
            "toBlock": "latest",
            "toAddress": f"{address}",
            "category": [
                "external",
                "erc20",
                "internal"
            ],
            "withMetadata": True,
            "excludeZeroValue": True,
            "maxCount": "0x3e8",
            "order": "desc"
            }
        ]})
    
    elif type == "3":

        return json.dumps({
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
            "fromBlock": "0x0",
            "toBlock": "latest",
            "fromAddress": f"{address}",
            "category": [
                "external",
                "erc20",
                "internal"
            ],
            "withMetadata": True,
            "excludeZeroValue": True,
            "maxCount": "0x3e8",
            "order": "desc"
            }
        ]})

    elif type == "4":
        return json.dumps({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getTokenMetadata",
            "params": [f"{address}"]
        })
    elif type == "5":

        return json.dumps({
            "id": 1,
            "jsonrpc": "2.0",
            "params": [f"{address}", "latest"],
            "method": "eth_getBalance"
        })
    
headers = {
  'Content-Type': 'application/json'
}

WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

def main():

    ex = False

    conn = sqlite3.connect("erc20.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS tokens (contract_address VARCHAR(70) NOT NULL,name VARCHAR(70),decimal INT,symbol VARCHAR(70),logo VARCHAR(300),PRIMARY KEY(contract_address));")
    cur.execute("CREATE TABLE IF NOT EXISTS blocks (wallet VARCHAR(70), from_block INT NOT NULL, to_block INT NOT NULL, start_time VARCHAR(50), end_time VARCHAR(50), PRIMARY KEY(wallet));")
    cur.execute("CREATE TABLE IF NOT EXISTS transactions (wallet VARCHAR(70), hash VARCHAR(70), id VARCHAR(70), from_address INT NOT NULL, to_address INT NOT NULL, category VARCHAR(30), asset VARCHAR(50), quantity DOUBLE, block_number INT, timestamp Date, PRIMARY KEY(hash,id));")
    conn.commit()

    u = input("If you want to change ENDPOINT URL enter (press enter to skip and use default one)\n:")

    with open("URL.txt","r+") as file:
        if u.lower().strip() != "":
            file.truncate(0)
            file.write(u)
            URL = u
        else:
            URL = file.read()

    e = input("Wallet Adress (Enter q for exit): ")


    while(e.lower().strip() != "q"):

        print("1) Check Balances\n"
                    "2) Check Transactions\n"
                    "3) Check Token Based Net Profits/Losses\n"
                    "4) Save last fetched data\n"
                    "5) Change wallet\n"
                    "6) Exit")
        
        e2 = input(":")

        while(e2.lower().strip() != "5"):
                
            try:

                if e2 == "1":

                    data = []

                    ETH_balance = 0

                    pay = gen_pay(e,e2)
                    res = requests.request("POST", URL, headers=headers, data=pay)
                    res = json.loads(res.text)["result"]["tokenBalances"]
                    
                    for i in res:

                        balance = int(i["tokenBalance"],16) 
                        if balance == 0:
                            continue
                        contract = i["contractAddress"]

                        
                        query = f"SELECT * from TOKENS where contract_address = '{contract}'"

                        cur.execute(query)
                        values = cur.fetchone()
                        
                        if values == None:
                            pay = gen_pay(contract,"4")
                            res = requests.request("POST", URL, headers=headers, data=pay)
                            res = json.loads(res.text)["result"]
                            decimal = res["decimals"]
                            symbol = res["symbol"]
                            logo = res["logo"]
                            name = res["name"]
                            params = (contract, name, decimal, symbol, logo)
                            query = f"INSERT INTO TOKENS VALUES(?, ?, ?, ?, ?)"
                            cur.execute(query, params)
                        else:
                            name = values[1]
                            decimal = values[2]
                            symbol = values[3]
                            logo = values[4]

                        if decimal == 0:
                            continue

                        if contract == WETH:
                            ETH_balance += balance / 10 ** decimal

                        data += [[contract, name, symbol, balance / 10 ** decimal, logo]]

                    pay = gen_pay(e,"5")
                    res = requests.request("POST", URL, headers=headers, data=pay)
                    res = json.loads(res.text)["result"]
                    balance = int(res, 16) / 10 ** 18
                    ETH_balance += balance

                    data += [[]]
                    data += [["-","Ethereum (REAL)","ETH",ETH_balance]]
                    
                    conn.commit()
                    df = DataFrame(data, columns = ["Contract", "Name", "Symbol", "Balance", "Logo"])
                    now = datetime.datetime.now()
                    now = now.strftime("%Y-%m-%d %H.%M.%S")
                    name = f"Wallet Data {now} {e}"
                    print(df[["Name", "Symbol", "Balance"]])

                elif e2 == "2":

                    arr = get_data_and_sort(e,e2,URL)
                    df = DataFrame([[i["hash"],i["from"], i["to"], i["category"], i["asset"], i["value"], int(i["blockNum"],16)] for i in arr],
                                     columns = ["Transaction Hash","From", "To", "Category", "Asset", "Quantity", "Block Number"])
                    
                    first = int(arr[0]["blockNum"],16)
                    last = int(arr[-1]["blockNum"],16)
                    name = f"Transactions of {e} to block number {last}"


                    query = "SELECT * from blocks WHERE wallet = ?;"
                    cur.execute(query, (e,))
                    value = cur.fetchone()
                    counter = 0
                    if value == None:
                        query = "INSERT INTO blocks VALUES(?, ?, ?, ? ,?)"
                        cur.execute(query, (e, last+1, first, arr[-1]["metadata"]["blockTimestamp"], arr[0]["metadata"]["blockTimestamp"]))
                        for i in arr:
                            counter += 1
                            query = "INSERT INTO transactions VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
                            cur.execute(query, (e, i["hash"], i["uniqueId"], i["from"], i["to"], i["category"], i["asset"], i["value"], int(i["blockNum"], 16),))
                    else:
                        found = False
                        for i in arr:
                            blockNum = int(i["blockNum"],16)
                            timestamp = i["metadata"]["blockTimestamp"]
                            if blockNum > value[2]:
                                counter += 1
                                found = True
                                query = "INSERT INTO transactions VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                cur.execute(query, (e, i["hash"], i["uniqueId"], i["from"], i["to"], i["category"], i["asset"], i["value"], int(i["blockNum"], 16),))
                        if found:
                            query = "UPDATE blocks SET to_block = ?, end_date = ? where wallet = ?"
                            cur.execute(query, (blockNum, timestamp, e,))
                        
                    conn.commit()

                    print(f"{counter} number of transactions added to database.\n")

                elif e2 == "3":

                    data_dict = dict()
                    arr = get_data_and_sort(e,"2",URL)
                    last_tx = 0
                    last_erc = ""
                    last_ETH = 0
                    stack = 0 
                    buy_send = False
                    buy_receive = False
                    sell_send = False
                    sell_receive = False
                    
                    trades = []

                    for obs in arr:

                        category = obs["category"]
                        from_ = obs["from"]
                        value = obs["value"]
                        asset = obs["asset"]
                        if asset == None:
                            continue

                        if category == "internal" or category == "external":
                            # Buy send transaction
                            if from_ == e.lower():
                                if last_tx == obs["hash"]:
                                    stack += 1
                                    last_ETH -= obs["value"]
                                else:
                                    if stack >= 1 and ((buy_receive and buy_send) or (sell_send and sell_receive)):
                                        stack = 0
                                        if last_ETH < 0:
                                            state = "BUY"
                                        else:
                                            state = "SELL"
                                        trades += [[last_erc, state, abs(last_ETH), obs["metadata"]["blockTimestamp"]]]
                                        if last_erc not in data_dict:
                                            data_dict[last_erc] = last_ETH
                                        else:
                                            data_dict[last_erc] += last_ETH
                                    buy_receive = False
                                    buy_send = False
                                    sell_receive = False
                                    sell_send = False
                                    last_ETH = -obs["value"]
                                buy_send = True
                            # Sell receive transaction
                            else:
                                if last_tx == obs["hash"]:
                                    stack += 1
                                    last_ETH += obs["value"]
                                else:
                                    if stack >= 1 and ((buy_receive and buy_send) or (sell_send and sell_receive)):
                                        stack = 0
                                        if last_ETH < 0:
                                            state = "BUY"
                                        else:
                                            state = "SELL"
                                        trades += [[last_erc, state, abs(last_ETH), obs["metadata"]["blockTimestamp"]]]
                                        if last_erc not in data_dict:
                                            data_dict[last_erc] = last_ETH
                                        else:
                                            data_dict[last_erc] += last_ETH
                                    last_ETH = obs["value"]
                                    buy_receive = False
                                    buy_send = False
                                    sell_receive = False
                                    sell_send = False
                                sell_receive = True
                        else:
                            if asset == "WETH":
                                 # Buy send transaction
                                if from_ == e.lower():
                                    if last_tx == obs["hash"]:
                                        stack += 1
                                        last_ETH -= obs["value"]
                                    else:
                                        if stack >= 1 and ((buy_receive and buy_send) or (sell_send and sell_receive)):
                                            stack = 0
                                            if last_ETH < 0:
                                                state = "BUY"
                                            else:
                                                state = "SELL"
                                            trades += [[last_erc, state, abs(last_ETH), obs["metadata"]["blockTimestamp"]]]
                                            if last_erc not in data_dict:
                                                data_dict[last_erc] = last_ETH
                                            else:
                                                data_dict[last_erc] += last_ETH
                                        last_ETH = -obs["value"]
                                        buy_receive = False
                                        buy_send = False
                                        sell_receive = False
                                        sell_send = False
                                    buy_send = True
                                # Sell receive transaction
                                else:
                                    if last_tx == obs["hash"]:
                                        stack += 1
                                        last_ETH += obs["value"]
                                    else:
                                        if stack >= 1 and ((buy_receive and buy_send) or (sell_send and sell_receive)):
                                            stack = 0
                                            if last_ETH < 0:
                                                state = "BUY"
                                            else:
                                                state = "SELL"
                                            trades += [[last_erc, state, abs(last_ETH), obs["metadata"]["blockTimestamp"]]]
                                            if last_erc not in data_dict:
                                                data_dict[last_erc] = last_ETH
                                            else:
                                                data_dict[last_erc] += last_ETH
                                        last_ETH = obs["value"]
                                        buy_receive = False
                                        buy_send = False
                                        sell_receive = False
                                        sell_send = False
                                    sell_receive = True
                            else:
                                # Sell send transaction
                                if from_ == e.lower():
                                    if last_tx == obs["hash"]:
                                        stack += 1
                                    else:
                                        if stack >= 1 and ((buy_receive and buy_send) or (sell_send and sell_receive)):
                                            stack = 0
                                            if last_ETH < 0:
                                                state = "BUY"
                                            else:
                                                state = "SELL"
                                            trades += [[last_erc, state, abs(last_ETH), obs["metadata"]["blockTimestamp"]]]
                                            if last_erc not in data_dict:
                                                data_dict[last_erc] = last_ETH
                                            else:
                                                data_dict[last_erc] += last_ETH
                                        last_ETH = 0
                                        buy_receive = False
                                        buy_send = False
                                        sell_receive = False
                                        sell_send = False
                                    last_erc = asset
                                    sell_send = True
                                # Buy receive transaction
                                else:
                                    if last_tx == obs["hash"]:
                                        stack += 1
                                    else:
                                        if stack >= 1 and ((buy_receive and buy_send) or (sell_send and sell_receive)):
                                            stack = 0
                                            if last_ETH < 0:
                                                state = "BUY"
                                            else:
                                                state = "SELL"
                                            trades += [[last_erc, state, abs(last_ETH), obs["metadata"]["blockTimestamp"]]]
                                            if last_erc not in data_dict:
                                                data_dict[last_erc] = last_ETH
                                            else:
                                                data_dict[last_erc] += last_ETH
                                        last_ETH = 0
                                        buy_receive = False
                                        buy_send = False
                                        sell_receive = False
                                        sell_send = False
                                    last_erc = asset
                                    buy_receive = True

                        last_tx = obs["hash"]
                    
                    if stack >= 1 and ((buy_receive and buy_send) or (sell_send and sell_receive)): 
                        if last_ETH < 0:
                            state = "BUY"
                        else:
                            state = "SELL"
                        trades += [[last_erc, state, abs(last_ETH), arr[-1]["metadata"]["blockTimestamp"]]]
                        if last_erc not in data_dict:
                            data_dict[last_erc] = last_ETH
                        else:
                            data_dict[last_erc] += last_ETH

                    trades_dataframe = DataFrame(trades, columns = ["Asset", "Action", "ETH Value", "Timestamp"])
                    trades_dataframe.to_excel(f"Trade History of {e}.xlsx")
                    last_data = list(data_dict.items())
                    df = DataFrame(last_data, columns = ["Asset", "ETH Profit/Loss"])
                    name = f"Profit Data Wallet {e}"
                    print(df)

                                        
                elif e2 == "4":

                    df.to_excel(f"{name}.xlsx")
                    print("Excel file created successfully\n")

                elif e2 == "5":
                    break
                
                elif e2 == "6":
                    ex = True
                    break

            except Exception as exception:
                print(exception)

            print("1) Check Balances\n"
                    "2) Check Transactions\n"
                    "3) Check Token Based Net Profits/Losses\n"
                    "4) Save last fetched data\n"
                    "5) Change wallet\n"
                    "6) Exit")
        
            e2 = input(":")
        
        if ex == True:
            break
        e = input("Wallet Adress (Enter q for exit): ")


if __name__ == "__main__":

    main()






    


