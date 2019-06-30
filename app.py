from flask import Flask, jsonify, render_template
import json
import numpy as np
import pandas as pd
import decimal
import datetime

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import Column, Integer, String, DECIMAL, DateTime, cast, Date, extract
from flask_sqlalchemy import SQLAlchemy

def alchemyencoder(obj):
   if isinstance(obj, datetime.date):
       return obj.isoformat()
   elif isinstance(obj, decimal.Decimal):
       return float(obj)


engine = create_engine('mysql://flask:flasktest@awsflask.cpmhfsqdvkzl.us-east-2.rds.amazonaws.com/innodb')
session = Session(engine)
# Reflect Database into ORM class
Base = automap_base()
Base.prepare(engine, reflect=True)
Crypto_Table = Base.classes.crypto
session = Session(engine)
app = Flask(__name__)

# file = "static/data/historical_data_final.csv"
# historical_df = pd.read_csv(file)

currency_list = {
        "XRP" : 1,
        "ETH" : 2, 
        "LTC" : 3, 
        "BCC" : 4, 
        "EOS" : 5, 
        "BNC" : 6
        }   


@app.route("/")
def index():
    return "it works!!!!!"

@app.route("/livedata")
def sqldata():
    results = session.query(Crypto_Table.symbol, func.max(Crypto_Table.price),cast(Crypto_Table.crypto_timestamp, Date),extract('hour', cast(Crypto_Table.crypto_timestamp, DateTime)), extract('minute', cast(Crypto_Table.crypto_timestamp, DateTime)))\
     .group_by(Crypto_Table.symbol,cast(Crypto_Table.crypto_timestamp, Date),extract('hour', cast(Crypto_Table.crypto_timestamp, DateTime)), extract('minute', cast(Crypto_Table.crypto_timestamp, DateTime)))\
    .all()
    # filter(cast(Crypto_Table.crypto_timestamp, Timestamp) <= dateTimeInput2).distinct().all()
    test = list(np.ravel(results))
    return json.dumps(test, default=alchemyencoder)
     #test = list(np.ravel(results))
#     return json.dumps({'items': test}, default=alchemyencoder)
    
'''
The following routes are for calling the historical data API 
of each ETC and plotting the data in JS.
'''

@app.route("/BTC")
def names():
    """Return a list of BTC names."""
    return (jsonify(list(currency_list)))
    
@app.route("/historical_data/<correctedCurrency>")
def historical_data(correctedCurrency):
    currency_data = historical_df.loc[historical_df["correctedCurrency"] == correctedCurrency]

    data = {
        "Date": currency_data["Date"].tolist(),
        "Close": currency_data["Close"].tolist(),
    }

    return jsonify(data)
@app.route("/livedata/<firstCurrency>/")
#/<firstCurrency>&<secondCurrency>&<dateTimeInput1>&<dateTimeInput2>")
def currency_data(firstCurrency=None):

    results = session.query(Crypto_Table.symbol, Crypto_Table.price, cast(Crypto_Table.crypto_timestamp,DateTime))\
    .filter(Crypto_Table.symbol == firstCurrency).\
      limit(1000).all()
    # filter(cast(Crypto_Table.crypto_timestamp, Timestamp) <= dateTimeInput2).distinct().all()
    test = list(np.ravel(results))
    return json.dumps(test, default=alchemyencoder)


@app.route("/livedata/<firstCurrency>/<dateTimeInput1>")
#/<firstCurrency>&<secondCurrency>&<dateTimeInput1>&<dateTimeInput2>")
def collect_data(firstCurrency=None,dateTimeInput1=None):

    results = session.query(Crypto_Table.symbol, func.max(Crypto_Table.price),cast(Crypto_Table.crypto_timestamp, Date),extract('hour', cast(Crypto_Table.crypto_timestamp, DateTime)), extract('minute', cast(Crypto_Table.crypto_timestamp, DateTime)))\
    .filter(Crypto_Table.symbol == firstCurrency).\
    filter(cast(Crypto_Table.crypto_timestamp, DateTime) == dateTimeInput1).\
      group_by(Crypto_Table.symbol,cast(Crypto_Table.crypto_timestamp, Date),extract('hour', cast(Crypto_Table.crypto_timestamp, DateTime)), extract('minute', cast(Crypto_Table.crypto_timestamp, DateTime)))\
    .limit(1000).all()
    # filter(cast(Crypto_Table.crypto_timestamp, Timestamp) <= dateTimeInput2).distinct().all()
    test = list(np.ravel(results))
    return json.dumps(test, default=alchemyencoder)
    # engine = create_engine('mysql://flask:flasktest@awsflask.cpmhfsqdvkzl.us-east-2.rds.amazonaws.com/innodb')
    # session = Session(engine)
    # # Reflect Database into ORM class
    # Base = automap_base()
    # Base.prepare(engine, reflect=True)
    # Crypto_Table = Base.classes.crypto


@app.route("/api/v1.0/cryptosies")
def stations():
    conn = engine.connect()
    results = conn.execute("SELECT  symbol,max(price)as price,cast(crypto_timestamp as date) as crypto_date FROM crypto where cast(crypto_timestamp as date)= current_date group by symbol,cast(crypto_timestamp as date) order by crypto_timestamp desc")
    items = [dict(r) for r in results]
    return(json.dumps({'items': items}, default=alchemyencoder))
#     items = [dict(r) for r in result]
#     return(json.dumps({'items': items}, default=alchemyencoder))


#"""Return a list of stations."""
    #results = session.query(Crypto_Table).all()
    #print(results)
    # results = 
    # return(results)
    # return jsonify(results)
    # Unravel results into a 1D array and convert to a list
    # items = [dict(r) for r in results]
    # return(json.dumps({'items': items}, default=alchemyencoder))



    # items = [dict(r) for r in results]
    # return(json.dumps({'items': items}, default=alchemyencoder))





if __name__ == "__main__":
    app.run(debug=True)
