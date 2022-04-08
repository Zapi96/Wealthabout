from flask_restful import Resource,reqparse
from functionsCalculations import *

import sqlite3

class Price(Resource):

    # Parser for post
    parser = reqparse.RequestParser()
    parser.add_argument('prices',
                        type=float,
                        action = 'append',
                        required=True,
                        help="This field cannot be left blank!"
                        )

    # Get command
    def get(self,date):

        # Open and load DB to DataFrame
        cnx = sqlite3.connect('Data.db')
        Pt = pd.read_sql('SELECT * FROM resultsPrices', cnx)
        Pt.set_index('index', inplace=True)
        Pt.index.name = None
        cnx.close()

        # Return value coresponding to date
        if date in Pt.index:
            return {'Pt': Pt['Pt'].iloc[int(date)]},200
        return {'Pt': 'date without price'},404

    def post(self,date):

        # Parse new set of data
        data = Price.parser.parse_args()

        # Create dictionary with data to be inserted
        new_price = {'date': int(date), 'prices': data['prices']}

        try:
            Price.insert(new_price)
        except:
            return {"message": "An error occurred inserting the item."}

        return new_price

    @classmethod
    def insert(cls, new_prices):

        # Load DB into DataFrames
        cnx = sqlite3.connect('Data.db')
        prices = pd.read_sql('SELECT * FROM dataPrices', cnx)
        Pt = pd.read_sql('SELECT * FROM resultsPrices', cnx)
        # Rename the rows
        prices.set_index('index', inplace=True)
        prices.index.name = None
        Pt.set_index('index', inplace=True)
        Pt.index.name = None
        # Extract new price
        new_price = pd.DataFrame([new_prices['prices']], columns=list(range(400)))
        as_list = new_price.index.tolist()
        as_list[0] = new_prices['date']
        new_price.index = as_list
        # Run the business logic
        businessLogic(prices, new_price, Pt)
        cnx.close()



class PriceList(Resource):
    def get(self):

        # Load the DB into Dataframe
        cnx = sqlite3.connect('Data.db')
        Pt = pd.read_sql('SELECT * FROM resultsPrices', cnx)
        Pt.set_index('index', inplace=True)
        Pt.index.name = None
        cnx.close()
        # Retunr only the Pt values and the dates
        return {'items': Pt['Pt'].values.tolist(),
                'dates':Pt.index.values.tolist()}

