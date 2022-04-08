from flask import Flask
from flask_restful import Api

from prices import Price, PriceList

# Initialization of the REST API
app = Flask(__name__)
api = Api(app)

# Add the resources price and price list
api.add_resource(Price,'/prices/<string:date>')
api.add_resource(PriceList, '/prices')











