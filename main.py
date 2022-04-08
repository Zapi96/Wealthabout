import os

from app import *

from functionsRead import *
from functionsCalculations import *


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # Path definition
    folderData = 'C:\\Users\\JJMZ\\Documents\\WealthaboutFinal'
    fileData = 'Data.xlsx'
    path = os.path.join(folderData, fileData)

    # Select if a local simulation of reception of data is to be made
    simulate = False

    # Select if the database must be recalculated from the Excel
    recalculate = True

    # Read file and initial calculation if database does not exist or recalculate is active
    if not 'Data.db' in os.listdir() or recalculate:
        # Read data
        prices = readTable(path)

        # First calculation
        Pt = calculationInitial(prices, 100)

        # Store data in DBs
        cnx = sqlite3.connect('Data.db')
        prices.to_sql(name='dataPrices', con=cnx,if_exists='replace')
        Pt.to_sql(name='resultsPrices', con=cnx,if_exists='replace')
        cnx.close()

    # Local simulation
    if simulate:
        # Load databases
        cnx = sqlite3.connect('Data.db')
        prices = pd.read_sql('SELECT * FROM dataPrices',cnx)
        Pt = pd.read_sql('SELECT * FROM resultsPrices', cnx)

        # Rename indices
        prices.set_index('index',inplace=True)
        prices.index.name = None
        Pt.set_index('index', inplace=True)
        Pt.index.name = None
        cnx.close()


        # New data generation
        for _ in range(100):
            new_price = pricesGenerator()

            # Run business logic
            prices,Pt = businessLogic(prices, new_price, Pt)


    app.run(port=5000)