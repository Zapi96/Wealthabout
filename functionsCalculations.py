import numpy as np
import pandas as pd
import random
import collections
import sqlite3



def returnUnderlying(x):
    """
    Function in charge of calculating the underlying return
    :param x: Column of DataFrame
    :type x: pd.Series
    :return: rt -return of ech underlying security
    :rtype: pd.Series
    """
    # Select current and next pt and calculate rt
    pt = np.array(x[1:])
    ptprev = np.array(x[0:-1])

    rt = pt/ptprev-1

    return pd.Series(rt)



def returnIndex(w,rt):
    """
    Function in charge of computing the sum of the multiplication of weights by rt
    :param w: weights
    :type w: pd.DataFrame
    :param rt: Return of ech underlying security
    :type rt: pd.Series
    :return: Return of the index on date
    :rtype: pd.DataFrame
    """

    return rt.mul(w,axis=1).sum(axis=1)

def priceIndex(Ptprev,Rt):
    """
    Function in charge of computing the price of the index on date t
    :param Ptprev:Price of the index on previous date
    :type Ptprev: float
    :param Rt: Return of the index on date
    :type Rt: float
    :return: Price of the index on date t
    :rtype: float
    """

    Pt = Ptprev*(1+Rt)

    return Pt

def calculationPt(Pt,Pt0=None):
    """
    Function in charge of calculating Pt for the whole dataframe
    :param Pt: DataFrame with Pt data
    :type Pt: pd.DataFrame
    :param Pt0: Initial value of Pt
    :type Pt0: float
    :return: DataFrame with Pt data
    :rtype: pd.Dataframe
    """
    # Loop over the DataFrame values
    for i in range(len(Pt)):
        # Check if the Pt DataFrame does not have a previous vae
        if i == 0 and not 'Ptprev' in Pt.columns:
            Pt['Ptprev'] = pd.Series([Pt0], index=[0])
            Pt['Pt'] = None
        elif i != 0:
            Pt.loc[i, 'Ptprev'] = Pt['Pt'].iloc[i - 1]
        # Compute price index
        Ptprev = Pt.iloc[i].Ptprev
        Rt = Pt.iloc[i].Rt
        Pt.loc[i, 'Pt'] = priceIndex(Ptprev, Rt)

    return Pt

def calculationInitial(prices,Pt0):
    """
    First calculation of Pt when the database is first created or when it must be completely recalculated
    :param prices: DataFrame with received data of prices
    :type prices: pd.DataFrame
    :param Pt0: Initial Pt
    :type Pt0: float
    :return: DataFrame with Pt data
    :rtype: pd.DataFrame
    """

    # Rows with prices
    t = [str(i) for i in prices.index[2:]]
    # Compute returnUnderlying
    rt = prices.loc[t].apply(returnUnderlying, axis=0)
    # Compute returnIndex
    Rt = returnIndex(prices.loc['weights'], rt)
    # Create Pt dataframe with results
    Pt = pd.DataFrame(Rt,columns=['Rt'])
    # Calculate Pt with data
    Pt = calculationPt(Pt,Pt0)
    # Rename indices
    Pt.index = prices.index[3:]

    return Pt

def calculationNew(prices,Pt):
    """
    Succesive calculations of Pt when the database is updated
    :param prices: DataFrame with received data of prices
    :type prices: pd.DataFrame
    :param Pt: Data of Pt
    :type Pt: pd.DataFrame
    :return: DataFrame with Pt data
    :rtype: pd.DataFrame
    """

    # Rows with prices
    rows = prices.index
    # Last prices extraction
    prices = prices.loc[rows[[0, 1, -2, -1]]]
    # Rows selection
    t = [str(i) for i in prices.index[2:]]
    # Compute returnUnderlying
    rt = prices.loc[t].apply(returnUnderlying, axis=0)
    # Compute returnIndex
    Rt = returnIndex(prices.loc['weights'], rt)
    # Create Rt dataframe with results
    Rt = pd.DataFrame(Rt,columns=['Rt'])
    # Increase initial index
    Rt.index += int(prices.index[-1])
    # Update previous value
    Rt['Ptprev'] = Pt['Pt'].iloc[-1]
    # Concatenate old dataframe with new result of calculation
    Pt = pd.concat([Pt, Rt])
    # Calculate new Pt
    newPt = calculationPt(Pt.iloc[-1:].reset_index(drop=True))
    # Add the new Pt to the concatenated DataFrame
    Pt.loc[int(prices.index[-1]),'Pt'] = newPt['Pt'].iloc[-1]

    return Pt


def pricesGenerator():
    """
    This function generated new prices for local simulation
    :return: New price
    :rtype: pd.DataFrame
    """
    # Randomly create date and list of prices
    new_prices = [random.uniform(0.6, 2) for _ in range(400)]
    new_date = random.randint(0, 10)

    # Create DataFrame of new price
    new_price = pd.DataFrame([new_prices],columns =list(range(400)))
    as_list = new_price.index.tolist()
    as_list[0] = new_date
    new_price.index = as_list
    return new_price



def pricesChecker(prices,new_prices,cnx):
    """
    Check if the price was already on the list and update he database
    :param prices: DataFrame with prices data
    :type prices: pd.DataFrame
    :param new_prices: New price generated
    :type new_prices: pd.DataFrame
    :param cnx: Conecction to DB
    :type cnx:
    :return: New DataFrame with new price added and recalculation option
    :rtype:
    """

    # Check if the dataframe passed with the new prices contain duplicated dates and select the last one
    repeated_dates = [item for item, count in collections.Counter(new_prices).items() if count > 1]

    for repeated in repeated_dates:
        idx = [i for i,x in enumerate(new_prices.index) if x==repeated]
        for i in idx[:-1]:
            new_prices.loc[i] = np.NaN
        new_prices.dropna(axis=0,inplace=True)

    # Initialize recalculate variable
    recalculate = False

    # Check if the price was on the DB and set if recalculation is required
    for date in new_prices.index:
        if str(date) in prices.index or date < int(prices.index[-1]):
            recalculate = True
        prices.loc[str(date)] = new_prices.loc[date].values.tolist()

    # Update DB
    prices.to_sql(name='dataPrices', con=cnx, if_exists='replace')

    # Modify the indices to match the correct date
    rows = list(prices.index)
    rowsSorted = rows[2:].copy()
    rowsSorted.sort(key=int)
    rows = rows[:2].copy()
    rows.extend(rowsSorted)
    prices = prices.reindex(rows, axis=0)

    return prices,recalculate,cnx


def businessLogic(prices, new_price,Pt):
    """
    This function contains all the main business logic
    :param prices: DataFrame with prices data
    :type prices: pd.DataFrame
    :param new_prices: New price generated
    :type new_prices: pd.DataFrame
    :param Pt: Data of Pt
    :type Pt: pd.DataFrame
    :return: New prices and Pt dataFrames
    :rtype:
    """
    cnx = sqlite3.connect('Data.db')
    prices, recalculate, cnx = pricesChecker(prices, new_price, cnx)
    cnx.close()
    if recalculate:
        Pt = calculationInitial(prices, 100)
        cnx = sqlite3.connect('Data.db')
        Pt.to_sql(name='resultsPrices', con=cnx, if_exists='replace')
        cnx.close()
    else:
        Pt = calculationNew(prices, Pt)

        cnx = sqlite3.connect('Data.db')
        cursor = cnx.cursor()

        added = Pt.iloc[-1].name, *Pt.iloc[-1].values.tolist()
        query = ''' INSERT INTO resultsPrices('index',Rt,Ptprev,Pt) VALUES(?,?,?,?) '''
        cursor.execute(query, added)
        query = ''' SELECT * FROM resultsPrices ORDER BY 'index' ASC '''
        cursor.execute(query)

        cnx.close()

    return prices,Pt




