import pandas as pd



def readTable(path):
    """
    This function reads the excel file
    :param path: Path where the file is located
    :type path: str
    :return: DataFrame with read data
    :rtype:
    """
    # Read the file
    df = pd.read_excel(path, header=None)
    # Modify the names of the DataFrame
    rowNames = df[0].values[0:2].tolist()
    rowNames.extend([str(i) for i in range(len(df[0].values[2:]))])
    df[0] = rowNames
    df.set_index(0,inplace=True)
    df.index.name =None
    return df