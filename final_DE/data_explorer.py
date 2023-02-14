import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class DataExplorer:
    def __init__(self, data_connection):
        self.data_connection = data_connection

    def query_explorer(self, query:str):
        '''
        Execute a query and return the result to the DataFrame
        query (str) : query to be executed
        '''
        data, column = self.data_connection.fetching(query)
        df = pd.DataFrame(data, columns=column)
        return df

    def visualizer(self, df, col_x, col_y, vis:str, title:str):
        '''
        Create a simple data visualization
        df = the dataframe
        col_x = column for x axis
        col_y = column for y axis
        vis = type of visualization (bar, line, or scatter)
        '''
        vis_map = {
            'bar' : sns.barplot,
            'line' : sns.lineplot,
            'scatter' : sns.lineplot
        }

        if vis not in vis_map:
            print('Error, vis must be input with "bar", "line", or "scatter"')
            return

        vis_map[vis](x=col_x, y=col_y, data=df)
        plt.title(title)
        return plt.show()