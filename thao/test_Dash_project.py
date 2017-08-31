import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
import xgboost as xgb
import time
import requests
import base64
import json
import pprint
import pandas as pd
import numpy as np
import datetime
from sklearn import preprocessing
import sqlalchemy
from sklearn.metrics import r2_score


app = dash.Dash()



app.layout = html.Div([
    dcc.Graph(id='graph-with-slider', animate=True),
    dcc.RadioItems(id='my-id', options=[
        {'label': '301', 'value': 301},
        {'label': '302', 'value': 302},
        {'label': '303', 'value': 303},
        {'label': '304', 'value': 304},
        {'label': '305', 'value': 305},
        {'label': '306', 'value': 306},
        {'label': '307', 'value': 307},
        {'label': '308', 'value': 308},
        {'label': '309', 'value': 309},
        {'label': '310', 'value':310},
        
    ],
    value=301),
    dcc.RadioItems(id='my-ii', options=[
        {'label': '1', 'value': 1},
        {'label': '2', 'value': 2},
        {'label': '3', 'value': 3},
        {'label': '4', 'value': 4},
        {'label': '5', 'value': 5},
        {'label': '6', 'value': 6},
        {'label': '7', 'value': 7},
        {'label': '8', 'value': 8},
        {'label': '9', 'value': 9},
        {'label': '10', 'value':10},
        
    ],
    value=3)    
    
    
])
@app.callback(
    dash.dependencies.Output('graph-with-slider', 'figure'),
    [dash.dependencies.Input('my-id', 'value'),
     dash.dependencies.Input('my-ii', 'value')])


    
def update_figure(selected_days,ratio):
    
    data6=pd.read_csv('c:/project/data7.csv')
    data7=pd.read_csv('c:/project/data7.csv')
    data7['preds'][data7.days > selected_days] += 2000*ratio
    trace0=go.Scatter(
                    x=data7.days,
                    y=data7.preds,
                    
                )
    trace1=go.Scatter(
                    x=data6.days,
                    y=data6.preds,
                    
                )
    return{
            'data': [trace0,trace1],
            'layout': go.Layout(
                xaxis={'title': 'days'},
                yaxis={'title': 'preds'},
                margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }

if __name__ == '__main__':
    app.run_server()  