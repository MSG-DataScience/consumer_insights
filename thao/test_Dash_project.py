<<<<<<< HEAD
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
from sklearn.externals import joblib


app = dash.Dash()



app.layout = html.Div([
    dcc.Graph(id='graph-with-slider', animate=True),
    html.Label('Facebook'),
   dcc.Dropdown(id='my-id', options=[
        {'label': 'none', 'value': 0},
        {'label': '9/17/2017', 'value': 260},
        {'label': '9/27/2017', 'value': 270},
        {'label': '10/7/2017', 'value': 280},
        {'label': '10/17/2017', 'value': 290},
        {'label': '10/27/2017', 'value': 300},
        {'label': '11/6/2017', 'value': 310},
        {'label': '11/16/2017', 'value': 320},
        {'label': '11/26/2017', 'value': 330},
        {'label': '12/6/2017', 'value':340},
    ],
    value=300),
           html.Label('Twitter'),
      dcc.Dropdown(id='my-ii', options=[
        {'label': 'none', 'value': 0},
        {'label': '9/17/2017', 'value': 260},
        {'label': '9/27/2017', 'value': 270},
        {'label': '10/7/2017', 'value': 280},
        {'label': '10/17/2017', 'value': 290},
        {'label': '10/27/2017', 'value': 300},
        {'label': '11/6/2017', 'value': 310},
        {'label': '11/16/2017', 'value': 320},
        {'label': '11/26/2017', 'value': 330},
        {'label': '12/6/2017', 'value':340},
    ],
    value=300),
              html.Label('Promo'),
    dcc.Dropdown(id='my-dd', options=[
        {'label': 'none', 'value': 0},
        {'label': '9/17/2017', 'value': 260},
        {'label': '9/27/2017', 'value': 270},
        {'label': '10/7/2017', 'value': 280},
        {'label': '10/17/2017', 'value': 290},
        {'label': '10/27/2017', 'value': 300},
        {'label': '11/6/2017', 'value': 310},
        {'label': '11/16/2017', 'value': 320},
        {'label': '11/26/2017', 'value': 330},
        {'label': '12/6/2017', 'value':340},
    ],
    value=300),
                          html.Label('Email'),
    dcc.Dropdown(id='my-asd', options=[
        {'label': 'none', 'value': 0},
        {'label': '9/17/2017', 'value': 260},
        {'label': '9/27/2017', 'value': 270},
        {'label': '10/7/2017', 'value': 280},
        {'label': '10/17/2017', 'value': 290},
        {'label': '10/27/2017', 'value': 300},
        {'label': '11/6/2017', 'value': 310},
        {'label': '11/16/2017', 'value': 320},
        {'label': '11/26/2017', 'value': 330},
        {'label': '12/6/2017', 'value':340},
    ],
    value=300),
            html.Label('Total'),
    html.Div(id='display-selected-values')
    
])
@app.callback(
    dash.dependencies.Output('graph-with-slider', 'figure'),
    [dash.dependencies.Input('my-id', 'value'),
     dash.dependencies.Input('my-ii', 'value'),
     dash.dependencies.Input('my-dd', 'value'),
     dash.dependencies.Input('my-asd', 'value')])


    
def update_figure(selected_days,ratio,lala,email):
    test_x=pd.read_csv("c:/Users/haot/test_xe.csv")
    clf=joblib.load('c:/Users/haot/xe.pkl')
    day=pd.read_csv("c:/Users/haot/test_date.csv")
    test_x['Facebook'][test_x.yearday > selected_days] += 100000
    test_x['Twitter'][test_x.yearday > ratio] += 100000
    test_x['promo'][test_x.yearday == lala] = 1
    test_x['sent'][test_x.yearday > email] = 1000000
    test_y=clf.predict(test_x)

    return{
            'data': [
                    go.Scatter(
                    x=day.date,
                    y=test_y,
                    
                )],
            'layout': go.Layout(
                
                xaxis={'title': 'days'},
                yaxis={'title': 'preds'},
                margin={'l': 100, 'b': 100, 't': 100, 'r': 100},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }
       
@app.callback(
    dash.dependencies.Output('display-selected-values', 'children'),
    [dash.dependencies.Input('my-id', 'value'),
     dash.dependencies.Input('my-ii', 'value'),
     dash.dependencies.Input('my-dd', 'value'),
     dash.dependencies.Input('my-asd', 'value')])
        
def update_figure(selected_days,ratio,lala,email):
    test_x=pd.read_csv("c:/Users/haot/test_xe.csv")
    clf=joblib.load('c:/Users/haot/xe.pkl')
    
    test_x['Facebook'][test_x.yearday  >  selected_days] += 1000000
    test_x['Twitter'][test_x.yearday > ratio] += 100000
    test_x['promo'][test_x.yearday == lala] = 1
    test_x['sent'][test_x.yearday > email] = 1000000
    test_y=clf.predict(test_x)
    c=sum(test_y)

    return u'{}'.format(round(c))

if __name__ == '__main__':
=======
import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import pandas as pd
from sklearn.externals import joblib


app = dash.Dash()



app.layout = html.Div([
    dcc.Graph(id='graph-with-slider', animate=True),
    html.Label('Facebook'),
   dcc.Dropdown(id='my-id', options=[
        {'label': 'none', 'value': 0},
        {'label': '9/17/2017', 'value': 260},
        {'label': '9/27/2017', 'value': 270},
        {'label': '10/7/2017', 'value': 280},
        {'label': '10/17/2017', 'value': 290},
        {'label': '10/27/2017', 'value': 300},
        {'label': '11/6/2017', 'value': 310},
        {'label': '11/16/2017', 'value': 320},
        {'label': '11/26/2017', 'value': 330},
        {'label': '12/6/2017', 'value':340},
    ],
    value=300),
           html.Label('Twitter'),
      dcc.Dropdown(id='my-ii', options=[
        {'label': 'none', 'value': 0},
        {'label': '9/17/2017', 'value': 260},
        {'label': '9/27/2017', 'value': 270},
        {'label': '10/7/2017', 'value': 280},
        {'label': '10/17/2017', 'value': 290},
        {'label': '10/27/2017', 'value': 300},
        {'label': '11/6/2017', 'value': 310},
        {'label': '11/16/2017', 'value': 320},
        {'label': '11/26/2017', 'value': 330},
        {'label': '12/6/2017', 'value':340},
    ],
    value=300),
              html.Label('Promo'),
    dcc.Dropdown(id='my-dd', options=[
        {'label': 'none', 'value': 0},
        {'label': '9/17/2017', 'value': 260},
        {'label': '9/27/2017', 'value': 270},
        {'label': '10/7/2017', 'value': 280},
        {'label': '10/17/2017', 'value': 290},
        {'label': '10/27/2017', 'value': 300},
        {'label': '11/6/2017', 'value': 310},
        {'label': '11/16/2017', 'value': 320},
        {'label': '11/26/2017', 'value': 330},
        {'label': '12/6/2017', 'value':340},
    ],
    value=300),
                          html.Label('Email'),
    dcc.Dropdown(id='my-asd', options=[
        {'label': 'none', 'value': 0},
        {'label': '9/17/2017', 'value': 260},
        {'label': '9/27/2017', 'value': 270},
        {'label': '10/7/2017', 'value': 280},
        {'label': '10/17/2017', 'value': 290},
        {'label': '10/27/2017', 'value': 300},
        {'label': '11/6/2017', 'value': 310},
        {'label': '11/16/2017', 'value': 320},
        {'label': '11/26/2017', 'value': 330},
        {'label': '12/6/2017', 'value':340},
    ],
    value=300),
            html.Label('Total'),
    html.Div(id='display-selected-values')
    
])
@app.callback(
    dash.dependencies.Output('graph-with-slider', 'figure'),
    [dash.dependencies.Input('my-id', 'value'),
     dash.dependencies.Input('my-ii', 'value'),
     dash.dependencies.Input('my-dd', 'value'),
     dash.dependencies.Input('my-asd', 'value')])


    
def update_figure(selected_days,ratio,lala,email):
    test_x=pd.read_csv("c:/Users/haot/test_xe.csv")
    clf=joblib.load('c:/Users/haot/xe.pkl')
    day=pd.read_csv("c:/Users/haot/test_date.csv")
    test_x['Facebook'][test_x.yearday > selected_days] += 100000
    test_x['Twitter'][test_x.yearday > ratio] += 100000
    test_x['promo'][test_x.yearday == lala] = 1
    test_x['sent'][test_x.yearday > email] = 1000000
    test_y=clf.predict(test_x)

    return{
            'data': [
                    go.Scatter(
                    x=day.date,
                    y=test_y,
                    
                )],
            'layout': go.Layout(
                
                xaxis={'title': 'days'},
                yaxis={'title': 'preds'},
                margin={'l': 100, 'b': 100, 't': 100, 'r': 100},
                legend={'x': 0, 'y': 1},
                hovermode='closest'
            )
        }
       
@app.callback(
    dash.dependencies.Output('display-selected-values', 'children'),
    [dash.dependencies.Input('my-id', 'value'),
     dash.dependencies.Input('my-ii', 'value'),
     dash.dependencies.Input('my-dd', 'value'),
     dash.dependencies.Input('my-asd', 'value')])
        
def update_figure(selected_days,ratio,lala,email):
    test_x=pd.read_csv("c:/Users/haot/test_xe.csv")
    clf=joblib.load('c:/Users/haot/xe.pkl')
    
    test_x['Facebook'][test_x.yearday  >  selected_days] += 1000000
    test_x['Twitter'][test_x.yearday > ratio] += 100000
    test_x['promo'][test_x.yearday == lala] = 1
    test_x['sent'][test_x.yearday > email] = 1000000
    test_y=clf.predict(test_x)
    c=sum(test_y)

    return u'{}'.format(round(c))

if __name__ == '__main__':
>>>>>>> 0c9d4d58a5cfa254b093e71c93a6ca591d35c6c9
    app.run_server()  