import os
import time
import tempfile
import pathlib

from datetime import date
import awswrangler as wr
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import dash
from dash.dependencies import Input, Output
from dash import dcc
from dash import html
import dash.dash_table as dt
from dash.dash_table.Format import Group

from os import listdir
from os.path import isfile, join

def create_df(account, product, date, quantity):
    df1 = pd.DataFrame({
        "account": account,
        "product": product,
        "date": date,
        "quantity": quantity
    })
    return df1

df2 = pd.DataFrame({
    "id": [3],
    "name": ["bar"]
})

#path1 = f"s3://unravel-saas-demo/test/sales.parquet"
path1 = f"sales.parquet"

def write_to_s3(writer):
    account = [1, 2, 3, 4, 5, 6, 7]
    product = ["apple", "banana", "pineapple", "plum", "orange", "guava", "kiwi"]
#    date = [1631138217, 1631208217, 1631308217, 1631378217, 1631478217, 1631548217, 1631648217] 
    date = [1631138217, 1631208217, 1631308217, 1631378217, 1631478217, 1631648217, 1631548217] 
    quantity = [11, 4, 7, 23, 12, 8, 17]

    df1 = create_df(account, product, date, quantity) 
    table = pa.Table.from_pandas(df1)
    if writer is None:
        writer = pq.ParquetWriter('sales1.parquet', table.schema)
    writer.write_table(table=table)
    
#    account= account + 1

#        wr.s3.to_parquet(
#            df1,
#            path=path1,
#        )

def read_from_s3():
#    path1 = f"s3://unravel-saas-demo/test/my.parquet"
    return wr.s3.read_parquet([path1])

#writer = None
#writer = write_to_s3(writer)
#print("s3 data: " + str(read_from_s3()))
#print(str(read_from_s3()))


def compute(df):
    table = pq.read_table("sales1.parquet")
    df = table.to_pandas()
    print(df)
    return df

def compute_rec(df):
    base_dir = pathlib.Path(tempfile.gettempdir()) / "goal"
    part = f'{base_dir}/test_1'
    dataset = ds.dataset(part, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()
    print(df)
    df['date'] = pd.to_datetime(df['date'], unit='s')
    print(df)
    s = df['date']
    print(s)
    #s1 = pd.date_range(s[0], s[4], freq='D').to_series()
    print(df['date'].dt.dayofweek)
    print(df['date'].dt.day_name())

    print("------------")
    sum_of_quantity = df['quantity'].sum()
    print(f"sum of quantity: {sum_of_quantity}")

    print("------------")
    var_of_quantity = df.loc[:, "quantity"].var()
    print(f"variance of quantity: {var_of_quantity}")

    accounts = df['account'].unique()
    unique_accounts = pd.Series(accounts)
    print("------------")
    print(type(unique_accounts))
    #print(f"unique accounts: {unique_accounts}")
    for account, value in unique_accounts.items():
        print(f"account #: {account}, Value: {value}")

    print("------------")
    products = df['product'].unique()
    unique_products = pd.Series(products)
    #print(f"unique products: {unique_products}")
    for product, value in unique_products.items():
        print(f"product #: {product}, Value: {value}")

    row_count = len(df.index)
    print(f"Number of rows: {row_count}")
    
    rec_data = {'Sum_of_Quantity': [sum_of_quantity], 
                'Variance of Quntity': [var_of_quantity],
                'Numver of rows': [row_count]}
    
    rec_df = pd.DataFrame(rec_data)

    return rec_df

#    pq_file = pq.ParquetFile(path1)
#    print(pq_file.metadata)
#    print(pq_file.schema)
#    print(pq_file.metadata.row_group(0))
#    print(pq_file.metadata.row_group(0).column(0))

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

#record_batch = pa.RecordBatch.from_pandas(df)
#print(record_batch)

#s3 = fs.S3FileSystem(region="us-east-1")
#table = pq.read_table("unravel-saas-demo/test/df1.parquet", filesystem=s3)
#print(table)
#df = table.to_pandas()
#print(df) 

#import duckdb as db
#print(db.query("SELECT SUM(account) FROM path1").fetchall())


'''
-----------------------------------------------
DASH 
-----------------------------------------------
'''
import dash_bootstrap_components as dbc

app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Parquet file read/write/update"
app_title = "Parquet Challenge"
app_logo = 'https://pbs.twimg.com/profile_images/1017450640627388424/mGctPJzG_400x400.jpg'

PAGE_SIZE = 5

def serve_layout():
    df = None
    df_rec = None
    df = compute(df)
    df_rec = compute_rec(df_rec)
    location = dcc.Location(id='url', refresh=False)
    navbar = dbc.Navbar(
        [
            html.A(
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=app_logo, height="30px")),
                        dbc.Col(dbc.NavbarBrand(app_title, className="ml-2")),
                    ],
                    align="center",
                    no_gutters=True,
                ),
                href="/",
            ),
            dbc.NavbarToggler(id="navbar-toggler"),
        ],
        color="dark",
        dark=True,
    )
#    return html.Div(A
    page_content = html.Div(
        className="row",
        children=[
            html.Div(
                dt.DataTable(
                    id='table-paging-with-graph',
                    columns=[
                        {"name": i, "id": i} for i in sorted(df.columns)
                    ],
                    page_current=0,
                    page_size=30,
                    page_action='custom',

                    filter_action='custom',
                    filter_query='',

                    sort_action='custom',
                    sort_mode='multi',
                    sort_by=[],
                ),
                style={'height': 450, 'overflowY': 'scroll'},
                className='m-5',
            ),
            html.Div(
                id='table-paging-with-graph-container',
            ),
            html.Div(
                dt.DataTable(
                    id='table-rec',
                    columns=[
                        {"name": i, "id": i} for i in sorted(df_rec.columns)
                    ],
                    page_current=0,
                    page_size=10,
                    page_action='custom',

                    filter_action='custom',
                    filter_query='',

                    sort_action='custom',
                    sort_mode='multi',
                    sort_by=[],
                ),
                style={'height': 450, 'overflowY': 'scroll'},
                className='m-3',
            ),
        ]
    )
    dialogs = html.Div(id='dialogs')

    return html.Div([location, navbar, page_content, dialogs])    

#app.layout = serve_layout(df)


operators = [['ge ', '>='],
             ['le ', '<='],
             ['lt ', '<'],
             ['gt ', '>'],
             ['ne ', '!='],
             ['eq ', '='],
             ['contains '],
             ['datestartswith ']]


def split_filter_part(filter_part):
    for operator_type in operators:
        for operator in operator_type:
            if operator in filter_part:
                name_part, value_part = filter_part.split(operator, 1)
                name = name_part[name_part.find('{') + 1: name_part.rfind('}')]

                value_part = value_part.strip()
                v0 = value_part[0]
                if (v0 == value_part[-1] and v0 in ("'", '"', '`')):
                    value = value_part[1: -1].replace('\\' + v0, v0)
                else:
                    try:
                        value = float(value_part)
                    except ValueError:
                        value = value_part

                # word operators need spaces after them in the filter string,
                # but we don't want these later
                return name, operator_type[0].strip(), value

    return [None] * 3


@app.callback(
    Output('table-paging-with-graph', "data"),
    Input('table-paging-with-graph', "page_current"),
    Input('table-paging-with-graph', "page_size"),
    Input('table-paging-with-graph', "sort_by"),
    Input('table-paging-with-graph', "filter_query"))

def update_table(page_current, page_size, sort_by, filter):
    filtering_expressions = filter.split(' && ')
    df = None
    dff = compute(df)
    for filter_part in filtering_expressions:
        col_name, operator, filter_value = split_filter_part(filter_part)

        if operator in ('eq', 'ne', 'lt', 'le', 'gt', 'ge'):
            # these operators match pandas series operator method names
            dff = dff.loc[getattr(dff[col_name], operator)(filter_value)]
        elif operator == 'contains':
            dff = dff.loc[dff[col_name].str.contains(filter_value)]
        elif operator == 'datestartswith':
            # this is a simplification of the front-end filtering logic,
            # only works with complete fields in standard format
            dff = dff.loc[dff[col_name].str.startswith(filter_value)]

    if len(sort_by):
        dff = dff.sort_values(
            [col['column_id'] for col in sort_by],
            ascending=[
                col['direction'] == 'asc'
                for col in sort_by
            ],
            inplace=False
        )

    return dff.iloc[
        page_current*page_size: (page_current + 1)*page_size
    ].to_dict('records')


@app.callback(
    Output('table-rec', "data"),
    Input('table-rec', "page_current"),
    Input('table-rec', "page_size"),
    Input('table-rec', "sort_by"),
    Input('table-rec', "filter_query"))

def update_table(page_current, page_size, sort_by, filter):
    filtering_expressions = filter.split(' && ')
    df = None
    dff = compute_rec(df)
    for filter_part in filtering_expressions:
        col_name, operator, filter_value = split_filter_part(filter_part)

        if operator in ('eq', 'ne', 'lt', 'le', 'gt', 'ge'):
            # these operators match pandas series operator method names
            dff = dff.loc[getattr(dff[col_name], operator)(filter_value)]
        elif operator == 'contains':
            dff = dff.loc[dff[col_name].str.contains(filter_value)]
        elif operator == 'datestartswith':
            # this is a simplification of the front-end filtering logic,
            # only works with complete fields in standard format
            dff = dff.loc[dff[col_name].str.startswith(filter_value)]

    if len(sort_by):
        dff = dff.sort_values(
            [col['column_id'] for col in sort_by],
            ascending=[
                col['direction'] == 'asc'
                for col in sort_by
            ],
            inplace=False
        )

    return dff.iloc[
        page_current*page_size: (page_current + 1)*page_size
    ].to_dict('records')


@app.callback(
    Output('table-paging-with-graph-container', "children"),
    Input('table-paging-with-graph', "data"))
def update_graph(rows):
    dff = pd.DataFrame(rows)
    return html.Div(
        [
            dcc.Graph(
                id=column,
                figure={
                    "data": [
                        {
                            "x": dff["date"],
                            "y": dff[column] if column in dff else [],
                            "type": "bar",
                            "marker": {"color": "#0074D9"},
                        }
                    ],
                    "layout": {
                        "xaxis": {"automargin": True},
                        "yaxis": {"automargin": True},
                        "height": 250,
                        "margin": {"t": 10, "l": 5, "r": 5},
                    },
                },
            )
            for column in ["account", "product", "quantity"]
        ]
    )

app.layout = serve_layout

if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8070)
