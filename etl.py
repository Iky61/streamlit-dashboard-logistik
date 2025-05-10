# import library and settings
import time
import pandas as pd
import numpy as np
import ssl
import pytz
import requests
from datetime import datetime, timedelta
import xmlrpc.client
from streamlit_gsheets import GSheetsConnection
import streamlit as st
import altair as alt
import warnings
warnings.filterwarnings('ignore')

from functions import SuportFunction, GetDataApi

# EXTRACTING DATA

# Get data products using Node3 API's
def get_product():
    field_db_product = ['id','default_code','type','display_name','categ_id','currency_id','rak','standard_price','uom_name','create_date','write_uid']
    db_product = GetDataApi.ApiOdoo(path='product.template', fields=field_db_product)
    db_product['uom_name'] = db_product['uom_name'].apply(lambda x: x.upper())
    db_product = db_product[db_product.type == 'product'][['display_name','uom_name','rak']].reset_index(drop=True).rename(columns={'display_name':'product_id'})
    return db_product

# Get Data maintenance Request
def get_stock_move_line(date, db_product=get_product()):

    # transform date to datetime
    date = pd.to_datetime(date)

    # get data stock move line using Node3 API's
    field_move_line = ['product_id','product_uom_id','date','reference','location_id','location_dest_id','state','product_qty','product_uom_qty','qty_done']
    stock_move_line = GetDataApi.ApiOdoo(path='stock.move.line', fields=field_move_line)

    # Cleaning Data String
    for i in ['product_id','product_uom_id','location_id','location_dest_id']:
        stock_move_line[i] = stock_move_line[i].apply(SuportFunction.transform_last_data_from_list)

    # Uppercase uom name
    stock_move_line['product_uom_id'] = stock_move_line['product_uom_id'].apply(lambda x: x.upper())

    # Transform data
    stock_move_line['datetime'] = pd.to_datetime(stock_move_line['date'])
    stock_move_line['date'] = pd.to_datetime(stock_move_line['datetime']).dt.date
    stock_move_line['date'] = pd.to_datetime(stock_move_line['date'])

    # In & Out Stok MRLW state not DONE
    stok_mrwl_ou_not_done = stock_move_line[(stock_move_line.location_id.apply(lambda x: 'MRWL' in x)) & (stock_move_line.state.isin(['done']) == False)]
    stok_mrwl_in_not_done = stock_move_line[
        (stock_move_line.location_id.apply(lambda x: 'MRWL' not in x)) & 
        (stock_move_line.location_dest_id.isin(['MRWL/Stock'])) & 
        (stock_move_line.state.isin(['done','cancel']) == False)
    ]

    # In & Out Stok MRLW
    stok_mrwl_out = stock_move_line[(stock_move_line.location_id.apply(lambda x: 'MRWL' in x)) & (stock_move_line.state.isin(['cancel']) == False)]
    stok_mrwl_in = stock_move_line[
        (stock_move_line.location_id.apply(lambda x: 'MRWL' not in x)) &
        (stock_move_line.location_dest_id.isin(['MRWL/Stock'])) &
        (stock_move_line.state.isin(['done','cancel']) == False)
    ]

    # Out Before
    stok_mrwl_out_before = stok_mrwl_out[stok_mrwl_out.date < date]
    stok_mrwl_out_before = stok_mrwl_out_before.groupby(['product_id']).agg({
        'reference':SuportFunction.join_to_array,
        'qty_done':np.sum
    }).rename(columns={'qty_done':'qty_out_before','reference':'reference_out_before'}).reset_index()

    # In Before
    stok_mrwl_in_before = stok_mrwl_in[stok_mrwl_in.date < date]
    stok_mrwl_in_before = stok_mrwl_in_before.groupby(['product_id']).agg({
        'reference':SuportFunction.join_to_array,
        'qty_done':np.sum
    }).rename(columns={'qty_done':'qty_in_before','reference':'reference_in_before'}).reset_index()

    # Out Now
    stok_mrwl_out_now = stok_mrwl_out[stok_mrwl_out.date == date]
    stok_mrwl_out_now = stok_mrwl_out_now.groupby(['product_id']).agg({
        'reference':SuportFunction.join_to_array,
        'qty_done':np.sum
    }).rename(columns={'qty_done':'qty_out_now','reference':'reference_out_now'}).reset_index()

    # In Now
    stok_mrwl_in_now = stok_mrwl_in[stok_mrwl_in.date == date]
    stok_mrwl_in_now = stok_mrwl_in_now.groupby(['product_id']).agg({
        'reference':SuportFunction.join_to_array,
        'qty_done':np.sum
    }).rename(columns={'qty_done':'qty_in_now','reference':'reference_in_now'}).reset_index()

    # merge data
    msg = db_product.merge(stok_mrwl_out_before, on='product_id', how='left')
    msg = msg.merge(stok_mrwl_in_before, on='product_id', how='left')
    msg = msg.merge(stok_mrwl_out_now, on='product_id', how='left')
    msg = msg.merge(stok_mrwl_in_now, on='product_id', how='left')

    # cleaning data
    for i in ['qty_out_before','qty_in_before','qty_out_now','qty_in_now']:
        msg[i].fillna(0, inplace=True)

    for i in ['reference_out_before','reference_in_before','reference_in_now','reference_out_now']:
        msg[i].fillna('', inplace=True)
        msg[i] = msg[i].apply(lambda x: ','.join(x))
        msg[i] = msg[i].apply(lambda x: x.split(','))

    # transform data
    msg['saldo_awal'] = msg.qty_in_before - msg.qty_out_before
    msg['in'] = msg.qty_in_now
    msg['out'] = msg.qty_out_now
    msg['saldo_akhir'] = (msg['saldo_awal'] + msg['in']) - msg['out']

    # recolumns data
    msg = msg[['product_id','uom_name','rak','reference_in_before','reference_out_before','reference_in_now','reference_out_now','saldo_awal','in','out','saldo_akhir']]
    msg = msg.rename(columns={
        'product_id':'Product ID',
        'uom_name':'UOM',
        'rak':'Rak',
        'reference_in_before':'Reference In Before',
        'reference_out_before':'Reference Out Before',
        'reference_in_now':'Reference In Now',
        'reference_out_now':'Reference Out Now',
        'saldo_awal':'Saldo Awal',
        'in':'In',
        'out':'Out',
        'saldo_akhir':'Saldo Akhir'
    })

    msg = msg[msg['Saldo Akhir'] != 0].reset_index(drop=True)

    # return data
    return {'summary': msg, 'stok_out_not_done':stok_mrwl_ou_not_done, 'stok_in_not_done':stok_mrwl_in_not_done}

