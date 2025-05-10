# import library
import streamlit  as st
import altair as alt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

# import library dari function.py
from etl import get_stock_move_line

# Konfigurasi tampilan
st.set_page_config(layout='wide', page_title='SAM Dashboard Logistic')
st.markdown("<h1 style='text-align: center;'>Dashboard Logistic</h1>", unsafe_allow_html=True)
st.write('')
st.write('')

# Inisialisasi hanya saat pertama kali  
if "date_input" not in st.session_state:
    st.session_state["date_input"] = datetime.today().date()

# Input layout
col1, col2, col3, col4, col5 = st.columns([0.4, 0.4, 0.4, 0.3, 2])
with col1:
    date_input = st.date_input("Tanggal", value=st.session_state["date_input"], format='DD/MM/YYYY')
    st.session_state['date_input'] = date_input

# Output area placeholder
output_placeholder = st.empty()

if date_input:
    with output_placeholder.container():
        # start time
        start_time = time.time()

        # Get data
        data = get_stock_move_line(date_input)
        stock_move_line = data['summary']
        stock_out_not_done = data['stok_out_not_done']
        stock_in_not_done = data['stok_in_not_done']

        date_input_name = str(date_input)
        stock_move_line.to_excel(f"./Datasets/Summary/stock_move_line {date_input_name}.xlsx", index=False)
        stock_in_not_done.to_excel(f"./Datasets/In Not Done/stock_move_line_in_not_done {date_input_name}.xlsx", index=False)
        stock_out_not_done.to_excel(f"./Datasets/Out Not Done/stock_move_line_out_not_done {date_input_name}.xlsx", index=False)

        # end time
        end_time = time.time()
        elapsed_time = np.round((end_time - start_time)/60, 2)
        
        # Display data
        st.write(f"Waktu yang dibutuhkan untuk mengambil data: {elapsed_time} menit")
        st.dataframe(stock_move_line)
else:
    None