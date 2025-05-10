@echo off
cd /d "D:\Dashboard Logistik\streamlit-dashboard-logistik"

call venv\Scripts\activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
streamlit run main.py

pause