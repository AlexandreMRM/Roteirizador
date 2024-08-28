from google.oauth2 import service_account
import gspread
import pandas as pd
import json

#nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
nome_credencial = "credencial.json"
with open(nome_credencial, 'r') as file:
    credencial = json.load(file)


credentials = service_account.Credentials.from_service_account_info(credencial)
scope = ['https://www.googleapis.com/auth/spreadsheets']
credentials = credentials.with_scopes(scope)
client = gspread.authorize(credentials)

# Abrir a planilha desejada pelo seu ID
spreadsheet = client.open_by_key('1vbGeqKyM4VSvHbMiyiqu1mkwEhneHi28e8cQ_lYMYhY')

# Selecionar a primeira planilha
sheet = spreadsheet.worksheet("BD")

sheet_data = sheet.get_all_values() # Carrega todos os valores que tem na planilha google e aba BD Geral

# Converter para um DataFrame pandas
df = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

print(df)