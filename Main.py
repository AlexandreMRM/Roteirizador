import streamlit as st
import datetime
import pandas as pd
import mysql.connector
import decimal
from datetime import timedelta
from google.oauth2 import service_account
import gspread 

def adicionar_juncao(voo, horario_voo, juncao):
    nova_linha = pd.DataFrame([[voo, horario_voo, juncao]], columns=['Voo', 'Horário', 'Junção'])
    st.session_state.df_juncao_voos = pd.concat([st.session_state.df_juncao_voos, nova_linha], ignore_index=True)

def gerar_df_phoenix(vw_name):
    # Parametros de Login AWS
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': 'test_phoenix_joao_pessoa'
    }
    # Conexão as Views
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT * FROM {vw_name}'

    # Script MySql para requests
    cursor.execute(
        request_name
    )
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas o cabecalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e muda o tipo de decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def transformar_timedelta(intervalo):
    
    intervalo = timedelta(hours=intervalo.hour, minutes=intervalo.minute, seconds=intervalo.second)

    return intervalo

def definir_horario_primeiro_hotel(df, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg):

    servico = df.at[index, 'Servico']

    data_voo = df.at[index, 'Data Voo']

    juncao = df.at[index, 'Junção']

    modo = df.at[index, 'Modo do Servico']

    if pd.isna(juncao) or modo!='REGULAR':

        hora_voo = df.at[index, 'Horario Voo']

    else:

        hora_voo = df.at[index, 'Menor Horário']

    data_hora_voo_str = f'{data_voo} {hora_voo}'

    data_hora_voo = pd.to_datetime(data_hora_voo_str, format='%Y-%m-%d %H:%M:%S')

    if servico=='HOTÉIS JOÃO PESSOA / AEROPORTO JOÃO PESSOA':

        data_hora_primeiro_hotel=data_hora_voo - intervalo_inicial

    elif servico=='HOTÉIS PITIMBU / AEROPORTO JOÃO PESSOA':

        data_hora_primeiro_hotel=data_hora_voo - intervalo_inicial - ajuste_pitimbu

    elif servico == 'HOTÉIS CAMPINA GRANDE / AEROPORTO JOÃO PESSOA' or servico=='HOTÉIS JOÃO PESSOA / AEROPORTO RECIFE' or \
        servico=='HOTÉIS PITIMBU / AEROPORTO RECIFE':

        data_hora_primeiro_hotel=data_hora_voo - intervalo_inicial - ajuste_rec_cg

    elif servico=='HOTEL CAMPINA GRANDE / AEROPORTO CAMPINA GRANDE':

        data_hora_primeiro_hotel=data_hora_voo - intervalo_inicial + (ajuste_pitimbu*1.5)

    return data_hora_primeiro_hotel

def definir_intervalo_ref(df, value, intervalo_bairros_iguais, intervalo_bairros_diferentes):

    if (bairro==df.at[value-1, 'Micro Região']) or \
        (bairro=='MANAIRA 1' and df.at[value-1, 'Micro Região']=='TAMBAU'):

        intervalo_ref=intervalo_bairros_iguais

    elif bairro=='CENTRO':

        intervalo_ref = intervalo_bairros_diferentes*2

    else:

        intervalo_ref = intervalo_bairros_diferentes

    return intervalo_ref

def abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                        paxs_hotel):

    mais_sugestao = 1

    carros+=1

    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=\
    definir_horario_primeiro_hotel(df_router_filtrado_2, index, intervalo_inicial, ajuste_pitimbu, 
                                                    ajuste_rec_cg)

    data_horario_primeiro_hotel = df_router_filtrado_2.at[value, 'Data Horario Apresentacao']

    paxs_total_roteiro = 0

    bairro = ''

    paxs_total_roteiro+=paxs_hotel

    df_router_filtrado_2.at[value, 'Roteiro'] = roteiro

    df_router_filtrado_2.at[value, 'Carros'] = carros

    return mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro

def preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value):

    df_router_filtrado_2.at[value, 'Roteiro'] = roteiro

    df_router_filtrado_2.at[value, 'Carros'] = carros

    return df_router_filtrado_2

st.set_page_config(layout='wide')

st.title('Roteirizador de Transfer Out')

st.divider()

st.header('Parâmetros')

if not 'df_router' in st.session_state:

    st.session_state.df_router = gerar_df_phoenix('vw_router')

if 'df_hoteis' not in st.session_state:

######### Carregar Dados Google Sheets ########## ALEXANDRE MAGNO

    nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
    credentials = service_account.Credentials.from_service_account_info(nome_credencial)
    scope = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(scope)
    client = gspread.authorize(credentials)

    # Abrir a planilha desejada pelo seu ID
    spreadsheet = client.open_by_key('1vbGeqKyM4VSvHbMiyiqu1mkwEhneHi28e8cQ_lYMYhY')

    # Selecionar a primeira planilha
    sheet = spreadsheet.worksheet("BD")

    sheet_data = sheet.get_all_values()

    st.session_state.df_hoteis = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

    ############################################################

row1 = st.columns(3)

with row1[0]:

    intervalo_inicial = st.time_input('Intervalo Inicial', value=datetime.time(3, 30), help='Intervalo de tempo entre o primeiro hotel e o voo', 
                                      key='intervalo_inicial')
    
    intervalo_inicial = transformar_timedelta(intervalo_inicial)
    
    intervalo_pu_hotel = st.time_input('Intervalo Hoteis | Primeiro vs Último', value=datetime.time(0, 45), 
                                       help='Intervalo de tempo entre o primeiro hotel e o último hotel do carro', key='intervalo_pu_hotel', step=300)
    
    intervalo_pu_hotel = transformar_timedelta(intervalo_pu_hotel)
    
    ajuste_rec_cg = st.time_input('Intervalo Ajuste | Recife ou Campina Grande', value=datetime.time(2, 0), 
                                  help='Intervalo p/ somar ao intervalo inicial quando a origem for de Recife ou Campina Grande', key='ajuste_rec_cg')

    ajuste_rec_cg = transformar_timedelta(ajuste_rec_cg)

with row1[1]:

    intervalo_bairros_iguais = st.time_input('Intervalo Hoteis | Bairros Iguais', value=datetime.time(0, 5), 
                                             help='Intervalo de tempo entre hoteis de mesmo bairro', key='intervalo_bairros_iguais')
    
    intervalo_bairros_iguais = transformar_timedelta(intervalo_bairros_iguais)
    
    intervalo_bairros_diferentes = st.time_input('Intervalo Hoteis | Bairros Diferentes', value=datetime.time(0, 10), 
                                                 help='Intervalo de tempo entre hoteis de bairros diferentes', key='intervalo_bairros_diferentes')
    
    intervalo_bairros_diferentes = transformar_timedelta(intervalo_bairros_diferentes)
    
    ajuste_pitimbu = st.time_input('Intervalo Ajuste | Pitimbú', value=datetime.time(1, 0), 
                                   help='Intervalo p/ somar ao intervalo inicial quando a origem for de Pitimbú', key='ajuste_pitimbu')
    
    ajuste_pitimbu = transformar_timedelta(ajuste_pitimbu)
    
with row1[2]:

    max_hoteis = st.number_input('Máximo de Hoteis por Carro', step=1, value=8, key='max_hoteis')

    pax_cinco_min = st.number_input('Paxs Extras', step=1, value=18, key='pax_cinco_min', help='Número de paxs para aumentar intervalo entre hoteis em 5 minutos')

    pax_max = st.number_input('Máximo de Paxs por Carro', step=1, value=46, key='pax_max')

st.divider()

st.header('Juntar Voos')

if 'df_juncao_voos' not in st.session_state:

    st.session_state.df_juncao_voos = pd.DataFrame(columns=['Voo', 'Horário', 'Junção'])

row2 = st.columns(3)

with row2[0]:

    row2_1=st.columns(2)

    with row2_1[0]:

        atualizar_hoteis = st.button('Atualizar Sequência de Hoteis')

    with row2_1[1]:

        atualizar_phoenix = st.button('Atualizar Dados Phoenix')

        if atualizar_phoenix:

            st.session_state.df_router = gerar_df_phoenix('vw_router')

    if atualizar_hoteis:
    ######### Carregar Dados Google Sheets ########## ALEXANDRE MAGNO

        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
        client = gspread.authorize(credentials)

        # Abrir a planilha desejada pelo seu ID
        spreadsheet = client.open_by_key('1vbGeqKyM4VSvHbMiyiqu1mkwEhneHi28e8cQ_lYMYhY')

        # Selecionar a primeira planilha
        sheet = spreadsheet.worksheet("BD")

        sheet_data = sheet.get_all_values()

        st.session_state.df_hoteis = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])
        #st.dataframe(st.session_state.df_hoteis)

    ############################################################

    container_roteirizar = st.container(border=True)

    data_roteiro = container_roteirizar.date_input('Data do Roteiro', value=None, format='DD/MM/YYYY', key='data_roteiro')

    row_container = container_roteirizar.columns(2)

    with row_container[0]:

        roteirizar = st.button('Roteirizar')

    with row_container[1]:

        visualizar_voos = st.button('Visualizar Voos')

if visualizar_voos:

    df_router_filtrado = st.session_state.df_router[(st.session_state.df_router['Data Execucao']==data_roteiro) & 
                                                    (st.session_state.df_router['Tipo de Servico']=='OUT') & 
                                                    (st.session_state.df_router['Status do Servico']!='CANCELADO')].reset_index(drop=True)
    
    st.session_state.df_servico_voos_horarios = df_router_filtrado[['Servico', 'Voo', 'Horario Voo']].sort_values(by=['Servico', 'Horario Voo'])\
        .drop_duplicates().reset_index(drop=True)
    
    st.session_state.df_servico_voos_horarios['Horario Voo'] = pd.to_datetime(st.session_state.df_servico_voos_horarios['Horario Voo'], format='%H:%M:%S').dt.time

    with row2[0]:

        st.dataframe(st.session_state.df_servico_voos_horarios, hide_index=True)

with row2[1]:

    with st.form('juntar_voos_form_novo'):

        horario_inicial = st.time_input('Horário Voo', value=None, key='horario_inicial', step=300)

        horario_final = st.time_input('Horário Voo', value=None, key='horario_final', step=300)

        if horario_inicial and horario_final:

            df_voos_hi_hf = st.session_state.df_servico_voos_horarios[(st.session_state.df_servico_voos_horarios['Horario Voo']>=horario_inicial) & 
                                                                    (st.session_state.df_servico_voos_horarios['Horario Voo']<=horario_final) &
                                                                    (st.session_state.df_servico_voos_horarios['Voo']!='G3 - 0001')][['Voo', 'Horario Voo']]\
                                                                        .reset_index(drop=True)
            
            df_voos_hi_hf = df_voos_hi_hf.rename(columns={'Horario Voo': 'Horário'})
        
            if len(st.session_state.df_juncao_voos)>0:

                juncao_max = st.session_state.df_juncao_voos['Junção'].max()

                df_voos_hi_hf['Junção'] = juncao_max+1

            else:

                df_voos_hi_hf['Junção'] = 1

        lancar_juncao = st.form_submit_button('Lançar Junção')

        if lancar_juncao:

            st.session_state.df_juncao_voos = pd.concat([st.session_state.df_juncao_voos, df_voos_hi_hf], ignore_index=True)

            with row2[0]:

                st.dataframe(st.session_state.df_servico_voos_horarios, hide_index=True)

    # Formulário antigo pra adicionar junções

    # with st.form('juntar_voos_form'):

    #     voo = st.text_input('Voo', value=None, key='voo')

    #     if 'df_servico_voos_horarios' in st.session_state and voo:

    #         horario_default = st.session_state.df_servico_voos_horarios.loc[st.session_state.df_servico_voos_horarios['Voo'] == voo, 'Horario Voo'].squeeze()

    #         if len(horario_default)>1 and len(horario_default)!=8:

    #             horario_default = pd.to_datetime(horario_default.iloc[0]).time()

    #         else:

    #             horario_default = pd.to_datetime(horario_default).time()

    #         horario_voo = st.time_input('Horário Voo', value=horario_default, key='horario_voo', step=300)

    #     else:

    #         horario_voo = st.time_input('Horário Voo', value=None, key='horario_voo', step=300)

    #     juncao = st.number_input('Junção', step=1, value=None, key='juncao')

    #     lancar_juncao = st.form_submit_button('Lançar Junção')

    #     if lancar_juncao:

    #         adicionar_juncao(voo, horario_voo, juncao)

    #         with row2[2]:

    #             st.dataframe(st.session_state.df_servico_voos_horarios, hide_index=True)

with row2[2]:

    limpar_juncoes = st.button('Limpar Junções')

    if limpar_juncoes:

        voo=None

        st.session_state.df_juncao_voos = pd.DataFrame(columns=['Voo', 'Horário', 'Junção'])

    st.dataframe(st.session_state.df_juncao_voos, hide_index=True)





















if roteirizar:

    # Filtrando apenas data especificada, OUTs e Status do Serviço diferente de 'CANCELADO'

    df_router_filtrado = st.session_state.df_router[(st.session_state.df_router['Data Execucao']==data_roteiro) & 
                                                    (st.session_state.df_router['Tipo de Servico']=='OUT') & 
                                                    (st.session_state.df_router['Status do Servico']!='CANCELADO')].reset_index(drop=True)
    
# Verificando se todos os hoteis estão na lista da sequência

    lista_hoteis_df_router = df_router_filtrado['Est Origem'].unique().tolist()

    lista_hoteis_sequencia = st.session_state.df_hoteis['Est Origem'].unique().tolist()

    itens_faltantes = set(lista_hoteis_df_router) - set(lista_hoteis_sequencia)

    itens_faltantes = list(itens_faltantes)

# Se o único hotel não cadastrado for 'SEM HOTEL ' ou se todos os hoteis estiverem cadastrados

    if len(itens_faltantes)==0:

        # Mensagens de andamento do script informando como foi a verificação dos hoteis cadastrados

        if 'SEM HOTEL ' in lista_hoteis_df_router:
        
            st.dataframe(df_router_filtrado[df_router_filtrado['Est Origem']=='SEM HOTEL '].reset_index(drop=True), hide_index=True)

            st.warning("Os serviços acima estão cadastrados no Phoenix com origem 'SEM HOTEL'. O estabelecimento 'SEM HOTEL' será considerado o último do roteiro")

            # df_router_filtrado = df_router_filtrado[df_router_filtrado['Est Origem']!='SEM HOTEL '].reset_index(drop=True)

        else:

            st.success('Todos os hoteis estão cadastrados na lista de sequência de hoteis')

        # Criando coluna de paxs totais

        df_router_filtrado['Total ADT | CHD'] = df_router_filtrado['Total ADT'] + df_router_filtrado['Total CHD']

        # Criando coluna de Junção através de pd.merge

        df_router_filtrado_2 = pd.merge(df_router_filtrado, st.session_state.df_juncao_voos[['Voo', 'Junção']], on='Voo', how='left')

        # Criando colunas Micro Região e Sequência através de pd.merge

        df_router_filtrado_2 = pd.merge(df_router_filtrado_2, st.session_state.df_hoteis, on='Est Origem', how='left')

        # Ordenando dataframe por ['Modo do Servico', 'Servico', 'Junção', 'Voo', 'Sequência']

        df_router_filtrado_2 = df_router_filtrado_2.sort_values(by=['Modo do Servico', 'Servico', 'Junção', 'Voo', 'Sequência']).reset_index(drop=True)

        # Ordenando cada junção pela sequência de hoteis

        max_juncao = df_router_filtrado_2['Junção'].dropna().max()

        if pd.isna(max_juncao):

            max_juncao = 0

        for juncao in range(1, int(max_juncao) + 1):

            df_ref = df_router_filtrado_2[(df_router_filtrado_2['Modo do Servico']=='REGULAR') & (df_router_filtrado_2['Junção']==juncao)]\
                .sort_values(by='Sequência').reset_index()

            index_inicial = df_ref['index'].min()

            index_final = df_ref['index'].max()

            df_ref = df_ref.drop('index', axis=1)

            df_router_filtrado_2.iloc[index_inicial:index_final+1] = df_ref

        # Colocando qual o menor horário de cada junção

        df_menor_horario = pd.DataFrame(columns=['Junção', 'Menor Horário'])

        contador=0

        for juncao in st.session_state.df_juncao_voos['Junção'].unique().tolist():

            menor_horario = st.session_state.df_juncao_voos[st.session_state.df_juncao_voos['Junção']==juncao]['Horário'].min()

            df_menor_horario.at[contador, 'Junção']=juncao

            df_menor_horario.at[contador, 'Menor Horário']=menor_horario

            contador+=1

        df_router_filtrado_2 = pd.merge(df_router_filtrado_2, df_menor_horario, on='Junção', how='left')

        df_router_filtrado_2['Roteiro']=0

        df_router_filtrado_2['Carros']=0

        roteiro = 0

        lista_colunas = ['index']

        df_hoteis_pax_max = pd.DataFrame(columns=lista_colunas.extend(df_router_filtrado_2.columns.tolist()))























        # Roteirizando hoteis com mais paxs que a capacidade máxima da frota

        df_ref_sem_juncao = df_router_filtrado_2[(pd.isna(df_router_filtrado_2['Junção']))].groupby(['Modo do Servico', 'Servico', 'Voo', 'Est Origem'])\
            .agg({'Total ADT | CHD': 'sum'}).reset_index()

        df_ref_sem_juncao = df_ref_sem_juncao[df_ref_sem_juncao['Total ADT | CHD']>=pax_max].reset_index()

        df_ref_com_juncao = df_router_filtrado_2[~(pd.isna(df_router_filtrado_2['Junção']))].groupby(['Modo do Servico', 'Servico', 'Junção', 'Est Origem'])\
            .agg({'Total ADT | CHD': 'sum'}).reset_index()

        df_ref_com_juncao = df_ref_com_juncao[df_ref_com_juncao['Total ADT | CHD']>=pax_max].reset_index()

        if len(df_ref_com_juncao)>0:

            for index in range(len(df_ref_com_juncao)):

                carro=0

                roteiro+=1

                loops = int(df_ref_com_juncao.at[index, 'Total ADT | CHD']//pax_max)

                modo = df_ref_com_juncao.at[index, 'Modo do Servico']

                servico = df_ref_com_juncao.at[index, 'Servico']

                ref_juncao = df_ref_com_juncao.at[index, 'Junção']

                hotel = df_ref_com_juncao.at[index, 'Est Origem']

                for loop in range(loops):

                    carro+=1

                    df_hotel_pax_max = df_router_filtrado_2[(df_router_filtrado_2['Modo do Servico']==modo) & (df_router_filtrado_2['Servico']==servico) & 
                                                            (df_router_filtrado_2['Junção']==ref_juncao) & (df_router_filtrado_2['Est Origem']==hotel)]\
                                                                .reset_index()
                    
                    paxs_total_ref = 0
                    
                    for index_2, value in df_hotel_pax_max['Total ADT | CHD'].items():

                        if paxs_total_ref+value>pax_max:

                            break

                        else:

                            paxs_total_ref+=value

                            df_router_filtrado_2 = df_router_filtrado_2.drop(index=df_hotel_pax_max.at[index_2, 'index'])

                            df_hoteis_pax_max = pd.concat([df_hoteis_pax_max, df_hotel_pax_max.loc[[index_2]]])

                            df_hoteis_pax_max.at[index_2, 'Roteiro']=roteiro

                            df_hoteis_pax_max.at[index_2, 'Carros']=carro          

        if len(df_ref_sem_juncao)>0:

            for index in range(len(df_ref_sem_juncao)):

                carro=0

                roteiro+=1

                loops = int(df_ref_sem_juncao.at[index, 'Total ADT | CHD']//pax_max)

                modo = df_ref_sem_juncao.at[index, 'Modo do Servico']

                servico = df_ref_sem_juncao.at[index, 'Servico']

                ref_voo = df_ref_sem_juncao.at[index, 'Voo']

                hotel = df_ref_sem_juncao.at[index, 'Est Origem']

                for loop in range(loops):

                    carro+=1

                    df_hotel_pax_max = df_router_filtrado_2[(df_router_filtrado_2['Modo do Servico']==modo) & (df_router_filtrado_2['Servico']==servico) & 
                                                            (df_router_filtrado_2['Voo']==ref_voo) & (df_router_filtrado_2['Est Origem']==hotel)]\
                                                                .reset_index()
                    
                    paxs_total_ref = 0
                    
                    for index_2, value in df_hotel_pax_max['Total ADT | CHD'].items():

                        if paxs_total_ref+value>pax_max:

                            break

                        else:

                            paxs_total_ref+=value

                            df_router_filtrado_2 = df_router_filtrado_2.drop(index=df_hotel_pax_max.at[index_2, 'index'])

                            df_hoteis_pax_max = pd.concat([df_hoteis_pax_max, df_hotel_pax_max.loc[[index_2]]])

                            df_hoteis_pax_max.at[index_2, 'Roteiro']=roteiro

                            df_hoteis_pax_max.at[index_2, 'Carros']=carro

        for index in range(len(df_hoteis_pax_max)):

            df_hoteis_pax_max.at[index, 'Data Horario Apresentacao']=\
                                            definir_horario_primeiro_hotel(df_hoteis_pax_max, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg)

        df_router_filtrado_2 = df_router_filtrado_2.reset_index(drop=True)






















        # Gerando horários de apresentação

        for index in range(len(df_router_filtrado_2)):

            # Se o serviço for privativo

            if df_router_filtrado_2.at[index, 'Modo do Servico']=='PRIVATIVO POR VEICULO' or \
                df_router_filtrado_2.at[index, 'Modo do Servico']=='PRIVATIVO POR PESSOA':

                roteiro+=1

                df_router_filtrado_2.at[index, 'Data Horario Apresentacao']=\
                    definir_horario_primeiro_hotel(df_router_filtrado_2, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg)
                
                df_router_filtrado_2.at[index, 'Roteiro'] = roteiro
                
                df_router_filtrado_2.at[index, 'Carros'] = 1

            # Se o serviço não for privativo

            elif df_router_filtrado_2.at[index, 'Modo do Servico']=='REGULAR':

                juntar = df_router_filtrado_2.at[index, 'Junção']

                voo = df_router_filtrado_2.at[index, 'Voo']

                # Se o voo não estiver em alguma junção

                if pd.isna(juntar):

                    servico = df_router_filtrado_2.at[index, 'Servico']

                    mais_sugestao=0

                    df_ref = df_router_filtrado_2[(df_router_filtrado_2['Modo do Servico']=='REGULAR') & (df_router_filtrado_2['Voo']==voo) & 
                                                  (df_router_filtrado_2['Servico']==servico)].reset_index()

                    index_inicial = df_ref['index'].min()

                    hoteis_mesmo_voo = len(df_ref['Est Origem'].unique().tolist())

                    if index==index_inicial:

                        # Se no voo não tiver mais que o número máximo de hoteis permitidos por carro

                        if hoteis_mesmo_voo<=max_hoteis:

                            roteiro+=1

                            carros = 1

                            paxs_total_roteiro = 0

                            bairro = ''

                            # Loop no voo para colocar os horários

                            for index_2, value in df_ref['index'].items():

                                # Se for o primeiro hotel do voo, define o horário inicial, colhe o horário do hotel e inicia somatório de paxs do roteiro

                                if value==index_inicial:

                                    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=\
                                        definir_horario_primeiro_hotel(df_router_filtrado_2, value, intervalo_inicial, ajuste_pitimbu, 
                                                                                        ajuste_rec_cg)
                                    
                                    data_horario_primeiro_hotel = df_router_filtrado_2.at[value, 'Data Horario Apresentacao']
                                    
                                    paxs_hotel = df_ref[df_ref['Est Origem']==df_router_filtrado_2.at[value, 'Est Origem']]['Total ADT | CHD'].sum()

                                    paxs_total_roteiro+=paxs_hotel

                                    df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                                # Se não for a primeira linha do voo, mas o hotel for igual o hotel anterior, só repete o horário de apresentação

                                elif df_router_filtrado_2.at[value, 'Est Origem']==df_router_filtrado_2.at[value-1, 'Est Origem']:

                                    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=df_router_filtrado_2.at[value-1, 'Data Horario Apresentacao']

                                    df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                                # Se não for a primeira linha do voo e o hotel não for igual ao anterior

                                else:

                                    # Colhe a quantidade de paxs do hotel anterior, o bairro do hotel atual, a quantidade de paxs do hotel atual 
                                    # e verifica se estoura a capacidade máxima de um carro

                                    paxs_hotel_anterior = paxs_hotel

                                    bairro=df_router_filtrado_2.at[value, 'Micro Região']

                                    paxs_hotel = df_ref[df_ref['Est Origem']==df_router_filtrado_2.at[value, 'Est Origem']]['Total ADT | CHD'].sum()

                                    # Se estourar a capacidade do carro, aí trata como se fosse o primeiro hotel e adiciona 1 na variável carros
                                    # pra, no final, eu saber quantos carros foram usados nesse roteiro e poder dividir 'igualmente' a quantidade de hoteis

                                    if paxs_total_roteiro+paxs_hotel>pax_max:

                                        mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                            abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                paxs_hotel)

                                    # Se não estourar a capacidade máxima

                                    else:

                                        paxs_total_roteiro+=paxs_hotel

                                        # Sempre que inicia um carro, o bairro fica vazio. Portanto, se não for o primeiro hotel do carro, vai definir a variavel
                                        # intervalo_ref pra o robô saber quantos minutos deve adicionar até o próximo horário de apresentação

                                        if bairro!='':

                                            intervalo_ref = definir_intervalo_ref(df_router_filtrado_2, value, intervalo_bairros_iguais, 
                                                                                  intervalo_bairros_diferentes)

                                        # Se no hotel anterior tiver muitos paxs, vai adicionar mais 5 minutos ao intervalo

                                        if paxs_hotel_anterior>=pax_cinco_min:

                                            intervalo_ref+=intervalo_bairros_iguais

                                        data_horario_hotel = df_router_filtrado_2.at[value-1, 'Data Horario Apresentacao']+intervalo_ref

                                        if data_horario_hotel - data_horario_primeiro_hotel>intervalo_pu_hotel:

                                            mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                                abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                paxs_hotel)

                                        else:

                                            df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=data_horario_hotel

                                            df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)
                        
                        # Se no voo tiver mais que o número máximo de hoteis permitidos por carro

                        else:

                            roteiro+=1

                            carros = 1

                            paxs_total_roteiro = 0

                            contador_hoteis = 0

                            bairro = ''

                            mais_sugestao=1

                            # Loop no voo para colocar os horários

                            for index_2, value in df_ref['index'].items():

                                # Se for o primeiro hotel do voo, define o horário inicial, colhe o horário do hotel e inicia somatório de paxs do roteiro

                                if value==index_inicial:

                                    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=\
                                        definir_horario_primeiro_hotel(df_router_filtrado_2, value, intervalo_inicial, ajuste_pitimbu, 
                                                                                        ajuste_rec_cg)
                                    
                                    data_horario_primeiro_hotel = df_router_filtrado_2.at[value, 'Data Horario Apresentacao']
                                    
                                    paxs_hotel = df_ref[df_ref['Est Origem']==df_router_filtrado_2.at[value, 'Est Origem']]['Total ADT | CHD'].sum()

                                    paxs_total_roteiro+=paxs_hotel

                                    df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                                    contador_hoteis+=1

                                # Se não for a primeira linha do voo, mas o hotel for igual o hotel anterior, só repete o horário de apresentação

                                elif df_router_filtrado_2.at[value, 'Est Origem']==df_router_filtrado_2.at[value-1, 'Est Origem']:

                                    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=df_router_filtrado_2.at[value-1, 'Data Horario Apresentacao']

                                    df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                                # Se não for a primeira linha do voo e o hotel não for igual ao anterior

                                else:

                                    # Colhe a quantidade de paxs do hotel anterior, o bairro do hotel atual, a quantidade de paxs do hotel atual 
                                    # e verifica se estoura a capacidade máxima de um carro

                                    contador_hoteis+=1

                                    paxs_hotel_anterior = paxs_hotel

                                    bairro=df_router_filtrado_2.at[value, 'Micro Região']

                                    paxs_hotel = df_ref[df_ref['Est Origem']==df_router_filtrado_2.at[value, 'Est Origem']]['Total ADT | CHD'].sum()

                                    if contador_hoteis>max_hoteis:

                                        mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                            abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                paxs_hotel)
                                        
                                        contador_hoteis = 1
                                        
                                    else:

                                        # Se estourar a capacidade do carro, aí trata como se fosse o primeiro hotel e adiciona 1 na variável carros
                                        # pra, no final, eu saber quantos carros foram usados nesse roteiro e poder dividir 'igualmente' a quantidade de hoteis

                                        if paxs_total_roteiro+paxs_hotel>pax_max:

                                            mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                                abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                    paxs_hotel)
                                            
                                            contador_hoteis = 1

                                        # Se não estourar a capacidade máxima

                                        else:

                                            paxs_total_roteiro+=paxs_hotel

                                            # Sempre que inicia um carro, o bairro fica vazio. Portanto, se não for o primeiro hotel do carro, vai definir a variavel
                                            # intervalo_ref pra o robô saber quantos minutos deve adicionar até o próximo horário de apresentação

                                            if bairro!='':

                                                intervalo_ref = definir_intervalo_ref(df_router_filtrado_2, value, intervalo_bairros_iguais, 
                                                                                      intervalo_bairros_diferentes)

                                            # Se no hotel anterior tiver muitos paxs, vai adicionar mais 5 minutos ao intervalo

                                            if paxs_hotel_anterior>=pax_cinco_min:

                                                intervalo_ref+=intervalo_bairros_iguais

                                            data_horario_hotel = df_router_filtrado_2.at[value-1, 'Data Horario Apresentacao']+intervalo_ref

                                            if data_horario_hotel - data_horario_primeiro_hotel>intervalo_pu_hotel:

                                                mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                                    abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                    paxs_hotel)
                                                
                                                contador_hoteis = 1

                                            else:

                                                df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=data_horario_hotel

                                                df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                # Se o voo estiver em alguma junção

                else:

                    servico = df_router_filtrado_2.at[index, 'Servico']

                    mais_sugestao=0

                    df_ref = df_router_filtrado_2[(df_router_filtrado_2['Modo do Servico']=='REGULAR') & (df_router_filtrado_2['Junção']==juntar) & 
                                                  (df_router_filtrado_2['Servico']==servico)].reset_index()

                    index_inicial = df_ref['index'].min()

                    hoteis_mesma_juncao = len(df_ref['Est Origem'].unique().tolist())

                    if index==index_inicial:

                        if hoteis_mesma_juncao<=max_hoteis:

                            roteiro+=1

                            carros = 1

                            paxs_total_roteiro = 0

                            bairro = ''

                            # Loop no voo para colocar os horários

                            for index_2, value in df_ref['index'].items():

                                # Se for o primeiro hotel do voo, define o horário inicial, colhe o horário do hotel e inicia somatório de paxs do roteiro

                                if value==index_inicial:

                                    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=\
                                        definir_horario_primeiro_hotel(df_router_filtrado_2, value, intervalo_inicial, ajuste_pitimbu, 
                                                                                        ajuste_rec_cg)
                                    
                                    data_horario_primeiro_hotel = df_router_filtrado_2.at[value, 'Data Horario Apresentacao']
                                    
                                    paxs_hotel = df_ref[df_ref['Est Origem']==df_router_filtrado_2.at[value, 'Est Origem']]['Total ADT | CHD'].sum()

                                    paxs_total_roteiro+=paxs_hotel

                                    df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                                # Se não for a primeira linha do voo, mas o hotel for igual o hotel anterior, só repete o horário de apresentação

                                elif df_router_filtrado_2.at[value, 'Est Origem']==df_router_filtrado_2.at[value-1, 'Est Origem']:

                                    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=df_router_filtrado_2.at[value-1, 'Data Horario Apresentacao']

                                    df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                                # Se não for a primeira linha do voo e o hotel não for igual ao anterior

                                else:

                                    # Colhe a quantidade de paxs do hotel anterior, o bairro do hotel atual, a quantidade de paxs do hotel atual 
                                    # e verifica se estoura a capacidade máxima de um carro

                                    paxs_hotel_anterior = paxs_hotel

                                    bairro=df_router_filtrado_2.at[value, 'Micro Região']

                                    paxs_hotel = df_ref[df_ref['Est Origem']==df_router_filtrado_2.at[value, 'Est Origem']]['Total ADT | CHD'].sum()

                                    # Se estourar a capacidade do carro, aí trata como se fosse o primeiro hotel e adiciona 1 na variável carros
                                    # pra, no final, eu saber quantos carros foram usados nesse roteiro e poder dividir 'igualmente' a quantidade de hoteis

                                    if paxs_total_roteiro+paxs_hotel>pax_max:

                                        mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                            abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                paxs_hotel)

                                    # Se não estourar a capacidade máxima

                                    else:

                                        paxs_total_roteiro+=paxs_hotel

                                        # Sempre que inicia um carro, o bairro fica vazio. Portanto, se não for o primeiro hotel do carro, vai definir a variavel
                                        # intervalo_ref pra o robô saber quantos minutos deve adicionar até o próximo horário de apresentação

                                        if bairro!='':

                                            intervalo_ref = definir_intervalo_ref(df_router_filtrado_2, value, intervalo_bairros_iguais, 
                                                                                  intervalo_bairros_diferentes)

                                        # Se no hotel anterior tiver muitos paxs, vai adicionar mais 5 minutos ao intervalo

                                        if paxs_hotel_anterior>=pax_cinco_min:

                                            intervalo_ref+=intervalo_bairros_iguais

                                        data_horario_hotel = df_router_filtrado_2.at[value-1, 'Data Horario Apresentacao']+intervalo_ref

                                        if data_horario_hotel - data_horario_primeiro_hotel>intervalo_pu_hotel:

                                            mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                                abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                paxs_hotel)

                                        else:

                                            df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=data_horario_hotel

                                            df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                        else:

                            roteiro+=1

                            carros = 1

                            paxs_total_roteiro = 0

                            contador_hoteis = 0

                            bairro = ''

                            mais_sugestao=1

                            # Loop no voo para colocar os horários

                            for index_2, value in df_ref['index'].items():

                                # Se for o primeiro hotel do voo, define o horário inicial, colhe o horário do hotel e inicia somatório de paxs do roteiro

                                if value==index_inicial:

                                    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=\
                                        definir_horario_primeiro_hotel(df_router_filtrado_2, value, intervalo_inicial, ajuste_pitimbu, 
                                                                                        ajuste_rec_cg)
                                    
                                    data_horario_primeiro_hotel = df_router_filtrado_2.at[value, 'Data Horario Apresentacao']
                                    
                                    paxs_hotel = df_ref[df_ref['Est Origem']==df_router_filtrado_2.at[value, 'Est Origem']]['Total ADT | CHD'].sum()

                                    paxs_total_roteiro+=paxs_hotel

                                    df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                                    contador_hoteis+=1

                                # Se não for a primeira linha do voo, mas o hotel for igual o hotel anterior, só repete o horário de apresentação

                                elif df_router_filtrado_2.at[value, 'Est Origem']==df_router_filtrado_2.at[value-1, 'Est Origem']:

                                    df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=df_router_filtrado_2.at[value-1, 'Data Horario Apresentacao']

                                    df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

                                # Se não for a primeira linha do voo e o hotel não for igual ao anterior

                                else:

                                    # Colhe a quantidade de paxs do hotel anterior, o bairro do hotel atual, a quantidade de paxs do hotel atual 
                                    # e verifica se estoura a capacidade máxima de um carro

                                    contador_hoteis+=1

                                    paxs_hotel_anterior = paxs_hotel

                                    bairro=df_router_filtrado_2.at[value, 'Micro Região']

                                    paxs_hotel = df_ref[df_ref['Est Origem']==df_router_filtrado_2.at[value, 'Est Origem']]['Total ADT | CHD'].sum()

                                    if contador_hoteis>max_hoteis:

                                        mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                            abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                paxs_hotel)
                                        
                                        contador_hoteis = 1
                                        
                                    else:

                                        # Se estourar a capacidade do carro, aí trata como se fosse o primeiro hotel e adiciona 1 na variável carros
                                        # pra, no final, eu saber quantos carros foram usados nesse roteiro e poder dividir 'igualmente' a quantidade de hoteis

                                        if paxs_total_roteiro+paxs_hotel>pax_max:

                                            mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                                abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                    paxs_hotel)
                                            
                                            contador_hoteis = 1

                                        # Se não estourar a capacidade máxima

                                        else:

                                            paxs_total_roteiro+=paxs_hotel

                                            # Sempre que inicia um carro, o bairro fica vazio. Portanto, se não for o primeiro hotel do carro, vai definir a variavel
                                            # intervalo_ref pra o robô saber quantos minutos deve adicionar até o próximo horário de apresentação

                                            if bairro!='':

                                                intervalo_ref = definir_intervalo_ref(df_router_filtrado_2, value, intervalo_bairros_iguais, 
                                                                                      intervalo_bairros_diferentes)

                                            # Se no hotel anterior tiver muitos paxs, vai adicionar mais 5 minutos ao intervalo

                                            if paxs_hotel_anterior>=pax_cinco_min:

                                                intervalo_ref+=intervalo_bairros_iguais

                                            data_horario_hotel = df_router_filtrado_2.at[value-1, 'Data Horario Apresentacao']+intervalo_ref

                                            if data_horario_hotel - data_horario_primeiro_hotel>intervalo_pu_hotel:

                                                mais_sugestao, carros, df_router_filtrado_2, data_horario_primeiro_hotel, bairro, paxs_total_roteiro = \
                                                    abrir_novo_carro(carros, df_router_filtrado_2, value, index, intervalo_inicial, ajuste_pitimbu, ajuste_rec_cg, 
                                                                    paxs_hotel)
                                                
                                                contador_hoteis = 1

                                            else:

                                                df_router_filtrado_2.at[value, 'Data Horario Apresentacao']=data_horario_hotel

                                                df_router_filtrado_2 = preencher_roteiro_carros(df_router_filtrado_2, roteiro, carros, value)

# Se tiver hotel não cadastrado, identifica quais são os hoteis e joga no final da lista da planilha para o usuário cadastrar

    else:

        df_itens_faltantes = pd.DataFrame(itens_faltantes, columns=['Est Origem'])

        st.dataframe(df_itens_faltantes, hide_index=True)

        df_itens_faltantes['Micro Região']=''

        df_itens_faltantes['Sequência']=''

        df_hoteis_geral = pd.concat([st.session_state.df_hoteis, df_itens_faltantes])

        ######### Carregar Dados Google Sheets ########## ALEXANDRE MAGNO
        nome_credencial = st.secrets["CREDENCIAL_SHEETS"]
        credentials = service_account.Credentials.from_service_account_info(nome_credencial)
        scope = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = credentials.with_scopes(scope)
        client = gspread.authorize(credentials)

        # Abrir a planilha desejada pelo seu ID
        spreadsheet = client.open_by_key('1vbGeqKyM4VSvHbMiyiqu1mkwEhneHi28e8cQ_lYMYhY')

        # Selecionar a primeira planilha
        sheet = spreadsheet.worksheet("BD")
        sheet_data = sheet.get_all_values()
        limpar_colunas = "A:D"
        sheet.batch_clear([limpar_colunas])
        data = [df_hoteis_geral.columns.values.tolist()] + df_hoteis_geral.values.tolist()
        sheet.update("A1", data)
        ##################################################

        st.error('Os hoteis acima não estão cadastrados na lista de sequência de hoteis. Eles foram inseridos no final da lista. Por favor, coloque-os na sequência e tente novamente')

        st.stop()

    df_roteiros_alternativos = pd.DataFrame(columns=df_router_filtrado_2.columns.tolist())

    lista_roteiros_alternativos = df_router_filtrado_2[df_router_filtrado_2['Carros']==2]['Roteiro'].unique().tolist()

    roteiro = 0










    # Gerando roteiros alternativos

    for item in lista_roteiros_alternativos:

        df_ref = df_router_filtrado_2[df_router_filtrado_2['Roteiro']==item].reset_index(drop=True)

        divisao_inteira = len(df_ref['Est Origem'].unique().tolist()) // df_ref['Carros'].max()

        if len(df_ref['Est Origem'].unique().tolist()) % df_ref['Carros'].max() == 0:

            max_hoteis = divisao_inteira

        else:

            max_hoteis = divisao_inteira + 1

        carros = 1

        paxs_total_roteiro = 0

        contador_hoteis = 0

        bairro = ''

        for index in range(len(df_ref)):

            # Se for o primeiro hotel do voo, define o horário inicial, colhe o horário do hotel e inicia somatório de paxs do roteiro

            if index==0:

                df_ref.at[index, 'Data Horario Apresentacao']=definir_horario_primeiro_hotel(df_ref, index, intervalo_inicial, ajuste_pitimbu, 
                                                                                             ajuste_rec_cg)
                
                data_horario_primeiro_hotel = df_ref.at[index, 'Data Horario Apresentacao']
                
                paxs_hotel = df_ref[df_ref['Est Origem']==df_ref.at[index, 'Est Origem']]['Total ADT | CHD'].sum()

                paxs_total_roteiro+=paxs_hotel

                df_ref = preencher_roteiro_carros(df_ref, item, carros, index)

                contador_hoteis+=1

            # Se não for a primeira linha do voo, mas o hotel for igual o hotel anterior, só repete o horário de apresentação

            elif df_ref.at[index, 'Est Origem']==df_ref.at[index-1, 'Est Origem']:

                df_ref.at[index, 'Data Horario Apresentacao']=df_ref.at[index-1, 'Data Horario Apresentacao']

                df_ref = preencher_roteiro_carros(df_ref, item, carros, index)

            # Se não for a primeira linha do voo e o hotel não for igual ao anterior

            else:

                # Colhe a quantidade de paxs do hotel anterior, o bairro do hotel atual, a quantidade de paxs do hotel atual 
                # e verifica se estoura a capacidade máxima de um carro

                contador_hoteis+=1

                if contador_hoteis>max_hoteis:

                    mais_sugestao = 1

                    carros+=1

                    df_ref.at[index, 'Data Horario Apresentacao']=definir_horario_primeiro_hotel(df_ref, index, intervalo_inicial, ajuste_pitimbu, 
                                                                                                 ajuste_rec_cg)
                    
                    paxs_hotel = df_ref[df_ref['Est Origem']==df_ref.at[index, 'Est Origem']]['Total ADT | CHD'].sum()

                    data_horario_primeiro_hotel = df_ref.at[index, 'Data Horario Apresentacao']

                    paxs_total_roteiro = 0

                    bairro = ''

                    paxs_total_roteiro+=paxs_hotel

                    df_ref.at[index, 'Roteiro'] = item

                    df_ref.at[index, 'Carros'] = carros
                    
                    contador_hoteis = 1
                    
                else:

                    paxs_hotel_anterior = paxs_hotel

                    bairro=df_ref.at[index, 'Micro Região']

                    paxs_hotel = df_ref[df_ref['Est Origem']==df_ref.at[index, 'Est Origem']]['Total ADT | CHD'].sum()

                    # Se estourar a capacidade do carro, aí trata como se fosse o primeiro hotel e adiciona 1 na variável carros
                    # pra, no final, eu saber quantos carros foram usados nesse roteiro e poder dividir 'igualmente' a quantidade de hoteis

                    if paxs_total_roteiro+paxs_hotel>pax_max:

                        mais_sugestao = 1

                        carros+=1

                        df_ref.at[index, 'Data Horario Apresentacao']=definir_horario_primeiro_hotel(df_ref, index, intervalo_inicial, ajuste_pitimbu, 
                                                                                                    ajuste_rec_cg)

                        data_horario_primeiro_hotel = df_ref.at[index, 'Data Horario Apresentacao']

                        paxs_total_roteiro = 0

                        bairro = ''

                        paxs_total_roteiro+=paxs_hotel

                        df_ref.at[index, 'Roteiro'] = item

                        df_ref.at[index, 'Carros'] = carros
                        
                        contador_hoteis = 1

                    # Se não estourar a capacidade máxima

                    else:

                        paxs_total_roteiro+=paxs_hotel

                        # Sempre que inicia um carro, o bairro fica vazio. Portanto, se não for o primeiro hotel do carro, vai definir a variavel
                        # intervalo_ref pra o robô saber quantos minutos deve adicionar até o próximo horário de apresentação

                        if bairro!='':

                            intervalo_ref = definir_intervalo_ref(df_ref, index, intervalo_bairros_iguais, intervalo_bairros_diferentes)

                        # Se no hotel anterior tiver muitos paxs, vai adicionar mais 5 minutos ao intervalo

                        if paxs_hotel_anterior>=pax_cinco_min:

                            intervalo_ref+=intervalo_bairros_iguais

                        data_horario_hotel = df_ref.at[index-1, 'Data Horario Apresentacao']+intervalo_ref

                        if data_horario_hotel - data_horario_primeiro_hotel>intervalo_pu_hotel:

                            mais_sugestao = 1

                            carros+=1

                            df_ref.at[index, 'Data Horario Apresentacao']=definir_horario_primeiro_hotel(df_ref, index, intervalo_inicial, ajuste_pitimbu, 
                                                                                                        ajuste_rec_cg)

                            data_horario_primeiro_hotel = df_ref.at[index, 'Data Horario Apresentacao']

                            paxs_total_roteiro = 0

                            bairro = ''

                            paxs_total_roteiro+=paxs_hotel

                            df_ref.at[index, 'Roteiro'] = item

                            df_ref.at[index, 'Carros'] = carros
                            
                            contador_hoteis = 1

                        else:

                            df_ref.at[index, 'Data Horario Apresentacao']=data_horario_hotel

                            df_ref = preencher_roteiro_carros(df_ref, item, carros, index)

        df_roteiros_alternativos = pd.concat([df_roteiros_alternativos, df_ref], ignore_index=True)

    st.divider()

    row3 = st.columns(3)

    coluna = 0

    # Plotando roteiros de cada carro

    if len(df_hoteis_pax_max)>0:

        for item in df_hoteis_pax_max['Roteiro'].unique().tolist():

            df_ref_1 = df_hoteis_pax_max[df_hoteis_pax_max['Roteiro']==item].reset_index(drop=True)

            lista_voos = df_hoteis_pax_max[df_hoteis_pax_max['Roteiro']==item]['Voo'].unique().tolist()

            titulo_voos=''

            for voo in lista_voos:

                if titulo_voos=='':

                    titulo_voos = voo

                else:

                    titulo_voos = f'{titulo_voos} + {voo}'

            for carro in df_ref_1['Carros'].unique().tolist():

                df_ref_2 = df_ref_1[df_ref_1['Carros']==carro].reset_index(drop=True)

                modo = df_ref_2.at[0, 'Modo do Servico']

                paxs_total = int(df_ref_2['Total ADT | CHD'].sum())

                titulo_roteiro = f'Roteiro {item}'

                titulo_carro = f'Carro {carro}'

                titulo_modo_voo_pax = f'*{modo.title()} | {titulo_voos} | {paxs_total} paxs*'

                df_ref_3 = df_ref_2.groupby('Est Origem').agg({'Total ADT | CHD': 'sum', 'Data Horario Apresentacao': 'first'})\
                    .sort_values(by='Data Horario Apresentacao').reset_index()
            
                with row3[coluna]:

                    container = st.container(border=True, height=500)

                    container.header(titulo_roteiro)

                    container.subheader(titulo_carro)

                    container.markdown(titulo_modo_voo_pax)

                    container.dataframe(df_ref_3[['Est Origem', 'Total ADT | CHD', 'Data Horario Apresentacao']], hide_index=True)

                    if coluna==2:

                        coluna=0

                    else:

                        coluna+=1

    for item in df_router_filtrado_2['Roteiro'].unique().tolist():

        df_ref_1 = df_router_filtrado_2[df_router_filtrado_2['Roteiro']==item].reset_index(drop=True)

        lista_voos = df_router_filtrado_2[df_router_filtrado_2['Roteiro']==item]['Voo'].unique().tolist()

        titulo_voos=''

        for voo in lista_voos:

            if titulo_voos=='':

                titulo_voos = voo

            else:

                titulo_voos = f'{titulo_voos} + {voo}'

        for carro in df_ref_1['Carros'].unique().tolist():

            df_ref_2 = df_ref_1[df_ref_1['Carros']==carro].reset_index(drop=True)

            modo = df_ref_2.at[0, 'Modo do Servico']

            paxs_total = int(df_ref_2['Total ADT | CHD'].sum())

            if modo=='REGULAR':

                titulo_roteiro = f'Roteiro {item}'

                titulo_carro = f'Carro {carro}'

                titulo_modo_voo_pax = f'*{modo.title()} | {titulo_voos} | {paxs_total} paxs*'

            else:

                reserva = df_ref_2.at[0, 'Reserva']

                titulo_roteiro = f'Roteiro {item}'

                titulo_carro = f'Carro {carro}'

                titulo_modo_voo_pax = f'*{modo.title()} | {reserva} | {titulo_voos} | {paxs_total} paxs*'

            df_ref_3 = df_ref_2.groupby('Est Origem').agg({'Total ADT | CHD': 'sum', 'Data Horario Apresentacao': 'first'})\
                .sort_values(by='Data Horario Apresentacao').reset_index()
        
            with row3[coluna]:

                container = st.container(border=True, height=500)

                container.header(titulo_roteiro)

                container.subheader(titulo_carro)

                container.markdown(titulo_modo_voo_pax)

                container.dataframe(df_ref_3[['Est Origem', 'Total ADT | CHD', 'Data Horario Apresentacao']], hide_index=True)

                if coluna==2:

                    coluna=0

                else:

                    coluna+=1

        if item in  df_roteiros_alternativos['Roteiro'].unique().tolist():

            df_ref_1 = df_roteiros_alternativos[df_roteiros_alternativos['Roteiro']==item].reset_index(drop=True)

            lista_voos = df_router_filtrado_2[df_router_filtrado_2['Roteiro']==item]['Voo'].unique().tolist()

            titulo_voos=''

            for voo in lista_voos:

                if titulo_voos=='':

                    titulo_voos = voo

                else:

                    titulo_voos = f'{titulo_voos} + {voo}'

            for carro in df_ref_1['Carros'].unique().tolist():

                df_ref_2 = df_ref_1[df_ref_1['Carros']==carro].reset_index(drop=True)

                modo = df_ref_2.at[0, 'Modo do Servico']

                paxs_total = int(df_ref_2['Total ADT | CHD'].sum())

                if modo=='REGULAR':

                    titulo_roteiro = f'Opção Alternativa | Roteiro {item}'

                    titulo_carro = f'Carro {carro}'

                    titulo_modo_voo_pax = f'*{modo.title()} | {titulo_voos} | {paxs_total} paxs*'

                else:

                    reserva = df_ref_2.at[0, 'Reserva']

                    titulo_roteiro = f'Opção Alternativa | Roteiro {item}'

                    titulo_carro = f'Carro {carro}'

                    titulo_modo_voo_pax = f'*{modo.title()} | {reserva} | {titulo_voos} | {paxs_total} paxs*'

                df_ref_3 = df_ref_2.groupby('Est Origem').agg({'Total ADT | CHD': 'sum', 'Data Horario Apresentacao': 'first'})\
                    .sort_values(by='Data Horario Apresentacao').reset_index()
            
                with row3[coluna]:

                    container = st.container(border=True, height=500)

                    container.header(titulo_roteiro)

                    container.subheader(titulo_carro)

                    container.markdown(titulo_modo_voo_pax)

                    container.dataframe(df_ref_3[['Est Origem', 'Total ADT | CHD', 'Data Horario Apresentacao']], hide_index=True)

                    if coluna==2:

                        coluna=0

                    else:

                        coluna+=1










        

        
