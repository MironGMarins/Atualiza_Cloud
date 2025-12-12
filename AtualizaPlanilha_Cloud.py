import asyncio
import os
import pandas as pd
import numpy as np
import time
import json
import re
import requests
import unicodedata
from datetime import datetime, timedelta

from google_spreadsheets.spreadsheet import GoogleSheetClient
import gspread

# ==============================================================================
# ⚠️ CONFIGURAÇÕES OBTIDAS DOS SEGREDOS (ENV VARS)
# ==============================================================================
# Se você não criou secret para o ID da conta, mantemos fixo aqui:
ACCOUNT_ID = "3619571" 

# Segredos de Autenticação
CLIENT_ID = os.getenv("BASECAMP_CLIENT_ID")
CLIENT_SECRET = os.getenv("BASECAMP_CLIENT_SECRET")
REDIRECT_URI = os.getenv("BASECAMP_REDIRECT_URI", "http://localhost:8000/callback")
REFRESH_TOKEN_SECRETO = os.getenv("BASECAMP_REFRESH_TOKEN")

# --- CONFIGURAÇÕES DE BUSCA ---
PALAVRAS_CHAVE_PROJETO = ["MEDIA PORTAL", "SPRINT"] 
TERMOS_DE_BUSCA_LISTAS = ["ATIVIDADES DA SEMANA", "BACKLOG"]
LIMITE_LISTAS_RECENTES = 6
LIMITE_TAREFAS_POR_LISTA = 0 

# Nomes das Abas
NOME_ABA_EQUIPES = "Equipes"
NOME_ABA_GERAL = "Total BaseCamp Semanas"
NOME_ABA_CONSOLIDADA = "Total BaseCamp para Notas"
NOME_ABA_HISTORICO = "HistoricoDiario"
NOME_ABA_BACKLOG = "Backlog"
MAX_RETRIES = 5

MESES_NUM_PT = {
    1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril', 5: 'Maio', 6: 'Junho',
    7: 'Julho', 8: 'Agosto', 9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
}
MESES_PT_NUM = {v: k for k, v in MESES_NUM_PT.items()} 

def a_second():
    time.sleep(0.05) 
    return True

# --- PREPARAÇÃO DO AMBIENTE CLOUD ---
def configurar_google_credentials():
    """Recria o arquivo json do Google a partir do Segredo"""
    conteudo_json = os.getenv("GCP_CREDENTIALS_JSON")
    caminho_arquivo = "google_credentials.json"
    
    if conteudo_json and not os.path.exists(caminho_arquivo):
        print(">>> Configurando credenciais Google...")
        with open(caminho_arquivo, "w") as f:
            f.write(conteudo_json)

# --- FUNÇÕES AUXILIARES ---
def normalizar_texto(texto):
    if not isinstance(texto, str): return ""
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn').upper().strip()

def encontrar_encarregado(sub_lista_val, df_equipes):
    if df_equipes.empty: return ""
    val_norm = normalizar_texto(sub_lista_val)
    val_limpo = val_norm.replace("ATIVIDADES", "").strip()
    if not val_limpo: return ""
    
    partes_busca = val_limpo.split()
    primeiro_nome_busca = partes_busca[0] if partes_busca else ""
    sobrenome_busca = partes_busca[1] if len(partes_busca) > 1 else ""
    
    for nome_real in df_equipes['Nome']:
        nome_real_norm = normalizar_texto(nome_real)
        partes_real = nome_real_norm.split()
        primeiro_nome_real = partes_real[0] if partes_real else ""
        
        if primeiro_nome_busca == primeiro_nome_real:
            if sobrenome_busca:
                if len(partes_real) > 1:
                    sobrenome_real = partes_real[1]
                    if sobrenome_busca[:4] == sobrenome_real[:4]:
                        return nome_real
            else:
                return nome_real
    return ""

def extrair_data_da_lista_dt(texto_lista):
    try:
        match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', str(texto_lista))
        if match:
            return pd.to_datetime(match.group(1), dayfirst=True)
    except: pass
    return pd.NaT

def converter_data_segura(series):
    series = series.astype(str).str.strip()
    series = series.replace(['nan', 'None', '', 'NaT', '0', '#N/A'], np.nan)
    return pd.to_datetime(series, dayfirst=True, errors='coerce')

def extrair_mes_ano_do_nome_aba(nome_aba):
    try:
        partes = nome_aba.split()
        if len(partes) == 2:
            mes_txt = partes[0].capitalize() 
            ano_txt = partes[1]              
            if mes_txt in MESES_PT_NUM and ano_txt.isdigit():
                return MESES_PT_NUM[mes_txt], int(ano_txt)
    except: pass
    return None

# --- AUTENTICAÇÃO CLOUD ---
def obter_token_cloud():
    """Gera token novo usando o Refresh Token dos segredos"""
    if not REFRESH_TOKEN_SECRETO:
        print("❌ ERRO: Segredo BASECAMP_REFRESH_TOKEN não encontrado.")
        return None

    print("🔄 Gerando Token de Acesso (Cloud)...")
    url = "https://launchpad.37signals.com/authorization/token"
    payload = {
        "type": "refresh",
        "refresh_token": REFRESH_TOKEN_SECRETO,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI
    }
    try:
        resp = requests.post(url, json=payload)
        if resp.status_code == 200:
            return resp.json()["access_token"]
        else:
            print(f"❌ Falha auth: {resp.text}")
            return None
    except Exception as e:
        print(f"❌ Erro conexão: {e}")
        return None

# --- MOTOR DE BUSCA ---
async def fetch_greedy_async(token, url):
    items = []
    page = 1
    if not url.startswith("http"):
        base_url = f"https://3.basecampapi.com/{ACCOUNT_ID}"
        url = f"{base_url}{url}"
    connector = "&" if "?" in url else "?"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    while True:
        target_url = f"{url}{connector}page={page}"
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                response = await asyncio.to_thread(requests.get, target_url, headers=headers)
                if response.status_code == 404: 
                    success = True; break
                if response.status_code != 200:
                    time.sleep(1); continue
                data = response.json()
                if not data: 
                    success = True; break
                items.extend(data)
                page += 1
                success = True
                await asyncio.to_thread(a_second)
                break 
            except: time.sleep(1)
        if success and (not data if 'data' in locals() else True): break
        if not success: break 
    return items

async def descobrir_bucket_ids(token):
    print("\n>>> BUSCANDO PROJETOS ATIVOS...")
    projects_url = f"/projects.json"
    all_projects = await fetch_greedy_async(token, projects_url)
    ids_encontrados = []
    print("   Projetos localizados:")
    for p in all_projects:
        pid = p.get('id'); pname = p.get('name', '').upper()
        if any(chave in pname for chave in PALAVRAS_CHAVE_PROJETO):
            ids_encontrados.append(pid)
            print(f"   ✅ [MATCH] {pname} (ID: {pid})")
    if not ids_encontrados: print("   ❌ NENHUM PROJETO ENCONTRADO.")
    return ids_encontrados

async def extract_tasks_complete(token, bucket_id, todolist):
    tasks_bucket = []
    list_id = todolist.get('id'); list_name = todolist.get('title'); list_app_url = todolist.get('app_url')
    print(f"\n   📂 {list_name}...", end=" ")

    todos_url = todolist.get('todos_url')
    if not todos_url: todos_url = f"https://3.basecampapi.com/{ACCOUNT_ID}/buckets/{bucket_id}/todolists/{list_id}/todos.json"

    root_items = []
    root_items += await fetch_greedy_async(token, todos_url)
    root_items += await fetch_greedy_async(token, todos_url + "?completed=true")
    root_items += await fetch_greedy_async(token, todos_url + "?status=archived")

    for t in root_items:
        t['hierarquia_semana'] = list_name; t['hierarquia_grupo'] = "(Raiz)"; t['parent_list_url'] = list_app_url
    tasks_bucket.extend(root_items)
    print(f"| Raiz: {len(root_items)}", end=" ")

    groups_url = f"https://3.basecampapi.com/{ACCOUNT_ID}/buckets/{bucket_id}/todolists/{list_id}/groups.json"
    all_groups = []
    all_groups += await fetch_greedy_async(token, groups_url)
    all_groups += await fetch_greedy_async(token, groups_url + "?status=archived")
    
    total_grupos_items = 0
    if all_groups:
        for g in all_groups:
            target_link = g.get('todos_url')
            if target_link:
                group_items = []
                group_items += await fetch_greedy_async(token, target_link)
                group_items += await fetch_greedy_async(token, target_link + "?completed=true")
                group_items += await fetch_greedy_async(token, target_link + "?status=archived")
                for t in group_items:
                    t['hierarquia_semana'] = list_name; t['hierarquia_grupo'] = g['title']; t['parent_list_url'] = list_app_url
                tasks_bucket.extend(group_items)
                total_grupos_items += len(group_items)
                print(".", end="", flush=True)

    print(f"| G: {total_grupos_items} | T: {len(tasks_bucket)}")
    return tasks_bucket

async def process_bucket(token, bucket_id):
    print(f"\n>>> PROCESSANDO BUCKET {bucket_id}")
    api_base = f"https://3.basecampapi.com/{ACCOUNT_ID}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.get(f"{api_base}/projects/{bucket_id}.json", headers=headers); proj = r.json()
        todoset_id = next((t['id'] for t in proj.get('dock', []) if t['name'] == 'todoset'), None)
    except: return []
    if not todoset_id: return []

    url_lists = f"{api_base}/buckets/{bucket_id}/todosets/{todoset_id}/todolists.json"
    all_lists = []
    all_lists += await fetch_greedy_async(token, url_lists)
    all_lists += await fetch_greedy_async(token, url_lists + "?status=archived")
    
    listas_alvo = []
    for l in all_lists:
        titulo = l.get('title', '').upper()
        if any(termo in titulo for termo in TERMOS_DE_BUSCA_LISTAS): listas_alvo.append(l)

    backlogs = [l for l in listas_alvo if "BACKLOG" in l.get('title', '').upper()]
    semanas  = [l for l in listas_alvo if "BACKLOG" not in l.get('title', '').upper()]
    
    def sort_key(x):
        d = extrair_data_da_lista_dt(x.get('title'))
        return d if pd.notnull(d) else datetime.min
    semanas.sort(key=sort_key, reverse=True)
    if LIMITE_LISTAS_RECENTES > 0: semanas = semanas[:LIMITE_LISTAS_RECENTES]

    lista_final = backlogs + semanas
    print(f"   Selecionadas: {len(lista_final)}")

    bucket_tasks = []
    for todolist in lista_final:
        tasks = await extract_tasks_complete(token, bucket_id, todolist)
        bucket_tasks.extend(tasks)
    return bucket_tasks

# --- LÓGICA DE DADOS ---
def processar_mes_atual(df_completo, sheet_client, df_equipes):
    print("\n>>> VERIFICANDO ABA DO MÊS ATUAL...")
    hoje = datetime.now()
    mes_atual = hoje.month; ano_atual = hoje.year
    nome_aba_atual = f"{MESES_NUM_PT[mes_atual]} {ano_atual}"

    df_completo['Data_Ref_Lista'] = df_completo['Atividades Semanal'].apply(extrair_data_da_lista_dt)
    mask_mes_atual = (df_completo['Data_Ref_Lista'].dt.month == mes_atual) & (df_completo['Data_Ref_Lista'].dt.year == ano_atual)
    df_mes = df_completo[mask_mes_atual].copy()
    
    if df_mes.empty: print(f"   ⚠️ Nada para {nome_aba_atual}."); return
    print(f"   📅 Atualizando: '{nome_aba_atual}'")

    if 'Data Final' in df_mes.columns:
        df_mes['Data_Final_Obj'] = pd.to_datetime(df_mes['Data Final'], dayfirst=True, errors='coerce')
        df_mes['Sexta_Limite'] = df_mes['Data_Ref_Lista'] + pd.Timedelta(days=4)
        mask_ajuste = (df_mes['Data_Final_Obj'].notna()) & (df_mes['Data_Final_Obj'] > df_mes['Sexta_Limite'])
        df_mes.loc[mask_ajuste, 'Data Final'] = df_mes.loc[mask_ajuste, 'Sexta_Limite'].dt.strftime('%d/%m/%Y')
        df_mes = df_mes.drop(columns=['Data_Final_Obj', 'Sexta_Limite'])

    if not df_equipes.empty:
        df_mes['Encarregado'] = df_mes['Sub-Lista / Grupo'].apply(lambda x: encontrar_encarregado(x, df_equipes))

    cols_final = ['ID', 'Status', 'Atividades Semanal', 'Sub-Lista / Grupo', 'Nome Task', 'Encarregado', 'Data Inicial', 'Data Final', 'Link', 'Link Lista']
    df_upload = df_mes[[c for c in cols_final if c in df_mes.columns]].fillna("")
    
    try:
        spreadsheet = sheet_client.client.open(title=os.getenv("SPREADSHEET_NAME"), folder_id=os.getenv("FOLDER_ID"))
        try: worksheet = spreadsheet.worksheet(nome_aba_atual); worksheet.clear()
        except gspread.exceptions.WorksheetNotFound: worksheet = spreadsheet.add_worksheet(title=nome_aba_atual, rows=len(df_upload)+100, cols=20)
        data = [df_upload.columns.values.tolist()] + df_upload.astype(str).values.tolist()
        worksheet.update(data, value_input_option='USER_ENTERED')
        print(f"      ✅ Sucesso!")
    except Exception as e: print(f"      ❌ Erro upload: {e}")

def atualizar_aba_backlog(df_global, sheet_client, df_equipes):
    print(f"\n>>> ATUALIZANDO ABA '{NOME_ABA_BACKLOG}'...")
    mask_backlog = df_global['Atividades Semanal'].astype(str).str.contains("BACKLOG", case=False, na=False)
    df_backlog = df_global[mask_backlog].copy()
    if df_backlog.empty: print("   ⚠️ Vazio."); return

    if not df_equipes.empty:
         df_backlog['Encarregado'] = df_backlog['Sub-Lista / Grupo'].apply(lambda x: encontrar_encarregado(x, df_equipes))

    cols_final = ['ID', 'Status', 'Atividades Semanal', 'Sub-Lista / Grupo', 'Nome Task', 'Encarregado', 'Data Inicial', 'Data Final', 'Link', 'Link Lista']
    for c in cols_final:
        if c not in df_backlog.columns: df_backlog[c] = ""
    df_upload = df_backlog[cols_final].fillna("")

    try:
        spreadsheet = sheet_client.client.open(title=os.getenv("SPREADSHEET_NAME"), folder_id=os.getenv("FOLDER_ID"))
        try: worksheet = spreadsheet.worksheet(NOME_ABA_BACKLOG); worksheet.clear()
        except gspread.exceptions.WorksheetNotFound: worksheet = spreadsheet.add_worksheet(title=NOME_ABA_BACKLOG, rows=len(df_upload)+100, cols=20)
        data = [df_upload.columns.values.tolist()] + df_upload.astype(str).values.tolist()
        worksheet.update(data, value_input_option='USER_ENTERED')
        print(f"   ✅ Atualizado!")
    except: pass

def consolidar_meses_para_notas(sheet_client):
    print("\n>>> CONSOLIDANDO NOTAS...")
    try:
        spreadsheet = sheet_client.client.open(title=os.getenv("SPREADSHEET_NAME"), folder_id=os.getenv("FOLDER_ID"))
        all_worksheets = spreadsheet.worksheets()
    except: return

    dfs_para_consolidar = []
    for ws in all_worksheets:
        mes_ano = extrair_mes_ano_do_nome_aba(ws.title)
        if mes_ano:
            mes_aba, ano_aba = mes_ano
            try:
                raw_data = ws.get_all_values()
                if not raw_data: continue
                headers = raw_data[0]; rows = raw_data[1:]
                df_aba = pd.DataFrame(rows, columns=headers)
                
                if 'Data Final' in df_aba.columns:
                    df_aba['Data_Final_DT'] = converter_data_segura(df_aba['Data Final'])
                    mask_correto = ((df_aba['Data_Final_DT'].dt.month == mes_aba) & (df_aba['Data_Final_DT'].dt.year == ano_aba)) | (df_aba['Data_Final_DT'].isna())
                    df_filtrado = df_aba[mask_correto].copy().drop(columns=['Data_Final_DT'])
                    if not df_filtrado.empty:
                        df_filtrado['Origem_Aba'] = ws.title 
                        dfs_para_consolidar.append(df_filtrado)
            except: pass

    if dfs_para_consolidar:
        df_final = pd.concat(dfs_para_consolidar, ignore_index=True)
        try:
            try: ws_cons = spreadsheet.worksheet(NOME_ABA_CONSOLIDADA); ws_cons.clear()
            except gspread.exceptions.WorksheetNotFound: ws_cons = spreadsheet.add_worksheet(title=NOME_ABA_CONSOLIDADA, rows=len(df_final)+500, cols=20)
            data = [df_final.columns.values.tolist()] + df_final.astype(str).values.tolist()
            ws_cons.update(data, value_input_option='USER_ENTERED')
            print("   ✅ Atualizado!")
        except: pass

def atualizar_historico_diario(df_global, sheet_client):
    print("\n>>> HISTÓRICO DIÁRIO...")
    hoje = datetime.now()
    inicio_semana = hoje - timedelta(days=hoje.weekday()) 
    
    df_global['Data_Ref_Lista'] = df_global['Atividades Semanal'].apply(extrair_data_da_lista_dt)
    df_global['Data_Ref_Lista'] = pd.to_datetime(df_global['Data_Ref_Lista']).dt.date
    inicio_semana_date = inicio_semana.date()
    
    df_semana = df_global[df_global['Data_Ref_Lista'] == inicio_semana_date]
    total_tarefas = len(df_semana)
    fechadas = df_semana[df_semana['Data Final'].astype(str).str.len() > 5].shape[0]
    
    try:
        spreadsheet = sheet_client.client.open(title=os.getenv("SPREADSHEET_NAME"), folder_id=os.getenv("FOLDER_ID"))
        try: ws_hist = spreadsheet.worksheet(NOME_ABA_HISTORICO)
        except gspread.exceptions.WorksheetNotFound:
            ws_hist = spreadsheet.add_worksheet(title=NOME_ABA_HISTORICO, rows=1000, cols=3)
            ws_hist.append_row(["Data", "Total_Fechadas", "Total_Tarefas"]) 
            
        dados_existentes = ws_hist.get_all_values()
        hoje_str = hoje.strftime('%d/%m/%Y')
        linha_encontrada = None
        for i, row in enumerate(dados_existentes):
            if i == 0: continue 
            if row[0] == hoje_str:
                linha_encontrada = i + 1 
                break
        
        nova_linha = [hoje_str, fechadas, total_tarefas]
        if linha_encontrada: ws_hist.update(f"A{linha_encontrada}:C{linha_encontrada}", [nova_linha], value_input_option='USER_ENTERED')
        else: ws_hist.append_row(nova_linha, value_input_option='USER_ENTERED')
        print("   ✅ Atualizado!")
    except: pass

async def main_process():
    print(f"=== INICIANDO SINCRONIA CLOUD ===")
    configurar_google_credentials()
    token = obter_token_cloud()
    if not token: return

    # 1. DESCOBRIR PROJETOS
    buckets_dinamicos = await descobrir_bucket_ids(token)
    if not buckets_dinamicos: return

    # 2. BAIXAR TAREFAS
    all_tasks = []
    for bucket_id in buckets_dinamicos:
        tasks = await process_bucket(token, bucket_id)
        all_tasks.extend(tasks)

    df = pd.DataFrame(all_tasks).drop_duplicates(subset='id', keep='first')
    
    # 3. TRATAMENTO
    df['ID'] = df.get('id', '')
    df['Nome Task'] = df.get('title', '')
    df['Atividades Semanal'] = df.get('hierarquia_semana', '')
    df['Sub-Lista / Grupo'] = df.get('hierarquia_grupo', '')
    df['Link'] = df.get('app_url', '')
    df['Link Lista'] = df.get('parent_list_url', '')
    
    df['Data Inicial'] = ''
    if 'created_at' in df.columns:
        df['Data Inicial'] = pd.to_datetime(df['created_at'], errors='coerce').dt.strftime('%d/%m/%Y')

    def get_completion(row):
        if isinstance(row.get('completion'), dict):
            d = row['completion'].get('created_at')
            if d: return pd.to_datetime(d).strftime('%d/%m/%Y')
        return ''
    df['Data Final'] = df.apply(get_completion, axis=1)

    def get_status(row):
        if row.get('status') == 'archived': return "Arquivado"
        if row.get('trashed'): return "Lixeira"
        if row.get('completed'): return "Fechado"
        return "Aberto"
    df['Status'] = df.apply(get_status, axis=1)
    
    # 4. LER EQUIPES
    print("\n>>> LENDO EQUIPES...")
    df_equipes = pd.DataFrame()
    sheet_client = GoogleSheetClient() 
    try:
        ss = sheet_client.client.open(title=os.getenv("SPREADSHEET_NAME"), folder_id=os.getenv("FOLDER_ID"))
        ws_eq = ss.worksheet(NOME_ABA_EQUIPES)
        records_eq = ws_eq.get_all_records()
        df_equipes = pd.DataFrame(records_eq)
    except: print("   ⚠️ Erro Equipes")

    # 5. EXECUÇÃO DAS ATUALIZAÇÕES
    processar_mes_atual(df, sheet_client, df_equipes)

    print(f"\n>>> ATUALIZANDO GERAL...")
    if not df_equipes.empty:
        df['Encarregado'] = df['Sub-Lista / Grupo'].apply(lambda x: encontrar_encarregado(x, df_equipes))
    
    cols = ['ID', 'Status', 'Atividades Semanal', 'Sub-Lista / Grupo', 'Nome Task', 'Encarregado', 'Data Inicial', 'Data Final', 'Link', 'Link Lista']
    final_df = df[[c for c in cols if c in df.columns]].fillna("")
    
    try:
        ws = ss.worksheet(NOME_ABA_GERAL)
        ws.clear()
        ws.update([final_df.columns.values.tolist()] + final_df.astype(str).values.tolist(), value_input_option='USER_ENTERED')
        print("✅ Geral OK.")
    except: pass

    atualizar_aba_backlog(final_df, sheet_client, df_equipes)
    consolidar_meses_para_notas(sheet_client)
    atualizar_historico_diario(final_df, sheet_client)

if __name__ == "__main__":
    asyncio.run(main_process())