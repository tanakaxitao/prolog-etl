import requests                      # ✅ Para chamadas HTTP à API Prolog
from datetime import datetime        # ✅ Para datas e horas no controle de extração
import time                          # ✅ Para controlar o tempo entre as requisições
import pandas as pd                 # ✅ Para transformar os dados em DataFrame
from config.settings import API_KEY # ✅ Correto: importa a chave da API do arquivo de configurações
import psycopg2
from config.settings import DB_CONFIG


BASE_URL = "https://prologapp.com/prolog/api/v3/"
HEADERS = {"x-prolog-api-token": API_KEY}


def extract_users(branch_office_id=1074):
    url = BASE_URL + "users"
    user_ids = []
    params = {"branchOfficesId": branch_office_id, "pageSize": 100, "pageNumber": 0}
    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200 or not response.json().get("content"):
            break
        user_ids += [u["id"] for u in response.json()["content"]]
        params["pageNumber"] += 1
        time.sleep(0.5)
    details = []
    for uid in user_ids:
        r = requests.get(f"{url}/{uid}", headers=HEADERS)
        if r.status_code == 200:
            details.append(r.json())
        time.sleep(0.3)
    return details


def extract_checklists(branch_office_id=1074, start_date="2025-01-01T00:00Z"):
    url = BASE_URL + "checklists"
    end_date = datetime.utcnow().strftime("%Y-%m-%dT23:59Z")
    all_data = []
    params = {
        "branchOfficesId": [branch_office_id],
        "includeInactive": "true",
        "pageSize": 100,
        "startDate": start_date,
        "endDate": end_date,
        "includeAnswers": "true",
        "pageNumber": 0
    }
    for _ in range(100):
        r = requests.get(url, headers=HEADERS, params=params)
        if r.status_code != 200:
            break
        page_data = r.json().get("content", [])
        if not page_data:
            break
        all_data.extend(page_data)
        params["pageNumber"] += 1
        time.sleep(0.8)
    return all_data


def extract_vehicles(branch_office_id=1074):
    url = BASE_URL + "vehicles"
    params = {
        "branchOfficesId": branch_office_id,
        "includeInactive": "true",
        "pageSize": "100",
        "pageNumber": "0",
        "startDate": "2024-06-01T00:00Z",
        "endDate": "2025-09-18T23:59Z"
    }
    r = requests.get(url, headers=HEADERS, params=params)
    return r.json().get("content", []) if r.status_code == 200 else []



def get_existing_order_ids():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Verifica se a tabela existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_schema = 'Prolog' 
                AND table_name = 'ordens_servico_prolog'
            )
        """)
        exists = cursor.fetchone()[0]

        if not exists:
            print("ℹ️ Tabela 'ordens_servico_prolog' ainda não existe. Coletando todas as OS.")
            return set()

        # Verifica se a tabela tem dados
        cursor.execute('SELECT COUNT(*) FROM "Prolog".ordens_servico_prolog')
        count = cursor.fetchone()[0]

        if count == 0:
            print("ℹ️ Tabela está vazia. Coletando todas as OS.")
            return set()

        # Coleta os IDs já existentes
        cursor.execute('SELECT "internalWorkOrderId" FROM "Prolog".ordens_servico_prolog')
        rows = cursor.fetchall()
        return {row[0] for row in rows}

    except Exception as e:
        print(f"⚠️ Erro ao acessar o banco: {e}")
        return set()
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



def extract_os(branch_office_id=1074):
    """
    Função para extrair os dados das Ordens de Serviço da Prolog.
    """
    base_url = BASE_URL + "work-orders"
    params = {"branchOfficesId": branch_office_id, "pageSize": 100, "pageNumber": 0}
    order_ids = []

    print("🔍 Buscando IDs das Ordens de Serviço...")

    while True:
        try:
            response = requests.get(base_url, headers=HEADERS, params=params, timeout=10)
            if response.status_code != 200:
                print(f"❌ Erro {response.status_code} na busca de IDs: {response.text}")
                break

            data = response.json().get("content", [])
            if not data:
                break

            order_ids += [o["internalWorkOrderId"] for o in data]
            print(f"✅ Página {params['pageNumber']} - {len(data)} ordens")
            params["pageNumber"] += 1
            time.sleep(0.5)

        except Exception as e:
            print(f"⚠️ Erro ao buscar IDs: {e}")
            break

    print(f"🔹 Total de Ordens encontradas: {len(order_ids)}")

    # Consulta os IDs já existentes no banco
    existing_ids = get_existing_order_ids()
    new_order_ids = [oid for oid in order_ids if oid not in existing_ids]

    print(f"🔹 Novas Ordens a serem buscadas: {len(new_order_ids)}")


    # Detalhamento das ordens
    print("📦 Buscando detalhes de cada OS...")
    details = []
    session = requests.Session()

    for idx, order_id in enumerate(new_order_ids, 1):
        order_url = f"{base_url}/{order_id}"
        success = False
        attempts = 0

        while attempts < 5:
            try:
                res = session.get(order_url, headers=HEADERS, timeout=10)
                if res.status_code == 200:
                    order = res.json()
                    order["itemServices"] = len(order.get("itemServices", []))
                    order["itemProducts"] = len(order.get("itemProducts", []))

                    if "completionBy" in order:
                        cb = order["completionBy"]
                        order["completionBy.id"] = cb.get("id")
                        order["completionBy.name"] = cb.get("name")
                        order["completionBy.serialNumber"] = cb.get("serialNumber")

                    details.append(order)
                    success = True
                    break

                elif res.status_code == 429:
                    wait_time = 2 ** attempts + 0.5
                    print(f"⏳ 429 - Aguardando {wait_time:.1f}s (tentativa {attempts+1}) para Ordem {order_id}")
                    time.sleep(wait_time)
                    attempts += 1
                else:
                    print(f"❌ Erro {res.status_code} na Ordem {order_id}")
                    break

            except Exception as e:
                print(f"⚠️ Erro na requisição da Ordem {order_id}: {e}")
                break

        if not success:
            print(f"❌ Falha ao buscar detalhes da Ordem {order_id}")

        if idx % 10 == 0:
            print(f"📊 {idx}/{len(order_ids)} ordens processadas")

        time.sleep(0.3)

    print(f"✅ Total de ordens detalhadas: {len(details)}")
    return details 
