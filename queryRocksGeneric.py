import requests
import time
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ===== CONFIG =====
URL_TOKEN = 'https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp'
URL_QUERY = "https://prodesp.id.cyberark.cloud/Redrock/query"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

BASE_NOME_ARQUIVO = "baserole"

PAGE_SIZE = 3000
RETRY = 3
MAX_WORKERS = 3
LOTE_PAGINAS = 20

# ===== TOKEN =====
def gerar_token():
    r = requests.post(URL_TOKEN, data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "all"
    })
    return r.json()["access_token"]

# ===== REQUEST =====
def fazer_request(body, headers):
    for i in range(RETRY):
        try:
            return requests.post(URL_QUERY, json=body, headers=headers, timeout=120)
        except Exception as e:
            print(f"⚠️ Tentativa {i+1} falhou: {e}", flush=True)
            time.sleep(2)
    return None

# ===== BUSCAR PÁGINA =====
def buscar_pagina(pagina, headers):

    print(f"📄 Página {pagina}", flush=True)

    script = """
    SELECT
        User.Username,
        User.ID AS UserId,
        User.Status AS UserStatus,
        User.LastLogin,
        Role.Name AS RoleName,
        Role.ID AS RoleId
    FROM
        RoleMember
    INNER JOIN User
        ON User.ID = split_part(RoleMember.ID, '_', 1)
    INNER JOIN Role
        ON Role.ID = regexp_replace(RoleMember.ID, '^[^_]+_', '')
    """

    body = {
        "Script": script,
        "Args": {
            "PageNumber": pagina,
            "PageSize": PAGE_SIZE,
            "Caching": -1
        }
    }

    r = fazer_request(body, headers)

    if not r or r.status_code != 200:
        print(f"❌ Erro página {pagina}", flush=True)
        return []

    resultados = r.json().get("Result", {}).get("Results", [])

    linhas = [item.get("Row", {}) for item in resultados]

    print(f"   ✔ Página {pagina}: {len(linhas)} registros", flush=True)

    return linhas

# ===== EXTRAÇÃO =====
def extrair_usuarios():

    headers = {
        "Authorization": f"Bearer {gerar_token()}",
        "Content-Type": "application/json"
    }

    pagina_inicial = 1
    parte = 1
    linhas_na_parte = 0
    total_geral = 0

    nome_arquivo = f"{BASE_NOME_ARQUIVO}_{parte}.csv"

    arquivo = open(nome_arquivo, mode="w", newline='', encoding='utf-8')
    writer = None

    while True:

        paginas = range(pagina_inicial, pagina_inicial + LOTE_PAGINAS)

        print(f"\n🚀 Lote {pagina_inicial} até {pagina_inicial + LOTE_PAGINAS - 1}", flush=True)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(buscar_pagina, p, headers) for p in paginas]

            for future in as_completed(futures):

                linhas = future.result()

                if not linhas:
                    continue

                # cria cabeçalho na primeira vez
                if writer is None:
                    campos = linhas[0].keys()
                    writer = csv.DictWriter(arquivo, fieldnames=campos, delimiter=';')
                    writer.writeheader()

                writer.writerows(linhas)

                linhas_na_parte += len(linhas)
                total_geral += len(linhas)

                print(f"📊 Total acumulado: {total_geral}", flush=True)

                # troca de arquivo
                if linhas_na_parte >= 500000:
                    arquivo.close()

                    parte += 1
                    linhas_na_parte = 0

                    nome_arquivo = f"{BASE_NOME_ARQUIVO}_{parte}.csv"
                    arquivo = open(nome_arquivo, mode="w", newline='', encoding='utf-8')
                    writer = None

                    print(f"📦 Novo arquivo: {nome_arquivo}", flush=True)

        pagina_inicial += LOTE_PAGINAS

    arquivo.close()

# ===== EXECUÇÃO =====
if __name__ == "__main__":
    extrair_usuarios()
