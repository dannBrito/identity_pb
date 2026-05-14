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

BASE_NOME_ARQUIVO = "baseusers"

PAGE_SIZE = 10000
MAX_WORKERS = 3   
LOTE_PAGINAS = 20
RETRY = 3

# ===== TOKEN =====
def gerar_token():
    r = requests.post(URL_TOKEN, data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "all"
    })
    return r.json()["access_token"]

# ===== BUSCAR PÁGINA (COM RETRY) =====
def buscar_pagina(pagina, headers):

    script = f"""
    SELECT
        User.Username,
        User.LastLogin
    FROM
        User
"""

    for tentativa in range(RETRY):
        try:
            print(f"📄 Página {pagina} (tentativa {tentativa+1})", flush=True)

            body = {
                "Script": script,
                "Args": {
                    "PageNumber": pagina,
                    "PageSize": PAGE_SIZE,
                    "Caching": -1
                }
            }

            r = requests.post(URL_QUERY, json=body, headers=headers, timeout=120)

            if r.status_code == 200:
                resultados = r.json().get("Result", {}).get("Results", [])
                linhas = [item.get("Row", {}) for item in resultados]

                print(f"✔ Página {pagina}: {len(linhas)} registros", flush=True)
                return pagina, linhas

        except Exception as e:
            print(f"⚠️ Página {pagina} erro: {e}", flush=True)
            time.sleep(3)

    print(f"❌ Página {pagina} falhou após retries", flush=True)
    return pagina, []

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

    continuar = True

    while continuar:

        paginas = range(pagina_inicial, pagina_inicial + LOTE_PAGINAS)

        print(f"\n🚀 Lote {pagina_inicial} até {pagina_inicial + LOTE_PAGINAS - 1}", flush=True)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(buscar_pagina, p, headers) for p in paginas]

            for future in as_completed(futures):

                pagina, linhas = future.result()

                if len(linhas) == 0:
                    print(f"🏁 Página {pagina} retornou 0 → fim dos dados", flush=True)
                    continuar = False
                    break

                # cria cabeçalho
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

    print(f"\n🎯 FINALIZADO! Total: {total_geral}", flush=True)

# ===== EXECUÇÃO =====
if __name__ == "__main__":
    extrair_usuarios()
