import requests
import pandas as pd
import time
import os

# ===== CONFIG =====
URL_TOKEN = 'https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp'
URL_QUERY = "https://prodesp.id.cyberark.cloud/Redrock/query"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

BASE_NOME_ARQUIVO = "baserole"

#  CONFIG OTIMIZADA
PAGE_SIZE = 5000
MAX_PAGINAS = 50   # evita timeout no GitHub
SLEEP = 0.3
RETRY = 3

# ===== TOKEN =====
def gerar_token():
    body = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials",
        "scope": "all"
    }

    r = requests.post(URL_TOKEN, data=body)

    if r.status_code != 200:
        raise Exception(f"Erro ao gerar token: {r.text}")

    return r.json()["access_token"]

# ===== REQUEST COM RETRY =====
def fazer_request(url, body, headers):
    for i in range(RETRY):
        try:
            r = requests.post(url, json=body, headers=headers, timeout=120)
            return r
        except Exception as e:
            print(f" Tentativa {i+1} falhou: {e}", flush=True)
            time.sleep(2)
    raise Exception(" Falha após várias tentativas")

# ===== EXTRAÇÃO =====
def extrair_usuarios():

    headers = {
        "Authorization": f"Bearer {gerar_token()}",
        "Content-Type": "application/json"
    }

    pagina = 1
    parte = 1
    linhas_na_parte = 0
    primeiro_bloco = True
    total_geral = 0

    while True:

        # 🔥 controle de execução
        if pagina > MAX_PAGINAS:
            print(" Parando execução controlada (limite GitHub)", flush=True)
            break

        print(f"\n Página {pagina}", flush=True)

        script = """
        SELECT
            User.Username,
            User.ID AS UserId,
            Role.Name AS RoleName,
            Role.ID AS RoleId
        FROM
            RoleMember
        INNER JOIN User
            ON User.ID = split_part(RoleMember.ID, '_', 1)
        INNER JOIN Role
            ON Role.ID = regexp_replace(RoleMember.ID, '^[^_]+_', '')
        ORDER BY
            User.Username,
            Role.Name
        """

        body = {
            "Script": script,
            "Args": {
                "PageNumber": pagina,
                "PageSize": PAGE_SIZE,
                "Caching": -1
            }
        }

        r = fazer_request(URL_QUERY, body, headers)

        if r.status_code == 401:
            print(" Token expirado, renovando...", flush=True)
            headers["Authorization"] = f"Bearer {gerar_token()}"
            continue

        if r.status_code != 200:
            print(" Erro HTTP:", r.status_code, flush=True)
            print(r.text, flush=True)
            break

        resposta = r.json()

        if not resposta.get("success"):
            print(" Erro na query:", resposta, flush=True)
            break

        resultados = resposta.get("Result", {}).get("Results", [])
        qtd = len(resultados)

        print(f"   ➜ Recebidos: {qtd}", flush=True)

        if qtd == 0:
            print(" Fim da extração.", flush=True)
            break

        linhas = [item.get("Row", {}) for item in resultados]
        df = pd.DataFrame(linhas)

        #  troca de arquivo se necessário
        if linhas_na_parte >= 1000000:
            parte += 1
            linhas_na_parte = 0
            primeiro_bloco = True
            print(f" Novo arquivo: {BASE_NOME_ARQUIVO}_{parte}.csv", flush=True)

        nome_arquivo = f"{BASE_NOME_ARQUIVO}_{parte}.csv"

        df.to_csv(
            nome_arquivo,
            mode="w" if primeiro_bloco else "a",
            index=False,
            header=primeiro_bloco,
            sep=";"
        )

        primeiro_bloco = False
        linhas_na_parte += qtd
        total_geral += qtd

        print(f"    Total acumulado: {total_geral}", flush=True)

        pagina += 1
        time.sleep(SLEEP)

    print(f"\n Finalizado! Total: {total_geral}", flush=True)

# ===== EXECUÇÃO =====
if __name__ == "__main__":
    extrair_usuarios()
