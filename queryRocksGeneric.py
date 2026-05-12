import requests
import time
import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================================================
# CONFIG
# ==================================================
URL_TOKEN = 'https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp'
URL_QUERY = "https://prodesp.id.cyberark.cloud/Redrock/query"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

BASE_NOME_ARQUIVO = "baserole"

PAGE_SIZE = 5000
RETRY = 3

# 🔥 THREADS
MAX_WORKERS = 3

# 🔥 limite Excel
LIMITE_LINHAS_ARQUIVO = 500000

# 🔥 grupos balanceados
GRUPOS = [
    "0123",
    "456",
    "789"
]

# ==================================================
# TOKEN
# ==================================================
def gerar_token():

    response = requests.post(
        URL_TOKEN,
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": "all"
        },
        timeout=60
    )

    if response.status_code != 200:
        raise Exception(f"Erro token: {response.text}")

    return response.json()["access_token"]

# ==================================================
# REQUEST
# ==================================================
def buscar_pagina(headers, grupo, pagina):

    filtros = " OR ".join(
        [f"LOWER(User.Username) LIKE '{letra}%'" for letra in grupo]
    )

    script = f"""
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
    WHERE
        {filtros}
    """

    body = {
        "Script": script,
        "Args": {
            "PageNumber": pagina,
            "PageSize": PAGE_SIZE,
            "Caching": -1
        }
    }

    for tentativa in range(RETRY):

        try:

            print(
                f"📄 [{grupo}] Página {pagina} (tentativa {tentativa+1})",
                flush=True
            )

            response = requests.post(
                URL_QUERY,
                json=body,
                headers=headers,
                timeout=120
            )

            if response.status_code == 401:
                raise Exception("Token expirado")

            if response.status_code != 200:
                raise Exception(
                    f"HTTP {response.status_code} - {response.text}"
                )

            resposta = response.json()

            resultados = resposta.get("Result", {}).get("Results", [])

            linhas = [item.get("Row", {}) for item in resultados]

            print(
                f"✔ [{grupo}] Página {pagina}: {len(linhas)} registros",
                flush=True
            )

            return linhas

        except Exception as e:

            print(
                f"⚠️ [{grupo}] Página {pagina} erro: {e}",
                flush=True
            )

            time.sleep(5)

    print(
        f"❌ [{grupo}] Página {pagina} falhou",
        flush=True
    )

    return []

# ==================================================
# PROCESSA GRUPO
# ==================================================
def processar_grupo(grupo, headers):

    print(
        f"\n🚀 PROCESSANDO GRUPO [{grupo}]",
        flush=True
    )

    dados_grupo = []

    pagina = 1

    while True:

        linhas = buscar_pagina(
            headers=headers,
            grupo=grupo,
            pagina=pagina
        )

        if len(linhas) == 0:

            print(
                f"🏁 Fim grupo [{grupo}]",
                flush=True
            )

            break

        dados_grupo.extend(linhas)

        print(
            f"📊 [{grupo}] Total acumulado: {len(dados_grupo)}",
            flush=True
        )

        if len(linhas) < PAGE_SIZE:

            print(
                f"🏁 Última página grupo [{grupo}]",
                flush=True
            )

            break

        pagina += 1

        time.sleep(0.3)

    return dados_grupo

# ==================================================
# EXTRAÇÃO
# ==================================================
def extrair_usuarios():

    token = gerar_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    total_geral = 0

    # 🔥 CONTROLE ARQUIVOS
    parte = 1
    linhas_arquivo = 0

    nome_arquivo = f"{BASE_NOME_ARQUIVO}_{parte}.csv"

    arquivo = open(
        nome_arquivo,
        mode="w",
        newline='',
        encoding='utf-8'
    )

    writer = None

    print(
        f"📦 Criando arquivo: {nome_arquivo}",
        flush=True
    )

    # ==================================================
    # THREADS
    # ==================================================
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

        futures = [
            executor.submit(processar_grupo, grupo, headers)
            for grupo in GRUPOS
        ]

        for future in as_completed(futures):

            dados_grupo = future.result()

            if not dados_grupo:
                continue

            # 🔥 cria cabeçalho
            if writer is None:

                campos = dados_grupo[0].keys()

                writer = csv.DictWriter(
                    arquivo,
                    fieldnames=campos,
                    delimiter=';'
                )

                writer.writeheader()

            # ==================================================
            # ESCREVE LINHAS
            # ==================================================
            for linha in dados_grupo:

                writer.writerow(linha)

                linhas_arquivo += 1
                total_geral += 1

                # 🔥 troca arquivo
                if linhas_arquivo >= LIMITE_LINHAS_ARQUIVO:

                    arquivo.close()

                    parte += 1
                    linhas_arquivo = 0

                    nome_arquivo = f"{BASE_NOME_ARQUIVO}_{parte}.csv"

                    arquivo = open(
                        nome_arquivo,
                        mode="w",
                        newline='',
                        encoding='utf-8'
                    )

                    writer = csv.DictWriter(
                        arquivo,
                        fieldnames=campos,
                        delimiter=';'
                    )

                    writer.writeheader()

                    print(
                        f"\n📦 Novo arquivo: {nome_arquivo}",
                        flush=True
                    )

            print(
                f"📊 TOTAL GERAL: {total_geral}",
                flush=True
            )

    arquivo.close()

    print(
        f"\n🎯 FINALIZADO!",
        flush=True
    )

    print(
        f"📊 TOTAL FINAL: {total_geral}",
        flush=True
    )

# ==================================================
# EXECUÇÃO
# ==================================================
if __name__ == "__main__":
    extrair_usuarios()
