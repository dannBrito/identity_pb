import requests
import time
import os
import csv

# ==================================================
# CONFIG
# ==================================================
URL_TOKEN = 'https://prodesp.id.cyberark.cloud/OAuth2/Token/PainelProdesp'
URL_QUERY = "https://prodesp.id.cyberark.cloud/Redrock/query"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

BASE_NOME_ARQUIVO = "baseusers"

PAGE_SIZE = 10000
RETRY = 3

#  limite seguro Excel
LIMITE_LINHAS_ARQUIVO = 500000

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
# BUSCAR PÁGINA
# ==================================================
def buscar_pagina(pagina, headers):

    script = """
    SELECT
        User.Username,
        User.LastLogin,
        User.Beneficiario_
    FROM
        User
    ORDER BY
        User.Username
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
                f" Página {pagina} (tentativa {tentativa+1})",
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

            resultados = resposta.get(
                "Result",
                {}
            ).get(
                "Results",
                []
            )

            linhas = [
                item.get("Row", {})
                for item in resultados
            ]

            print(
                f"✔ Página {pagina}: {len(linhas)} registros",
                flush=True
            )

            return linhas

        except Exception as e:

            print(
                f" Página {pagina} erro: {e}",
                flush=True
            )

            time.sleep(5)

    print(
        f" Página {pagina} falhou após retries",
        flush=True
    )

    return []

# ==================================================
# EXTRAÇÃO
# ==================================================
def extrair_usuarios():

    token = gerar_token()

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    pagina = 1

    parte = 1
    linhas_no_arquivo = 0
    total_geral = 0

    nome_arquivo = f"{BASE_NOME_ARQUIVO}_{parte}.csv"

    arquivo = open(
        nome_arquivo,
        mode="w",
        newline='',
        encoding='utf-8'
    )

    writer = None

    print(
        f" Criando arquivo: {nome_arquivo}",
        flush=True
    )

    while True:

        linhas = buscar_pagina(
            pagina=pagina,
            headers=headers
        )

        # fim paginação
        if len(linhas) == 0:

            print(
                "🏁 Nenhum registro retornado. Finalizando.",
                flush=True
            )

            break

        #  cria cabeçalho
        if writer is None:

            campos = linhas[0].keys()

            writer = csv.DictWriter(
                arquivo,
                fieldnames=campos,
                delimiter=';'
            )

            writer.writeheader()

        #  escreve linhas
        writer.writerows(linhas)

        linhas_no_arquivo += len(linhas)
        total_geral += len(linhas)

        print(
            f" Página atual: {pagina}",
            flush=True
        )

        print(
            f" Linhas no arquivo: {linhas_no_arquivo}",
            flush=True
        )

        print(
            f" Total geral: {total_geral}",
            flush=True
        )

        # ==================================================
        # TROCA ARQUIVO
        # ==================================================
        if linhas_no_arquivo >= LIMITE_LINHAS_ARQUIVO:

            arquivo.close()

            parte += 1
            linhas_no_arquivo = 0

            nome_arquivo = f"{BASE_NOME_ARQUIVO}_{parte}.csv"

            arquivo = open(
                nome_arquivo,
                mode="w",
                newline='',
                encoding='utf-8'
            )

            writer = None

            print(
                f"\n Novo arquivo: {nome_arquivo}",
                flush=True
            )

        #  última página
        if len(linhas) < PAGE_SIZE:

            print(
                " Última página detectada.",
                flush=True
            )

            break

        pagina += 1

        time.sleep(0.3)

    arquivo.close()

    print(
        f"\n FINALIZADO!",
        flush=True
    )

    print(
        f" TOTAL FINAL: {total_geral}",
        flush=True
    )

# ==================================================
# EXECUÇÃO
# ==================================================
if __name__ == "__main__":
    extrair_usuarios()
