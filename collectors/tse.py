"""
Coletor TSE - Candidaturas, Bens e Doações de Campanha
Dados: https://dadosabertos.tse.jus.br/
Arquivos CSV por estado/ano, atualizados a cada eleição.
"""

import asyncio
import csv
import io
import zipfile
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele"

# Anos de eleição disponíveis
ANOS_ELEICAO = [2024, 2022, 2020, 2018, 2016]

UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "BR",
]


class TSECollector:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=120.0,
            follow_redirects=True,
            headers={"User-Agent": "LicitacaoRadar/1.0"},
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    def _normalizar_nome(self, nome: str) -> str:
        import unicodedata
        if not nome:
            return ""
        nfkd = unicodedata.normalize("NFKD", nome.upper().strip())
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    def _limpar_cpf(self, cpf: str) -> str:
        return "".join(filter(str.isdigit, cpf or ""))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15))
    async def _download_csv(self, url: str) -> list[dict]:
        """Download e parse de CSV (possivelmente zippado) do TSE."""
        try:
            logger.debug(f"Baixando: {url}")
            r = await self.client.get(url)
            r.raise_for_status()

            content = r.content

            # TSE serve ZIP com CSV dentro
            if url.endswith(".zip") or content[:2] == b"PK":
                with zipfile.ZipFile(io.BytesIO(content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith(".csv")]
                    if not csv_files:
                        return []
                    with z.open(csv_files[0]) as f:
                        content = f.read()

            # decodifica (TSE usa latin-1)
            for encoding in ["latin-1", "utf-8", "cp1252"]:
                try:
                    text = content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                return []

            reader = csv.DictReader(
                io.StringIO(text),
                delimiter=";",
                quotechar='"',
            )
            return [row for row in reader]

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise

    async def coletar_candidatos(self, uf: str, ano: int = 2024) -> list[dict]:
        """
        Baixa dados de candidatos por UF e ano.
        Retorna lista de políticos normalizados.
        """
        url = f"{BASE_URL}/consulta_cand/consulta_cand_{ano}_{uf.upper()}.zip"
        rows = await self._download_csv(url)

        politicos = []
        for row in rows:
            cpf = self._limpar_cpf(row.get("CPF_CANDIDATO", ""))
            nome = row.get("NM_CANDIDATO", "").strip()

            if not cpf or not nome:
                continue

            politicos.append({
                "cpf": cpf,
                "nome": nome,
                "nome_normalizado": self._normalizar_nome(nome),
                "nome_urna": row.get("NM_URNA_CANDIDATO", "").strip(),
                "partido": row.get("SG_PARTIDO", ""),
                "cargo": row.get("DS_CARGO", ""),
                "uf": row.get("SG_UF", ""),
                "municipio": row.get("NM_MUNICIPIO", ""),
                "codigo_ibge": row.get("CD_MUNICIPIO", ""),
                "situacao_candidatura": row.get("DS_SIT_TOT_TURNO", ""),
                "eleito": row.get("DS_SIT_TOT_TURNO", "").upper() in [
                    "ELEITO", "ELEITO POR QP", "ELEITO POR MÉDIA"
                ],
                "ano_eleicao": ano,
            })

        logger.info(f"TSE candidatos {uf}/{ano}: {len(politicos)} registros")
        return politicos

    async def coletar_bens(self, uf: str, ano: int = 2024) -> list[dict]:
        """Baixa bens declarados pelos candidatos."""
        url = f"{BASE_URL}/bem_candidato/bem_candidato_{ano}_{uf.upper()}.zip"
        rows = await self._download_csv(url)

        bens = []
        for row in rows:
            cpf = self._limpar_cpf(row.get("CPF_CANDIDATO", ""))
            if not cpf:
                continue

            try:
                valor = float(
                    row.get("VR_BEM_CANDIDATO", "0")
                    .replace(".", "")
                    .replace(",", ".")
                )
            except (ValueError, AttributeError):
                valor = 0

            bens.append({
                "cpf": cpf,
                "tipo_bem": row.get("DS_TIPO_BEM_CANDIDATO", ""),
                "descricao": row.get("DS_BEM_CANDIDATO", ""),
                "valor": valor,
                "ano_eleicao": ano,
            })

        logger.info(f"TSE bens {uf}/{ano}: {len(bens)} registros")
        return bens

    async def coletar_doacoes(self, uf: str, ano: int = 2024) -> list[dict]:
        """Baixa doações de campanha (receitas dos candidatos)."""
        url = f"{BASE_URL}/prestacao_contas/receitas_candidatos_{ano}_{uf.upper()}.zip"
        rows = await self._download_csv(url)

        doacoes = []
        for row in rows:
            cpf_cand = self._limpar_cpf(row.get("CPF_CANDIDATO", ""))
            cpf_cnpj_doador = self._limpar_cpf(row.get("CPF_CNPJ_DOADOR", ""))
            if not cpf_cand:
                continue

            try:
                valor = float(
                    row.get("VR_RECEITA", "0")
                    .replace(".", "")
                    .replace(",", ".")
                )
            except (ValueError, AttributeError):
                valor = 0

            doacoes.append({
                "cpf_cnpj_doador": cpf_cnpj_doador,
                "nome_doador": row.get("NM_DOADOR", "").strip(),
                "cpf_candidato": cpf_cand,
                "nome_candidato": row.get("NM_CANDIDATO", "").strip(),
                "partido": row.get("SG_PARTIDO", ""),
                "cargo": row.get("DS_CARGO", ""),
                "uf": uf.upper(),
                "ano_eleicao": ano,
                "valor": valor,
                "data_doacao": row.get("DT_RECEITA"),
                "tipo_receita": row.get("DS_ORIGEM_RECEITA", ""),
            })

        logger.info(f"TSE doações {uf}/{ano}: {len(doacoes)} registros")
        return doacoes

    async def coletar_uf_completo(self, uf: str, anos: list[int] = None) -> dict:
        """Coleta candidatos + bens + doações de todos os anos para uma UF."""
        if anos is None:
            anos = ANOS_ELEICAO

        todos_politicos = []
        todos_bens = []
        todas_doacoes = []

        for ano in anos:
            try:
                politicos = await self.coletar_candidatos(uf, ano)
                todos_politicos.extend(politicos)

                bens = await self.coletar_bens(uf, ano)
                todos_bens.extend(bens)

                doacoes = await self.coletar_doacoes(uf, ano)
                todas_doacoes.extend(doacoes)

                await asyncio.sleep(1)

            except Exception as e:
                logger.warning(f"Erro coletando TSE {uf}/{ano}: {e}")
                continue

        # consolida patrimônio por CPF (soma de todos os bens do último ano)
        patrimonio_por_cpf: dict[str, float] = {}
        for bem in todos_bens:
            cpf = bem["cpf"]
            patrimonio_por_cpf[cpf] = patrimonio_por_cpf.get(cpf, 0) + bem["valor"]

        for p in todos_politicos:
            p["patrimonio_declarado"] = patrimonio_por_cpf.get(p["cpf"], 0)

        return {
            "politicos": todos_politicos,
            "bens": todos_bens,
            "doacoes": todas_doacoes,
        }
