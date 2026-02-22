"""
Coletor TSE - Candidaturas, Bens e Doações de Campanha
Dados: https://dadosabertos.tse.jus.br/
Arquivos CSV por estado/ano, separador ; e encoding latin-1.
"""

import asyncio
import csv
import io
import zipfile
import unicodedata
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele"

ANOS_ELEICAO = [2024, 2022, 2020, 2018, 2016]

# Mapeamento flexível de nomes de colunas do TSE
# O TSE às vezes muda nomes entre anos/versões
COL_CPF_CAND = ["CPF_CANDIDATO", "NR_CPF_CANDIDATO", "CPF"]
COL_NOME_CAND = ["NM_CANDIDATO", "NOME_CANDIDATO", "NM_CAND"]
COL_NOME_URNA = ["NM_URNA_CANDIDATO", "NM_URNA", "NOME_URNA"]
COL_PARTIDO = ["SG_PARTIDO", "PARTIDO", "SG_LEGENDA"]
COL_CARGO = ["DS_CARGO", "DESCRICAO_CARGO", "NM_CARGO"]
COL_UF = ["SG_UF", "UF", "SG_UF_NASCIMENTO"]
COL_MUNICIPIO = ["NM_MUNICIPIO", "MUNICIPIO", "NM_UE"]
COL_IBGE = ["CD_MUNICIPIO", "CODIGO_MUNICIPIO", "SQ_CANDIDATO"]
COL_SITUACAO = ["DS_SIT_TOT_TURNO", "SITUACAO_TURNO", "DS_SITUACAO_CANDIDATURA"]
COL_CPF_BEM = ["CPF_CANDIDATO", "NR_CPF_CANDIDATO"]
COL_TIPO_BEM = ["DS_TIPO_BEM_CANDIDATO", "TIPO_BEM", "DS_TIPO_BEM"]
COL_DESC_BEM = ["DS_BEM_CANDIDATO", "DESCRICAO_BEM", "DS_BEM"]
COL_VR_BEM = ["VR_BEM_CANDIDATO", "VALOR_BEM", "VR_BEM"]
COL_CPF_DOADOR = ["CPF_CNPJ_DOADOR", "NR_CPF_CNPJ_DOADOR", "CNPJ_CPF_DOADOR"]
COL_NOME_DOADOR = ["NM_DOADOR", "NOME_DOADOR"]
COL_VR_RECEITA = ["VR_RECEITA", "VALOR_RECEITA", "VR_RECEITA_ATUALIZADO"]
COL_DT_RECEITA = ["DT_RECEITA", "DATA_RECEITA"]
COL_ORIGEM = ["DS_ORIGEM_RECEITA", "ORIGEM_RECEITA", "DS_ESPECIE_RECEITA"]


def _get_col(row: dict, cols: list[str], default: str = "") -> str:
    """Busca um valor no dict usando lista de nomes alternativos de colunas."""
    for col in cols:
        if col in row and row[col] is not None:
            val = str(row[col]).strip()
            if val:
                return val
    return default


class TSECollector:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=120.0,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            },
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    def _normalizar_nome(self, nome: str) -> str:
        if not nome:
            return ""
        nfkd = unicodedata.normalize("NFKD", nome.upper().strip())
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    def _limpar_cpf(self, cpf: str) -> str:
        return "".join(filter(str.isdigit, cpf or ""))

    def _parse_valor(self, s: str) -> float:
        try:
            return float(str(s).replace(".", "").replace(",", ".").strip())
        except (ValueError, AttributeError):
            return 0.0

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15))
    async def _download_csv(self, url: str) -> list[dict]:
        """Download e parse de CSV zippado do TSE."""
        try:
            logger.debug(f"Baixando: {url}")
            r = await self.client.get(url)

            if r.status_code == 404:
                return []
            r.raise_for_status()

            content = r.content

            # Extrai CSV do ZIP
            if url.endswith(".zip") or content[:2] == b"PK":
                try:
                    with zipfile.ZipFile(io.BytesIO(content)) as z:
                        csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
                        if not csv_files:
                            logger.warning(f"ZIP sem CSV: {url} | arquivos: {z.namelist()}")
                            return []
                        # Pega o maior CSV (o principal, ignorando índices)
                        csv_file = sorted(csv_files, key=lambda f: z.getinfo(f).file_size, reverse=True)[0]
                        with z.open(csv_file) as f:
                            content = f.read()
                except zipfile.BadZipFile:
                    logger.warning(f"ZIP corrompido: {url}")
                    return []

            # Tenta decodificar — TSE usa latin-1 historicamente
            text = None
            for encoding in ["latin-1", "cp1252", "utf-8-sig", "utf-8"]:
                try:
                    text = content.decode(encoding)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue

            if text is None:
                logger.warning(f"Não foi possível decodificar: {url}")
                return []

            # Remove BOM se presente (corrompe o nome da primeira coluna)
            text = text.lstrip("\ufeff").lstrip("\ufeff")

            # Lê o CSV
            reader = csv.DictReader(
                io.StringIO(text),
                delimiter=";",
                quotechar='"',
                skipinitialspace=True,
            )

            try:
                rows = list(reader)
            except Exception as e:
                logger.warning(f"Erro ao parsear CSV {url}: {e}")
                return []

            if rows:
                # Normaliza chaves: remove espaços e BOM residual
                rows = [
                    {k.strip().lstrip("\ufeff"): v for k, v in row.items()}
                    for row in rows
                ]
                logger.debug(f"TSE CSV {url}: {len(rows)} linhas | colunas: {list(rows[0].keys())[:5]}")

            return rows

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            logger.error(f"TSE HTTP {e.response.status_code}: {url}")
            raise
        except Exception as e:
            logger.error(f"TSE erro {url}: {type(e).__name__}: {e}")
            raise

    async def coletar_candidatos(self, uf: str, ano: int = 2024) -> list[dict]:
        url = f"{BASE_URL}/consulta_cand/consulta_cand_{ano}_{uf.upper()}.zip"
        rows = await self._download_csv(url)

        politicos = []
        for row in rows:
            cpf = self._limpar_cpf(_get_col(row, COL_CPF_CAND))
            nome = _get_col(row, COL_NOME_CAND)

            if not cpf or not nome:
                continue

            situacao = _get_col(row, COL_SITUACAO)
            eleito = situacao.upper() in [
                "ELEITO", "ELEITO POR QP", "ELEITO POR MÉDIA",
                "ELEITO POR QP ", "MÉDIA", "ELEITO POR MEDIA",
            ]

            politicos.append({
                "cpf": cpf,
                "nome": nome,
                "nome_normalizado": self._normalizar_nome(nome),
                "nome_urna": _get_col(row, COL_NOME_URNA),
                "partido": _get_col(row, COL_PARTIDO),
                "cargo": _get_col(row, COL_CARGO),
                "uf": _get_col(row, COL_UF) or uf.upper(),
                "municipio": _get_col(row, COL_MUNICIPIO),
                "codigo_ibge": _get_col(row, COL_IBGE),
                "situacao_candidatura": situacao,
                "eleito": eleito,
                "ano_eleicao": ano,
            })

        logger.info(f"TSE candidatos {uf}/{ano}: {len(politicos)} registros")
        return politicos

    async def coletar_bens(self, uf: str, ano: int = 2024) -> list[dict]:
        url = f"{BASE_URL}/bem_candidato/bem_candidato_{ano}_{uf.upper()}.zip"
        rows = await self._download_csv(url)

        bens = []
        for row in rows:
            cpf = self._limpar_cpf(_get_col(row, COL_CPF_BEM))
            if not cpf:
                continue

            bens.append({
                "cpf": cpf,
                "tipo_bem": _get_col(row, COL_TIPO_BEM),
                "descricao": _get_col(row, COL_DESC_BEM),
                "valor": self._parse_valor(_get_col(row, COL_VR_BEM, "0")),
                "ano_eleicao": ano,
            })

        logger.info(f"TSE bens {uf}/{ano}: {len(bens)} registros")
        return bens

    async def coletar_doacoes(self, uf: str, ano: int = 2024) -> list[dict]:
        url = f"{BASE_URL}/prestacao_contas/receitas_candidatos_{ano}_{uf.upper()}.zip"
        rows = await self._download_csv(url)

        doacoes = []
        for row in rows:
            cpf_cand = self._limpar_cpf(_get_col(row, COL_CPF_CAND))
            if not cpf_cand:
                continue

            doacoes.append({
                "cpf_cnpj_doador": self._limpar_cpf(_get_col(row, COL_CPF_DOADOR)),
                "nome_doador": _get_col(row, COL_NOME_DOADOR),
                "cpf_candidato": cpf_cand,
                "nome_candidato": _get_col(row, COL_NOME_CAND),
                "partido": _get_col(row, COL_PARTIDO),
                "cargo": _get_col(row, COL_CARGO),
                "uf": uf.upper(),
                "ano_eleicao": ano,
                "valor": self._parse_valor(_get_col(row, COL_VR_RECEITA, "0")),
                "data_doacao": _get_col(row, COL_DT_RECEITA),
                "tipo_receita": _get_col(row, COL_ORIGEM),
            })

        logger.info(f"TSE doações {uf}/{ano}: {len(doacoes)} registros")
        return doacoes

    async def coletar_uf_completo(self, uf: str, anos: list[int] = None) -> dict:
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

                await asyncio.sleep(0.5)

            except Exception as e:
                logger.warning(f"Erro TSE {uf}/{ano}: {e}")
                continue

        # Consolida patrimônio por CPF
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
