"""
Coletor TSE - Candidaturas, Bens e Doações de Campanha
Dados: https://dadosabertos.tse.jus.br/

IMPORTANTE: Os arquivos TSE são servidos como CSV com separador ";"
e encoding latin-1. O arquivo ZIP pode conter um único CSV.
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


def _get_col(row: dict, *nomes: str, default: str = "") -> str:
    """Busca o primeiro nome encontrado no dict (insensível a BOM e espaço)."""
    for nome in nomes:
        if nome in row and row[nome] is not None:
            val = str(row[nome]).strip()
            if val:
                return val
    return default


def _limpar_cpf(cpf: str) -> str:
    return "".join(filter(str.isdigit, cpf or ""))


def _parse_valor(s: str) -> float:
    try:
        return float(str(s).replace(".", "").replace(",", ".").strip())
    except (ValueError, AttributeError):
        return 0.0


def _normalizar_nome(nome: str) -> str:
    if not nome:
        return ""
    nfkd = unicodedata.normalize("NFKD", nome.upper().strip())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


class TSECollector:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=120.0,
            follow_redirects=True,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                )
            },
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=15))
    async def _download_csv(self, url: str) -> list[dict]:
        """Download e parse de CSV zippado do TSE."""
        try:
            logger.debug(f"Baixando: {url}")
            r = await self.client.get(url)

            if r.status_code == 404:
                logger.debug(f"TSE 404 (arquivo não existe): {url}")
                return []

            r.raise_for_status()

            content = r.content
            logger.debug(f"TSE download OK: {url} | tamanho={len(content)} bytes")

            # Descompacta ZIP
            if len(content) > 2 and content[:2] == b"PK":
                try:
                    with zipfile.ZipFile(io.BytesIO(content)) as z:
                        csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
                        if not csv_files:
                            logger.warning(f"TSE ZIP sem CSV: {url} | arquivos={z.namelist()}")
                            return []
                        # Pega o maior CSV
                        csv_file = sorted(
                            csv_files,
                            key=lambda f: z.getinfo(f).file_size,
                            reverse=True
                        )[0]
                        logger.debug(f"TSE extraindo: {csv_file} ({z.getinfo(csv_file).file_size} bytes)")
                        with z.open(csv_file) as f:
                            content = f.read()
                except zipfile.BadZipFile:
                    logger.warning(f"TSE ZIP corrompido: {url}")
                    return []
            else:
                logger.warning(f"TSE resposta não é ZIP: {url} | primeiros bytes={content[:20]}")
                return []

            # Decodifica — TSE usa latin-1
            text = None
            for encoding in ["latin-1", "cp1252", "utf-8-sig", "utf-8"]:
                try:
                    text = content.decode(encoding)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue

            if text is None:
                logger.warning(f"TSE falha ao decodificar: {url}")
                return []

            # Remove BOM
            text = text.lstrip("\ufeff")

            # Verifica se tem conteúdo
            linhas = text.strip().splitlines()
            logger.debug(f"TSE CSV: {len(linhas)} linhas | primeira={linhas[0][:120] if linhas else 'vazio'}")

            if len(linhas) < 2:
                logger.warning(f"TSE CSV vazio ou só header: {url}")
                return []

            reader = csv.DictReader(
                io.StringIO(text),
                delimiter=";",
                quotechar='"',
                skipinitialspace=True,
            )

            try:
                rows = list(reader)
            except Exception as e:
                logger.warning(f"TSE erro ao parsear CSV {url}: {e}")
                return []

            # Normaliza chaves (remove BOM e espaços)
            rows = [
                {k.strip().lstrip("\ufeff"): v for k, v in row.items()}
                for row in rows
            ]

            if rows:
                logger.debug(f"TSE: {len(rows)} linhas | colunas={list(rows[0].keys())[:6]}")

            return rows

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise
        except Exception as e:
            logger.error(f"TSE erro {url}: {type(e).__name__}: {e}")
            raise

    async def coletar_candidatos(self, uf: str, ano: int = 2024) -> list[dict]:
        url = f"{BASE_URL}/consulta_cand/consulta_cand_{ano}_{uf.upper()}.zip"
        rows = await self._download_csv(url)

        politicos = []
        for row in rows:
            cpf = _limpar_cpf(_get_col(row, "CPF_CANDIDATO", "NR_CPF_CANDIDATO", "CPF"))
            nome = _get_col(row, "NM_CANDIDATO", "NOME_CANDIDATO", "NM_CAND")
            if not cpf or not nome:
                continue

            situacao = _get_col(row, "DS_SIT_TOT_TURNO", "SITUACAO_TURNO", "DS_SITUACAO_CANDIDATURA")
            eleito = situacao.upper() in [
                "ELEITO", "ELEITO POR QP", "ELEITO POR MÉDIA",
                "ELEITO POR MEDIA", "MÉDIA",
            ]

            politicos.append({
                "cpf": cpf,
                "nome": nome,
                "nome_normalizado": _normalizar_nome(nome),
                "nome_urna": _get_col(row, "NM_URNA_CANDIDATO", "NM_URNA", "NOME_URNA"),
                "partido": _get_col(row, "SG_PARTIDO", "PARTIDO", "SG_LEGENDA"),
                "cargo": _get_col(row, "DS_CARGO", "DESCRICAO_CARGO", "NM_CARGO"),
                "uf": _get_col(row, "SG_UF", "UF") or uf.upper(),
                "municipio": _get_col(row, "NM_MUNICIPIO", "MUNICIPIO", "NM_UE"),
                "codigo_ibge": _get_col(row, "CD_MUNICIPIO", "SQ_CANDIDATO"),
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
            cpf = _limpar_cpf(_get_col(row, "CPF_CANDIDATO", "NR_CPF_CANDIDATO"))
            if not cpf:
                continue
            bens.append({
                "cpf": cpf,
                "tipo_bem": _get_col(row, "DS_TIPO_BEM_CANDIDATO", "TIPO_BEM", "DS_TIPO_BEM"),
                "descricao": _get_col(row, "DS_BEM_CANDIDATO", "DESCRICAO_BEM", "DS_BEM"),
                "valor": _parse_valor(_get_col(row, "VR_BEM_CANDIDATO", "VALOR_BEM", "VR_BEM", default="0")),
                "ano_eleicao": ano,
            })

        logger.info(f"TSE bens {uf}/{ano}: {len(bens)} registros")
        return bens

    async def coletar_doacoes(self, uf: str, ano: int = 2024) -> list[dict]:
        url = f"{BASE_URL}/prestacao_contas/receitas_candidatos_{ano}_{uf.upper()}.zip"
        rows = await self._download_csv(url)

        doacoes = []
        for row in rows:
            cpf_cand = _limpar_cpf(_get_col(row, "CPF_CANDIDATO", "NR_CPF_CANDIDATO"))
            if not cpf_cand:
                continue
            doacoes.append({
                "cpf_cnpj_doador": _limpar_cpf(_get_col(row, "CPF_CNPJ_DOADOR", "NR_CPF_CNPJ_DOADOR")),
                "nome_doador": _get_col(row, "NM_DOADOR", "NOME_DOADOR"),
                "cpf_candidato": cpf_cand,
                "nome_candidato": _get_col(row, "NM_CANDIDATO", "NOME_CANDIDATO"),
                "partido": _get_col(row, "SG_PARTIDO", "PARTIDO"),
                "cargo": _get_col(row, "DS_CARGO", "DESCRICAO_CARGO"),
                "uf": uf.upper(),
                "ano_eleicao": ano,
                "valor": _parse_valor(_get_col(row, "VR_RECEITA", "VALOR_RECEITA", "VR_RECEITA_ATUALIZADO", default="0")),
                "data_doacao": _get_col(row, "DT_RECEITA", "DATA_RECEITA"),
                "tipo_receita": _get_col(row, "DS_ORIGEM_RECEITA", "ORIGEM_RECEITA", "DS_ESPECIE_RECEITA"),
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
