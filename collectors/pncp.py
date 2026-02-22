"""
Coletor PNCP - Portal Nacional de Contratações Públicas
API: https://pncp.gov.br/api/pncp/v1
Cobre federal, estadual e municipal desde 2023.
"""

import asyncio
import httpx
from datetime import date, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://pncp.gov.br/api/pncp/v1"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "LicitacaoRadar/1.0 (pesquisa-transparencia)",
}

MODALIDADES_MAP = {
    1: "Leilão - Eletrônico",
    2: "Diálogo Competitivo",
    3: "Concurso",
    4: "Concorrência - Eletrônica",
    5: "Concorrência - Presencial",
    6: "Pregão - Eletrônico",
    7: "Pregão - Presencial",
    8: "Dispensa de Licitação",
    9: "Inexigibilidade",
    10: "Manifestação de Interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão - Presencial",
}


class PNCPCollector:
    def __init__(self, db_session):
        self.db = db_session
        self.client = httpx.AsyncClient(
            headers=HEADERS,
            timeout=30.0,
            follow_redirects=True,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def _get(self, endpoint: str, params: dict = None) -> dict | None:
        try:
            r = await self.client.get(f"{BASE_URL}{endpoint}", params=params)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            logger.error(f"HTTP {e.response.status_code} em {endpoint}")
            raise
        except Exception as e:
            logger.error(f"Erro em {endpoint}: {e}")
            raise

    async def coletar_licitacoes(
        self,
        data_inicial: date = None,
        data_final: date = None,
        uf: str = None,
        codigo_ibge: str = None,
        pagina: int = 1,
        tamanho: int = 50,
    ) -> dict:
        """
        Busca licitações no PNCP com filtros.
        Retorna dict com dados + metadados de paginação.
        """
        if data_inicial is None:
            data_inicial = date.today() - timedelta(days=90)
        if data_final is None:
            data_final = date.today()

        params = {
            "dataInicial": data_inicial.strftime("%Y%m%d"),
            "dataFinal": data_final.strftime("%Y%m%d"),
            "pagina": pagina,
            "tamanhoPagina": tamanho,
        }
        if uf:
            params["uf"] = uf.upper()
        if codigo_ibge:
            params["codigoMunicipio"] = codigo_ibge

        data = await self._get("/contratacoes/publicacoes", params=params)
        return data or {"data": [], "totalRegistros": 0, "totalPaginas": 0}

    async def coletar_itens_licitacao(self, cnpj_orgao: str, ano: int, numero_seq: int) -> list:
        """Busca itens (produtos/serviços) de uma licitação específica."""
        data = await self._get(
            f"/orgaos/{cnpj_orgao}/compras/{ano}/{numero_seq}/itens",
            params={"pagina": 1, "tamanhoPagina": 500},
        )
        return (data or {}).get("data", [])

    async def coletar_resultado(self, cnpj_orgao: str, ano: int, numero_seq: int) -> dict | None:
        """Busca resultado (vencedor) de uma licitação."""
        data = await self._get(
            f"/orgaos/{cnpj_orgao}/compras/{ano}/{numero_seq}/propostas"
        )
        return data

    async def coletar_participantes(self, cnpj_orgao: str, ano: int, numero_seq: int) -> list:
        """Busca todos os participantes/propostas de uma licitação."""
        data = await self._get(
            f"/orgaos/{cnpj_orgao}/compras/{ano}/{numero_seq}/propostas"
        )
        if not data:
            return []
        return data if isinstance(data, list) else data.get("data", [])

    def normalizar_licitacao(self, raw: dict) -> dict:
        """Normaliza o JSON bruto do PNCP para o schema interno."""
        orgao = raw.get("orgaoEntidade", {})
        municipio = raw.get("unidadeOrgao", {})

        # determina esfera pelo CNPJ do órgão (heurística por natureza jurídica)
        cnpj = orgao.get("cnpj", "")

        return {
            "fonte": "PNCP",
            "numero_controle": raw.get("numeroControlePNCP", ""),
            "orgao_cnpj": cnpj,
            "orgao_nome": orgao.get("razaoSocial", ""),
            "orgao_esfera": raw.get("esferaId", ""),
            "orgao_uf": municipio.get("ufSigla", ""),
            "orgao_municipio": municipio.get("nomeUnidade", ""),
            "orgao_codigo_ibge": municipio.get("codigoIBGE", ""),
            "modalidade": MODALIDADES_MAP.get(raw.get("modalidadeId"), raw.get("modalidadeNome", "")),
            "tipo_objeto": raw.get("tipoInstrumentoConvocatorioNome", ""),
            "objeto_descricao": raw.get("objetoCompra", ""),
            "valor_estimado": raw.get("valorTotalEstimado"),
            "valor_homologado": raw.get("valorTotalHomologado"),
            "data_abertura": raw.get("dataPublicacaoPncp", "")[:10] if raw.get("dataPublicacaoPncp") else None,
            "data_homologacao": raw.get("dataResultadoCompra", "")[:10] if raw.get("dataResultadoCompra") else None,
            "situacao": raw.get("situacaoCompraNome", ""),
            "raw_data": raw,
        }

    async def coletar_tudo(
        self,
        uf: str = None,
        codigo_ibge: str = None,
        dias_retroativos: int = 90,
    ) -> list[dict]:
        """
        Coleta todas as licitações paginando automaticamente.
        Retorna lista de dicts normalizados.
        """
        data_inicial = date.today() - timedelta(days=dias_retroativos)
        data_final = date.today()

        logger.info(f"Iniciando coleta PNCP | UF={uf} | IBGE={codigo_ibge} | "
                    f"Período: {data_inicial} → {data_final}")

        # primeira página para saber total
        primeira = await self.coletar_licitacoes(
            data_inicial=data_inicial,
            data_final=data_final,
            uf=uf,
            codigo_ibge=codigo_ibge,
            pagina=1,
            tamanho=50,
        )

        total_paginas = primeira.get("totalPaginas", 1)
        total_registros = primeira.get("totalRegistros", 0)
        logger.info(f"Total: {total_registros} licitações em {total_paginas} páginas")

        resultados = [self.normalizar_licitacao(r) for r in primeira.get("data", [])]

        # coleta páginas restantes em paralelo (lotes de 5)
        paginas = list(range(2, total_paginas + 1))
        for i in range(0, len(paginas), 5):
            lote = paginas[i:i+5]
            tasks = [
                self.coletar_licitacoes(
                    data_inicial=data_inicial,
                    data_final=data_final,
                    uf=uf,
                    codigo_ibge=codigo_ibge,
                    pagina=p,
                    tamanho=50,
                )
                for p in lote
            ]
            respostas = await asyncio.gather(*tasks, return_exceptions=True)
            for resp in respostas:
                if isinstance(resp, Exception):
                    logger.warning(f"Erro em página: {resp}")
                    continue
                resultados.extend([self.normalizar_licitacao(r) for r in resp.get("data", [])])

            await asyncio.sleep(0.5)  # respeita rate limit

        logger.info(f"Coleta PNCP concluída: {len(resultados)} licitações")
        return resultados
