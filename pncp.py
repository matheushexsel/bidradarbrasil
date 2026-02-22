"""
Coletor PNCP - Portal Nacional de Contratações Públicas
API: https://pncp.gov.br/api/pncp/v1
"""

import asyncio
import httpx
from datetime import date, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

BASE_URL = "https://pncp.gov.br/api/pncp/v1"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; BidRadarBrasil/1.0; +https://bidradarbrasil.com.br)",
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
    def __init__(self, db_session=None):
        self.db = db_session
        self.client = httpx.AsyncClient(
            headers=HEADERS,
            timeout=60.0,
            follow_redirects=True,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=3, max=15))
    async def _get(self, endpoint: str, params: dict = None) -> dict | list | None:
        url = f"{BASE_URL}{endpoint}"
        try:
            r = await self.client.get(url, params=params)
            logger.debug(f"PNCP {r.status_code} {url} params={params}")

            if r.status_code == 404:
                return None
            if r.status_code == 429:
                logger.warning("PNCP rate limit — aguardando 30s")
                await asyncio.sleep(30)
                raise Exception("Rate limited")
            if r.status_code == 204:
                return None

            r.raise_for_status()

            # Log primeiros bytes para debug
            text = r.text
            if len(text) < 500:
                logger.debug(f"PNCP resposta: {text}")

            return r.json()

        except httpx.HTTPStatusError as e:
            logger.error(f"PNCP HTTP {e.response.status_code} em {url}: {e.response.text[:300]}")
            raise
        except Exception as e:
            logger.error(f"PNCP erro em {url}: {type(e).__name__}: {e}")
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
        if data_inicial is None:
            data_inicial = date.today() - timedelta(days=30)
        if data_final is None:
            data_final = date.today()

        # PNCP aceita datas no formato YYYYMMDD
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

        result = await self._get("/contratacoes/publicacoes", params=params)

        if result is None:
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

        # Trata diferentes estruturas de resposta do PNCP
        if isinstance(result, list):
            # Às vezes retorna lista direta
            return {"data": result, "totalRegistros": len(result), "totalPaginas": 1}

        if isinstance(result, dict):
            # Estrutura padrão: {data: [...], totalRegistros: N, totalPaginas: N}
            if "data" in result:
                return result
            # Estrutura alternativa: {content: [...], totalElements: N, totalPages: N}
            if "content" in result:
                return {
                    "data": result["content"],
                    "totalRegistros": result.get("totalElements", 0),
                    "totalPaginas": result.get("totalPages", 1),
                }
            # Retornou dict mas sem estrutura conhecida — loga para debug
            logger.warning(f"PNCP resposta inesperada. Chaves: {list(result.keys())}")
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

        return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

    def normalizar_licitacao(self, raw: dict) -> dict:
        """Normaliza o JSON bruto do PNCP para o schema interno."""
        orgao = raw.get("orgaoEntidade", {})
        municipio = raw.get("unidadeOrgao", {})
        cnpj = orgao.get("cnpj", "")

        # Determina esfera
        esfera_id = raw.get("esferaId", "")
        esfera_map = {"F": "FEDERAL", "E": "ESTADUAL", "M": "MUNICIPAL"}
        esfera = esfera_map.get(str(esfera_id), str(esfera_id))

        return {
            "fonte": "PNCP",
            "numero_controle": raw.get("numeroControlePNCP", ""),
            "orgao_cnpj": cnpj,
            "orgao_nome": orgao.get("razaoSocial", ""),
            "orgao_esfera": esfera,
            "orgao_uf": municipio.get("ufSigla", "") or raw.get("uf", ""),
            "orgao_municipio": municipio.get("nomeUnidade", "") or municipio.get("municipioNome", ""),
            "orgao_codigo_ibge": municipio.get("codigoIBGE", "") or municipio.get("municipioIbge", ""),
            "modalidade": MODALIDADES_MAP.get(raw.get("modalidadeId"), raw.get("modalidadeNome", "")),
            "tipo_objeto": raw.get("tipoInstrumentoConvocatorioNome", ""),
            "objeto_descricao": raw.get("objetoCompra", ""),
            "valor_estimado": raw.get("valorTotalEstimado"),
            "valor_homologado": raw.get("valorTotalHomologado"),
            "data_abertura": (raw.get("dataPublicacaoPncp") or "")[:10] or None,
            "data_homologacao": (raw.get("dataResultadoCompra") or "")[:10] or None,
            "situacao": raw.get("situacaoCompraNome", ""),
            "numero_participantes": raw.get("quantidadeItens"),
            "raw_data": raw,
        }

    async def testar_api(self) -> dict:
        """Testa a conectividade com a API do PNCP e retorna diagnóstico."""
        logger.info("Testando API do PNCP...")

        # Testa com data recente (últimos 7 dias)
        hoje = date.today()
        semana = hoje - timedelta(days=7)

        params = {
            "dataInicial": semana.strftime("%Y%m%d"),
            "dataFinal": hoje.strftime("%Y%m%d"),
            "pagina": 1,
            "tamanhoPagina": 5,
        }

        try:
            r = await self.client.get(
                f"{BASE_URL}/contratacoes/publicacoes",
                params=params
            )
            logger.info(f"PNCP status: {r.status_code}")
            logger.info(f"PNCP headers: {dict(r.headers)}")
            logger.info(f"PNCP body (500 chars): {r.text[:500]}")
            return {"status": r.status_code, "body": r.text[:500]}
        except Exception as e:
            logger.error(f"PNCP teste falhou: {e}")
            return {"status": "erro", "body": str(e)}

    async def coletar_tudo(
        self,
        uf: str = None,
        codigo_ibge: str = None,
        dias_retroativos: int = 30,
    ) -> list[dict]:
        """Coleta todas as licitações paginando automaticamente."""
        data_inicial = date.today() - timedelta(days=dias_retroativos)
        data_final = date.today()

        logger.info(
            f"Iniciando coleta PNCP | UF={uf} | IBGE={codigo_ibge} | "
            f"Período: {data_inicial} → {data_final}"
        )

        # Primeira página — descobre total
        primeira = await self.coletar_licitacoes(
            data_inicial=data_inicial,
            data_final=data_final,
            uf=uf,
            codigo_ibge=codigo_ibge,
            pagina=1,
            tamanho=50,
        )

        total_paginas = primeira.get("totalPaginas", 1) or 1
        total_registros = primeira.get("totalRegistros", 0)
        logger.info(f"PNCP: {total_registros} registros em {total_paginas} páginas | UF={uf}")

        resultados = [self.normalizar_licitacao(r) for r in primeira.get("data", [])]

        if total_paginas <= 1:
            logger.info(f"Coleta PNCP concluída: {len(resultados)} licitações | UF={uf}")
            return resultados

        # Coleta demais páginas em lotes de 5
        for i in range(0, list(range(2, total_paginas + 1)).__len__(), 5):
            lote = list(range(2, total_paginas + 1))[i:i+5]
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
                    logger.warning(f"Erro em página do lote: {resp}")
                    continue
                resultados.extend([self.normalizar_licitacao(r) for r in resp.get("data", [])])

            await asyncio.sleep(1)

        logger.info(f"Coleta PNCP concluída: {len(resultados)} licitações | UF={uf}")
        return resultados
