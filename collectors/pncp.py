"""
Coletor PNCP - Portal Nacional de Contratações Públicas
API: https://pncp.gov.br
Tenta múltiplos endpoints para contornar bloqueios geográficos.
"""

import asyncio
import httpx
from datetime import date, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

ENDPOINTS = [
    "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacoes",
    "https://pncp.gov.br/api/pncp/v1/contratacoes/publicacoes",
]

HEADERS = {
    "Accept": "application/json",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://pncp.gov.br/",
    "Origin": "https://pncp.gov.br",
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
        self._endpoint_ativo = ENDPOINTS[0]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    async def _get(self, params: dict) -> dict | None:
        """Tenta cada endpoint até um retornar dados válidos."""
        for endpoint in ENDPOINTS:
            try:
                r = await self.client.get(endpoint, params=params)

                if r.status_code in (403, 429, 503):
                    logger.warning(f"PNCP bloqueou {endpoint} (HTTP {r.status_code}), tentando próximo...")
                    await asyncio.sleep(2)
                    continue

                if r.status_code == 404:
                    continue

                if r.status_code != 200:
                    logger.warning(f"PNCP {endpoint} retornou {r.status_code}")
                    continue

                try:
                    data = r.json()
                except Exception:
                    logger.warning(f"PNCP {endpoint} JSON inválido: {r.text[:200]}")
                    continue

                result = self._normalizar_resposta(data)

                if result.get("totalRegistros", 0) > 0:
                    if endpoint != self._endpoint_ativo:
                        logger.info(f"PNCP: endpoint ativo = {endpoint}")
                        self._endpoint_ativo = endpoint
                    return result

                # Resposta válida mas vazia — legítimo para períodos sem dados
                logger.debug(f"PNCP {endpoint}: 0 resultados para {params}")
                return result

            except httpx.TimeoutException:
                logger.warning(f"PNCP {endpoint}: timeout")
                continue
            except Exception as e:
                logger.warning(f"PNCP {endpoint}: {type(e).__name__}: {e}")
                continue

        logger.error("PNCP: todos os endpoints falharam")
        return None

    def _normalizar_resposta(self, data) -> dict:
        if data is None:
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
        if isinstance(data, list):
            return {"data": data, "totalRegistros": len(data), "totalPaginas": 1}
        if isinstance(data, dict):
            if "data" in data:
                return {
                    "data": data.get("data", []),
                    "totalRegistros": data.get("totalRegistros", 0),
                    "totalPaginas": data.get("totalPaginas", 1),
                }
            if "content" in data:
                return {
                    "data": data.get("content", []),
                    "totalRegistros": data.get("totalElements", 0),
                    "totalPaginas": data.get("totalPages", 1),
                }
            if "items" in data:
                return {
                    "data": data.get("items", []),
                    "totalRegistros": data.get("total", 0),
                    "totalPaginas": data.get("pages", 1),
                }
        return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

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

        result = await self._get(params)
        return result or {"data": [], "totalRegistros": 0, "totalPaginas": 0}

    def normalizar_licitacao(self, raw: dict) -> dict:
        orgao = raw.get("orgaoEntidade", {})
        municipio = raw.get("unidadeOrgao", {})
        cnpj = orgao.get("cnpj", "")

        esfera_id = str(raw.get("esferaId", ""))
        esfera_map = {
            "F": "FEDERAL", "E": "ESTADUAL", "M": "MUNICIPAL",
            "1": "FEDERAL", "2": "ESTADUAL", "3": "MUNICIPAL",
        }
        esfera = esfera_map.get(esfera_id, "MUNICIPAL")

        return {
            "fonte": "PNCP",
            "numero_controle": raw.get("numeroControlePNCP", ""),
            "orgao_cnpj": cnpj,
            "orgao_nome": orgao.get("razaoSocial", ""),
            "orgao_esfera": esfera,
            "orgao_uf": (
                municipio.get("ufSigla", "")
                or raw.get("uf", "")
                or municipio.get("uf", "")
            ),
            "orgao_municipio": (
                municipio.get("nomeUnidade", "")
                or municipio.get("municipioNome", "")
            ),
            "orgao_codigo_ibge": (
                municipio.get("codigoIBGE", "")
                or municipio.get("municipioIbge", "")
                or municipio.get("codigoIbge", "")
            ),
            "modalidade": MODALIDADES_MAP.get(
                raw.get("modalidadeId"),
                raw.get("modalidadeNome", "")
            ),
            "tipo_objeto": raw.get("tipoInstrumentoConvocatorioNome", ""),
            "objeto_descricao": raw.get("objetoCompra", ""),
            "valor_estimado": raw.get("valorTotalEstimado"),
            "valor_homologado": raw.get("valorTotalHomologado"),
            "data_abertura": (raw.get("dataPublicacaoPncp") or "")[:10] or None,
            "data_homologacao": (raw.get("dataResultadoCompra") or "")[:10] or None,
            "situacao": raw.get("situacaoCompraNome", ""),
            "raw_data": raw,
        }

    async def coletar_tudo(
        self,
        uf: str = None,
        codigo_ibge: str = None,
        dias_retroativos: int = 30,
    ) -> list[dict]:
        data_inicial = date.today() - timedelta(days=dias_retroativos)
        data_final = date.today()

        logger.info(
            f"Iniciando coleta PNCP | UF={uf} | IBGE={codigo_ibge} | "
            f"Período: {data_inicial} → {data_final}"
        )

        primeira = await self.coletar_licitacoes(
            data_inicial=data_inicial,
            data_final=data_final,
            uf=uf,
            codigo_ibge=codigo_ibge,
            pagina=1,
            tamanho=50,
        )

        total_paginas = max(primeira.get("totalPaginas", 1) or 1, 1)
        total_registros = primeira.get("totalRegistros", 0)
        logger.info(f"PNCP: {total_registros} registros em {total_paginas} páginas | UF={uf}")

        resultados = [self.normalizar_licitacao(r) for r in primeira.get("data", []) if r]

        paginas = list(range(2, total_paginas + 1))
        for i in range(0, len(paginas), 5):
            lote = paginas[i:i + 5]
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
                resultados.extend([self.normalizar_licitacao(r) for r in resp.get("data", []) if r])
            await asyncio.sleep(1)

        logger.info(f"Coleta PNCP concluída: {len(resultados)} licitações | UF={uf}")
        return resultados
