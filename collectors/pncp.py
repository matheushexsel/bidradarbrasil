"""
Coletor PNCP - Portal Nacional de Contratações Públicas
Endpoint: /api/consulta/v1/contratacoes/publicacao (singular)
"""

import asyncio
import httpx
from datetime import date, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

ENDPOINT = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": "https://pncp.gov.br/app/",
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
            timeout=httpx.Timeout(connect=15.0, read=60.0, write=10.0, pool=10.0),
            follow_redirects=True,
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException))
    )
    async def _get_pagina(self, params: dict) -> dict:
        try:
            r = await self.client.get(ENDPOINT, params=params)
            logger.debug(f"PNCP HTTP {r.status_code} | params={params}")

            if r.status_code == 200:
                try:
                    data = r.json()
                    return self._normalizar_resposta(data)
                except Exception as e:
                    logger.warning(f"PNCP JSON inválido: {e} | body={r.text[:300]}")
                    raise

            if r.status_code in (204, 404):
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

            if r.status_code == 422:
                logger.warning(f"PNCP 422 | body={r.text[:300]}")
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

            if r.status_code == 500:
                raise httpx.HTTPStatusError("Retry 500", request=r.request, response=r)

            logger.warning(f"PNCP HTTP {r.status_code} | body={r.text[:300]}")
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

        except httpx.TimeoutException as e:
            logger.warning(f"PNCP timeout: {e}")
            raise
        except httpx.ConnectError as e:
            logger.warning(f"PNCP ConnectError: {e}")
            raise
        except Exception as e:
            logger.warning(f"PNCP {type(e).__name__}: {e}")
            raise

    def _normalizar_resposta(self, data) -> dict:
        if data is None:
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}
        if isinstance(data, list):
            return {"data": data, "totalRegistros": len(data), "totalPaginas": 1}
        if isinstance(data, dict):
            if "data" in data:
                return {
                    "data": data.get("data") or [],
                    "totalRegistros": data.get("totalRegistros", 0),
                    "totalPaginas": max(data.get("totalPaginas", 1) or 1, 1),
                }
            if "content" in data:
                return {
                    "data": data.get("content") or [],
                    "totalRegistros": data.get("totalElements", 0),
                    "totalPaginas": max(data.get("totalPages", 1) or 1, 1),
                }
            logger.warning(f"PNCP estrutura desconhecida: {list(data.keys())[:10]}")
        return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

    def normalizar_licitacao(self, raw: dict) -> dict:
        orgao = raw.get("orgaoEntidade") or {}
        municipio = raw.get("unidadeOrgao") or {}

        esfera_id = str(raw.get("esferaId", ""))
        esfera_map = {
            "F": "FEDERAL", "E": "ESTADUAL", "M": "MUNICIPAL",
            "1": "FEDERAL", "2": "ESTADUAL", "3": "MUNICIPAL",
        }

        # Código IBGE — tenta todas as variantes conhecidas da API PNCP
        codigo_ibge = (
            municipio.get("codigoIBGE")
            or municipio.get("codigoMunicipioIbge")
            or municipio.get("municipioIbge")
            or municipio.get("codigoIbge")
            or municipio.get("municipio", {}).get("codigo") if isinstance(municipio.get("municipio"), dict) else None
            or raw.get("codigoMunicipioIbge")
            or ""
        )

        # UF — tenta todas as variantes
        uf = (
            municipio.get("ufSigla")
            or municipio.get("uf")
            or raw.get("ufSigla")
            or raw.get("uf")
            or ""
        ).upper()

        # Data de abertura — dataPublicacaoPncp é a data de publicação no PNCP
        # dataAbertura é quando abre para propostas (mais relevante quando disponível)
        data_abertura = (
            (raw.get("dataAberturaProposta") or "")[:10]
            or (raw.get("dataPublicacaoPncp") or "")[:10]
            or None
        )

        return {
            "fonte": "PNCP",
            "numero_controle": raw.get("numeroControlePNCP", ""),
            "orgao_cnpj": orgao.get("cnpj", ""),
            "orgao_nome": orgao.get("razaoSocial", ""),
            "orgao_esfera": esfera_map.get(esfera_id, "MUNICIPAL"),
            "orgao_uf": uf,
            "orgao_municipio": (
                municipio.get("nomeUnidade")
                or municipio.get("municipioNome")
                or municipio.get("nome")
                or ""
            ),
            "orgao_codigo_ibge": str(codigo_ibge) if codigo_ibge else "",
            "modalidade": MODALIDADES_MAP.get(
                raw.get("modalidadeId"),
                raw.get("modalidadeNome", "")
            ),
            "tipo_objeto": raw.get("tipoInstrumentoConvocatorioNome", ""),
            "objeto_descricao": raw.get("objetoCompra", ""),
            "valor_estimado": raw.get("valorTotalEstimado"),
            "valor_homologado": raw.get("valorTotalHomologado"),
            "data_abertura": data_abertura,
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
        data_final = date.today()
        data_inicial = data_final - timedelta(days=dias_retroativos)

        logger.info(f"Iniciando coleta PNCP | UF={uf} | Período: {data_inicial} → {data_final}")

        params_base = {
            "dataInicial": data_inicial.strftime("%Y%m%d"),
            "dataFinal": data_final.strftime("%Y%m%d"),
            "pagina": 1,
            "tamanhoPagina": 50,
        }

        if uf:
            params_base["uf"] = uf.upper()
        if codigo_ibge:
            params_base["codigoMunicipioIbge"] = codigo_ibge

        licitacoes = []
        modalidades = list(MODALIDADES_MAP.keys())

        for mod in modalidades:
            params = {**params_base, "codigoModalidadeContratacao": mod}

            primeira = await self._get_pagina(params)
            total_paginas = max(primeira.get("totalPaginas", 1) or 1, 1)
            total_registros = primeira.get("totalRegistros", 0)

            if total_registros > 0:
                logger.info(f"PNCP UF={uf} Modalidade={mod}: {total_registros} registros em {total_paginas} páginas")

            licitacoes.extend([
                self.normalizar_licitacao(r)
                for r in primeira.get("data", []) if r
            ])

            paginas_restantes = list(range(2, min(total_paginas + 1, 201)))
            for i in range(0, len(paginas_restantes), 5):
                tasks = [
                    self._get_pagina({**params, "pagina": p})
                    for p in paginas_restantes[i:i + 5]
                ]
                respostas = await asyncio.gather(*tasks, return_exceptions=True)
                for resp in respostas:
                    if isinstance(resp, Exception):
                        continue
                    licitacoes.extend([
                        self.normalizar_licitacao(r)
                        for r in resp.get("data", []) if r
                    ])
                await asyncio.sleep(0.5)

        logger.info(f"Coleta PNCP concluída: {len(licitacoes)} licitações | UF={uf}")
        return licitacoes
