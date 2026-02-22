"""
Coletor PNCP - Portal Nacional de Contratações Públicas

O PNCP bloqueia IPs de cloud americanos no endpoint /api/pncp/v1.
Estratégia: tenta múltiplos endpoints e loga o erro exato para diagnóstico.
"""

import asyncio
import httpx
from datetime import date, timedelta
from loguru import logger

# Endpoints em ordem de prioridade
ENDPOINTS = [
    "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacoes",
    "https://pncp.gov.br/api/pncp/v1/contratacoes/publicacoes",
    "https://treina.pncp.gov.br/api/pncp/v1/contratacoes/publicacoes",
]

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": "https://pncp.gov.br/app/",
    "Origin": "https://pncp.gov.br",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
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
            timeout=httpx.Timeout(connect=15.0, read=45.0, write=10.0, pool=10.0),
            follow_redirects=True,
            http2=False,
        )
        self._endpoint_ativo = ENDPOINTS[0]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    async def _get(self, params: dict) -> dict | None:
        """Tenta cada endpoint. Loga o erro exato de cada falha."""
        for endpoint in ENDPOINTS:
            try:
                r = await self.client.get(endpoint, params=params)

                logger.debug(f"PNCP {endpoint} → HTTP {r.status_code}")

                if r.status_code == 200:
                    try:
                        data = r.json()
                    except Exception as json_err:
                        logger.warning(
                            f"PNCP {endpoint}: JSON inválido — {json_err} | "
                            f"body={r.text[:300]}"
                        )
                        continue

                    result = self._normalizar_resposta(data)
                    if result.get("totalRegistros", 0) > 0:
                        self._endpoint_ativo = endpoint
                    return result

                if r.status_code == 204:
                    return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

                if r.status_code in (301, 302, 307, 308):
                    logger.warning(f"PNCP {endpoint}: redirect para {r.headers.get('location')}")
                    continue

                if r.status_code == 401:
                    logger.warning(f"PNCP {endpoint}: autenticação necessária (401)")
                    continue

                if r.status_code == 403:
                    logger.warning(
                        f"PNCP {endpoint}: acesso negado (403) — possível bloqueio geográfico. "
                        f"body={r.text[:200]}"
                    )
                    continue

                if r.status_code == 429:
                    logger.warning(f"PNCP {endpoint}: rate limit (429)")
                    await asyncio.sleep(5)
                    continue

                if r.status_code == 503:
                    logger.warning(f"PNCP {endpoint}: serviço indisponível (503)")
                    await asyncio.sleep(3)
                    continue

                if r.status_code == 404:
                    logger.debug(f"PNCP {endpoint}: 404")
                    continue

                logger.warning(
                    f"PNCP {endpoint}: HTTP {r.status_code} inesperado | "
                    f"body={r.text[:300]}"
                )
                continue

            except httpx.ConnectError as e:
                logger.warning(f"PNCP {endpoint}: ConnectError — {e}")
                continue
            except httpx.ConnectTimeout as e:
                logger.warning(f"PNCP {endpoint}: ConnectTimeout — {e}")
                continue
            except httpx.ReadTimeout as e:
                logger.warning(f"PNCP {endpoint}: ReadTimeout — {e}")
                continue
            except httpx.ProxyError as e:
                logger.warning(f"PNCP {endpoint}: ProxyError — {e}")
                continue
            except Exception as e:
                logger.warning(f"PNCP {endpoint}: {type(e).__name__} — {e}")
                continue

        logger.error(
            f"PNCP: todos os {len(ENDPOINTS)} endpoints falharam para params={params}"
        )
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
                    "totalPaginas": data.get("totalPaginas", 1) or 1,
                }
            if "content" in data:
                return {
                    "data": data.get("content", []),
                    "totalRegistros": data.get("totalElements", 0),
                    "totalPaginas": data.get("totalPages", 1) or 1,
                }
            if "items" in data:
                return {
                    "data": data.get("items", []),
                    "totalRegistros": data.get("total", 0),
                    "totalPaginas": data.get("pages", 1) or 1,
                }
            # Resposta desconhecida — loga chaves para debug
            logger.warning(f"PNCP: estrutura de resposta desconhecida. Chaves: {list(data.keys())[:10]}")
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
