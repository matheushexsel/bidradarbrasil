"""
Coletor PNCP - Portal Nacional de Contratações Públicas

NOTA: A API do PNCP NÃO suporta filtro por UF no endpoint de publicações.
Parâmetros aceitos: dataInicial, dataFinal, pagina, tamanhoPagina.
Filtragem por UF é feita em memória após a coleta.
"""

import asyncio
import httpx
from datetime import date, timedelta
from loguru import logger

# Único endpoint funcional confirmado
ENDPOINT = "https://pncp.gov.br/api/pncp/v1/contratacoes/publicacoes"

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

    async def _get_pagina(self, params: dict) -> dict:
        """Faz uma requisição sem filtro de UF (API não suporta esse filtro)."""
        try:
            r = await self.client.get(ENDPOINT, params=params)
            logger.debug(f"PNCP HTTP {r.status_code} | params={params}")

            if r.status_code == 200:
                try:
                    data = r.json()
                    return self._normalizar_resposta(data)
                except Exception as e:
                    logger.warning(f"PNCP JSON inválido: {e} | body={r.text[:300]}")
                    return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

            if r.status_code == 204:
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

            logger.warning(f"PNCP HTTP {r.status_code} | body={r.text[:300]}")
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

        except httpx.TimeoutException as e:
            logger.warning(f"PNCP timeout: {e}")
        except httpx.ConnectError as e:
            logger.warning(f"PNCP ConnectError: {e}")
        except Exception as e:
            logger.warning(f"PNCP {type(e).__name__}: {e}")

        return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

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
            logger.warning(f"PNCP estrutura desconhecida: {list(data.keys())[:8]}")
        return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

    def _extrair_uf(self, raw: dict) -> str:
        """Extrai a UF de um item da resposta PNCP."""
        municipio = raw.get("unidadeOrgao", {})
        return (
            municipio.get("ufSigla", "")
            or municipio.get("uf", "")
            or raw.get("uf", "")
            or raw.get("ufSigla", "")
        ).upper()

    def normalizar_licitacao(self, raw: dict) -> dict:
        orgao = raw.get("orgaoEntidade", {})
        municipio = raw.get("unidadeOrgao", {})

        esfera_id = str(raw.get("esferaId", ""))
        esfera_map = {
            "F": "FEDERAL", "E": "ESTADUAL", "M": "MUNICIPAL",
            "1": "FEDERAL", "2": "ESTADUAL", "3": "MUNICIPAL",
        }

        return {
            "fonte": "PNCP",
            "numero_controle": raw.get("numeroControlePNCP", ""),
            "orgao_cnpj": orgao.get("cnpj", ""),
            "orgao_nome": orgao.get("razaoSocial", ""),
            "orgao_esfera": esfera_map.get(esfera_id, "MUNICIPAL"),
            "orgao_uf": self._extrair_uf(raw),
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
        """
        Coleta licitações do PNCP.
        NOTA: A API não suporta filtro por UF — coleta nacional e filtra em memória.
        Para evitar sobrecarga, usa janelas de 7 dias quando uf é especificado.
        """
        data_final = date.today()
        data_inicial = data_final - timedelta(days=dias_retroativos)

        logger.info(
            f"Iniciando coleta PNCP | UF={uf} | "
            f"Período: {data_inicial} → {data_final}"
        )

        # Coleta em janelas de 7 dias para reduzir volume por request
        janela_dias = 7
        todas_licitacoes: list[dict] = []

        cursor = data_inicial
        while cursor <= data_final:
            fim_janela = min(cursor + timedelta(days=janela_dias - 1), data_final)

            params = {
                "dataInicial": cursor.strftime("%Y%m%d"),
                "dataFinal": fim_janela.strftime("%Y%m%d"),
                "pagina": 1,
                "tamanhoPagina": 50,
            }

            primeira = await self._get_pagina(params)
            total_paginas = max(primeira.get("totalPaginas", 1) or 1, 1)
            total_registros = primeira.get("totalRegistros", 0)

            logger.info(
                f"PNCP {cursor}→{fim_janela}: "
                f"{total_registros} registros em {total_paginas} páginas"
            )

            lote = [self.normalizar_licitacao(r) for r in primeira.get("data", []) if r]

            # Páginas restantes
            paginas = list(range(2, min(total_paginas + 1, 21)))  # máx 20 páginas por janela
            for i in range(0, len(paginas), 5):
                tasks = [
                    self._get_pagina({**params, "pagina": p})
                    for p in paginas[i:i + 5]
                ]
                respostas = await asyncio.gather(*tasks, return_exceptions=True)
                for resp in respostas:
                    if isinstance(resp, Exception):
                        continue
                    lote.extend([self.normalizar_licitacao(r) for r in resp.get("data", []) if r])
                await asyncio.sleep(0.5)

            todas_licitacoes.extend(lote)
            cursor = fim_janela + timedelta(days=1)
            await asyncio.sleep(1)

        # Filtra por UF em memória
        if uf:
            uf_upper = uf.upper()
            resultado = [l for l in todas_licitacoes if l.get("orgao_uf") == uf_upper]
            logger.info(f"PNCP concluído: {len(resultado)}/{len(todas_licitacoes)} licitações para UF={uf}")
            return resultado

        logger.info(f"PNCP concluído: {len(todas_licitacoes)} licitações")
        return todas_licitacoes
