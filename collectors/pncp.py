"""
Coletor PNCP - Portal Nacional de Contratações Públicas

Dois endpoints complementares:
  1. /contratacoes/publicacao → editais publicados (objeto, valor estimado, órgão)
  2. /contratos               → contratos assinados (CNPJ vencedor, valor final)

Fluxo:
  - Coleta contratos primeiro (têm todos os dados que precisamos)
  - Para cada contrato, enriquece com dados do edital via numeroControlePNCP
  - Resultado: registro completo com vencedor + objeto + valores
"""

import asyncio
import httpx
from datetime import date, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

BASE = "https://pncp.gov.br/api/consulta/v1"
ENDPOINT_PUBLICACAO = f"{BASE}/contratacoes/publicacao"
ENDPOINT_CONTRATOS  = f"{BASE}/contratos"

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

    # ----------------------------------------------------------
    # REQUEST GENÉRICO COM RETRY
    # ----------------------------------------------------------

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=2, max=30),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TimeoutException)),
    )
    async def _get(self, endpoint: str, params: dict) -> dict:
        try:
            r = await self.client.get(endpoint, params=params)
            logger.debug(f"PNCP HTTP {r.status_code} | {endpoint.split('/')[-1]} | params={params}")

            if r.status_code == 200:
                data = r.json()
                return self._normalizar_resposta(data)

            if r.status_code in (204, 404):
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

            if r.status_code == 422:
                logger.warning(f"PNCP 422 | body={r.text[:200]}")
                return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

            if r.status_code == 500:
                raise httpx.HTTPStatusError("Retry 500", request=r.request, response=r)

            logger.warning(f"PNCP HTTP {r.status_code} | body={r.text[:200]}")
            return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

        except httpx.TimeoutException:
            logger.warning(f"PNCP timeout: {endpoint}")
            raise
        except httpx.ConnectError as e:
            logger.warning(f"PNCP ConnectError: {e}")
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
            logger.warning(f"PNCP estrutura desconhecida: {list(data.keys())[:8]}")
        return {"data": [], "totalRegistros": 0, "totalPaginas": 0}

    async def _coletar_todas_paginas(self, endpoint: str, params_base: dict) -> list[dict]:
        """Coleta todas as páginas de um endpoint com paralelismo controlado."""
        primeira = await self._get(endpoint, {**params_base, "pagina": 1})
        total_paginas = max(primeira.get("totalPaginas", 1) or 1, 1)
        total_registros = primeira.get("totalRegistros", 0)

        registros = list(primeira.get("data", []))

        if total_registros > 0:
            logger.info(
                f"PNCP {endpoint.split('/')[-1]} | "
                f"UF={params_base.get('uf', '?')} | "
                f"{total_registros} registros em {total_paginas} páginas"
            )

        paginas_restantes = list(range(2, min(total_paginas + 1, 201)))
        for i in range(0, len(paginas_restantes), 5):
            tasks = [
                self._get(endpoint, {**params_base, "pagina": p})
                for p in paginas_restantes[i:i + 5]
            ]
            respostas = await asyncio.gather(*tasks, return_exceptions=True)
            for resp in respostas:
                if isinstance(resp, Exception):
                    logger.warning(f"PNCP página falhou: {resp}")
                    continue
                registros.extend(resp.get("data", []))
            await asyncio.sleep(0.5)

        return registros

    # ----------------------------------------------------------
    # NORMALIZAÇÃO - CONTRATO (tem vencedor)
    # ----------------------------------------------------------

    def normalizar_contrato(self, raw: dict) -> dict:
        """
        Normaliza registro do endpoint /contratos.
        Este endpoint retorna contratos já assinados com CNPJ do fornecedor.

        Campos relevantes do /contratos:
          - niFornecedor / cnpjFornecedor → CNPJ da empresa vencedora
          - nomeRazaoSocialFornecedor → razão social
          - valorInicial → valor do contrato
          - dataPublicacaoPncp → data de publicação
          - objetoContrato → descrição
          - orgaoEntidade → órgão contratante
          - unidadeOrgao → município/UF
          - numeroControlePNCP → chave de ligação com licitação
        """
        orgao    = raw.get("orgaoEntidade") or {}
        municipio = raw.get("unidadeOrgao") or {}

        esfera_map = {
            "F": "FEDERAL", "E": "ESTADUAL", "M": "MUNICIPAL",
            "1": "FEDERAL", "2": "ESTADUAL", "3": "MUNICIPAL",
        }
        esfera_id = str(raw.get("esferaId", ""))

        uf = (
            municipio.get("ufSigla")
            or municipio.get("uf")
            or raw.get("ufSigla")
            or raw.get("uf")
            or ""
        ).upper()

        codigo_ibge = (
            municipio.get("codigoIBGE")
            or municipio.get("codigoMunicipioIbge")
            or municipio.get("municipioIbge")
            or municipio.get("codigoIbge")
            or raw.get("codigoMunicipioIbge")
            or ""
        )

        # CNPJ do vencedor — campo principal do /contratos
        cnpj_fornecedor = (
            raw.get("niFornecedor")
            or raw.get("cnpjFornecedor")
            or raw.get("cnpjContratada")
            or ""
        )
        nome_fornecedor = (
            raw.get("nomeRazaoSocialFornecedor")
            or raw.get("razaoSocialFornecedor")
            or raw.get("nomeContratada")
            or ""
        )

        # Número de controle da LICITAÇÃO vinculada (para cruzamento)
        numero_controle_licitacao = (
            raw.get("numeroControlePNCPCompra")
            or raw.get("numeroControlePNCP")
            or ""
        )

        # Número de controle do próprio contrato
        numero_controle_contrato = raw.get("numeroControlePNCP", "")

        return {
            "fonte": "PNCP",
            # Usa numero_controle da licitação vinculada para upsert
            # Se não tiver, usa o do próprio contrato
            "numero_controle": numero_controle_licitacao or numero_controle_contrato,
            "numero_controle_contrato": numero_controle_contrato,
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
                raw.get("modalidadeNome", ""),
            ),
            "tipo_objeto": raw.get("tipoContrato", raw.get("tipoInstrumentoConvocatorioNome", "")),
            "objeto_descricao": (
                raw.get("objetoContrato")
                or raw.get("objetoCompra")
                or ""
            ),
            "valor_estimado": raw.get("valorInicial"),
            "valor_homologado": (
                raw.get("valorGlobal")
                or raw.get("valorInicial")
            ),
            "data_abertura": (raw.get("dataPublicacaoPncp") or "")[:10] or None,
            "data_homologacao": (
                raw.get("dataAssinatura")
                or raw.get("dataPublicacaoPncp")
                or ""
            )[:10] or None,
            "situacao": raw.get("situacaoContrato", raw.get("situacaoCompraNome", "")),
            # Dados do vencedor — usados no pipeline para buscar empresa
            "cnpj_fornecedor": "".join(filter(str.isdigit, cnpj_fornecedor)),
            "nome_fornecedor": nome_fornecedor,
            "raw_data": raw,
        }

    # ----------------------------------------------------------
    # NORMALIZAÇÃO - PUBLICAÇÃO (edital, sem vencedor)
    # ----------------------------------------------------------

    def normalizar_licitacao(self, raw: dict) -> dict:
        """
        Normaliza registro do endpoint /contratacoes/publicacao.
        Usado como fallback quando não há contrato assinado ainda.
        """
        orgao    = raw.get("orgaoEntidade") or {}
        municipio = raw.get("unidadeOrgao") or {}

        esfera_map = {
            "F": "FEDERAL", "E": "ESTADUAL", "M": "MUNICIPAL",
            "1": "FEDERAL", "2": "ESTADUAL", "3": "MUNICIPAL",
        }
        esfera_id = str(raw.get("esferaId", ""))

        uf = (
            municipio.get("ufSigla")
            or municipio.get("uf")
            or raw.get("ufSigla")
            or raw.get("uf")
            or ""
        ).upper()

        codigo_ibge = (
            municipio.get("codigoIBGE")
            or municipio.get("codigoMunicipioIbge")
            or municipio.get("municipioIbge")
            or municipio.get("codigoIbge")
            or raw.get("codigoMunicipioIbge")
            or ""
        )

        return {
            "fonte": "PNCP",
            "numero_controle": raw.get("numeroControlePNCP", ""),
            "numero_controle_contrato": None,
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
                raw.get("modalidadeNome", ""),
            ),
            "tipo_objeto": raw.get("tipoInstrumentoConvocatorioNome", ""),
            "objeto_descricao": raw.get("objetoCompra", ""),
            "valor_estimado": raw.get("valorTotalEstimado"),
            "valor_homologado": raw.get("valorTotalHomologado"),
            # dataPublicacaoPncp = quando foi publicado no portal (data correta)
            "data_abertura": (raw.get("dataPublicacaoPncp") or "")[:10] or None,
            "data_homologacao": (raw.get("dataResultadoCompra") or "")[:10] or None,
            "situacao": raw.get("situacaoCompraNome", ""),
            "cnpj_fornecedor": "",
            "nome_fornecedor": "",
            "raw_data": raw,
        }

    # ----------------------------------------------------------
    # COLETA PRINCIPAL
    # ----------------------------------------------------------

    async def coletar_tudo(
        self,
        uf: str = None,
        codigo_ibge: str = None,
        dias_retroativos: int = 90,
    ) -> list[dict]:
        data_final   = date.today()
        data_inicial = data_final - timedelta(days=dias_retroativos)

        di = data_inicial.strftime("%Y%m%d")
        df = data_final.strftime("%Y%m%d")

        logger.info(f"Iniciando coleta PNCP | UF={uf} | {data_inicial} → {data_final}")

        params_base = {
            "dataInicial": di,
            "dataFinal":   df,
            "tamanhoPagina": 50,
        }
        if uf:
            params_base["uf"] = uf.upper()
        if codigo_ibge:
            params_base["codigoMunicipioIbge"] = codigo_ibge

        # --------------------------------------------------
        # 1. CONTRATOS (têm vencedor — prioridade máxima)
        # O endpoint /contratos aceita no máximo ~30 dias por request.
        # Janelas maiores retornam 422 nas páginas avançadas (24+).
        # --------------------------------------------------
        logger.info(f"PNCP coletando /contratos | UF={uf}")
        contratos_raw = []
        janela = 30
        cursor = data_inicial
        while cursor <= data_final:
            fim = min(cursor + timedelta(days=janela - 1), data_final)
            params_janela = {
                k: v for k, v in params_base.items()
                if k not in ("dataInicial", "dataFinal")
            }
            params_janela["dataInicial"] = cursor.strftime("%Y%m%d")
            params_janela["dataFinal"]   = fim.strftime("%Y%m%d")
            janela_raw = await self._coletar_todas_paginas(ENDPOINT_CONTRATOS, params_janela)
            contratos_raw.extend(janela_raw)
            cursor = fim + timedelta(days=1)
            await asyncio.sleep(0.3)

        contratos = [self.normalizar_contrato(r) for r in contratos_raw if r]
        logger.info(f"PNCP /contratos: {len(contratos)} contratos | UF={uf}")

        # Índice por numero_controle para deduplicação
        por_controle: dict[str, dict] = {}
        for c in contratos:
            nc = c.get("numero_controle")
            if nc:
                por_controle[nc] = c

        # --------------------------------------------------
        # 2. PUBLICAÇÕES (editais — todas as modalidades)
        # Só adiciona se ainda não temos o contrato assinado
        # --------------------------------------------------
        logger.info(f"PNCP coletando /publicacao | UF={uf}")
        for mod in MODALIDADES_MAP.keys():
            params_mod = {**params_base, "codigoModalidadeContratacao": mod}
            pub_raw = await self._coletar_todas_paginas(ENDPOINT_PUBLICACAO, params_mod)

            for r in pub_raw:
                if not r:
                    continue
                nc = r.get("numeroControlePNCP", "")
                if nc and nc not in por_controle:
                    # Ainda não temos o contrato — salva o edital
                    por_controle[nc] = self.normalizar_licitacao(r)
                elif nc and nc in por_controle:
                    # Já temos o contrato — enriquece com o objeto do edital se estiver vazio
                    existing = por_controle[nc]
                    if not existing.get("objeto_descricao"):
                        existing["objeto_descricao"] = r.get("objetoCompra", "")
                    if not existing.get("valor_estimado"):
                        existing["valor_estimado"] = r.get("valorTotalEstimado")

            await asyncio.sleep(0.3)

        resultado = list(por_controle.values())
        # Filtra registros sem numero_controle
        resultado = [r for r in resultado if r.get("numero_controle")]

        com_vencedor = sum(1 for r in resultado if r.get("cnpj_fornecedor"))
        logger.info(
            f"PNCP concluído | UF={uf} | "
            f"{len(resultado)} total | "
            f"{com_vencedor} com vencedor | "
            f"{len(resultado) - com_vencedor} editais abertos"
        )
        return resultado
