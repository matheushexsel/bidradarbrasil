"""
Coletor CNPJ + QSA - Receita Federal via BrasilAPI + ReceitaWS
Estratégia: BrasilAPI primeiro (sem auth), ReceitaWS como fallback.
Para massa de CNPJs: dataset completo via Brasil.io (mensal).
"""

import asyncio
import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

BRASILAPI_URL = "https://brasilapi.com.br/api/cnpj/v1"
RECEITAWS_URL = "https://www.receitaws.com.br/v1/cnpj"
# Dataset completo (atualizado mensalmente): https://dados.rfb.gov.br/CNPJ/


class CNPJCollector:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": "LicitacaoRadar/1.0"},
        )
        self._cache: dict[str, dict] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    def _limpar_cnpj(self, cnpj: str) -> str:
        return "".join(filter(str.isdigit, cnpj or ""))

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5))
    async def _brasilapi(self, cnpj: str) -> dict | None:
        try:
            r = await self.client.get(f"{BRASILAPI_URL}/{cnpj}")
            if r.status_code == 200:
                return r.json()
            return None
        except Exception:
            return None

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=2, max=8))
    async def _receitaws(self, cnpj: str) -> dict | None:
        try:
            r = await self.client.get(f"{RECEITAWS_URL}/{cnpj}")
            if r.status_code == 200:
                data = r.json()
                if data.get("status") != "ERROR":
                    return data
            return None
        except Exception:
            return None

    async def buscar_empresa(self, cnpj: str) -> dict | None:
        """Busca dados de uma empresa. Usa cache em memória."""
        cnpj_limpo = self._limpar_cnpj(cnpj)
        if len(cnpj_limpo) != 14:
            return None

        if cnpj_limpo in self._cache:
            return self._cache[cnpj_limpo]

        data = await self._brasilapi(cnpj_limpo)
        if not data:
            await asyncio.sleep(1)
            data = await self._receitaws(cnpj_limpo)

        if data:
            normalizado = self._normalizar(cnpj_limpo, data)
            self._cache[cnpj_limpo] = normalizado
            return normalizado

        return None

    async def buscar_em_lote(self, cnpjs: list[str], delay: float = 0.3) -> dict[str, dict]:
        """Busca múltiplos CNPJs com delay para respeitar rate limit."""
        resultados = {}
        cnpjs_unicos = list(set(self._limpar_cnpj(c) for c in cnpjs if c))

        for i, cnpj in enumerate(cnpjs_unicos):
            if cnpj in self._cache:
                resultados[cnpj] = self._cache[cnpj]
                continue

            empresa = await self.buscar_empresa(cnpj)
            if empresa:
                resultados[cnpj] = empresa

            if (i + 1) % 10 == 0:
                logger.debug(f"CNPJ: {i+1}/{len(cnpjs_unicos)}")

            await asyncio.sleep(delay)

        return resultados

    def _normalizar(self, cnpj: str, raw: dict) -> dict:
        """Normaliza dados de qualquer fonte para schema interno."""

        # BrasilAPI format
        if "qsa" in raw:
            socios = [
                {
                    "cnpj": cnpj,
                    "cpf_cnpj_socio": self._limpar_cnpj(s.get("cnpj_cpf_do_socio", "")),
                    "nome_socio": s.get("nome_socio", "").strip(),
                    "nome_normalizado": self._normalizar_nome(s.get("nome_socio", "")),
                    "qualificacao": s.get("qualificacao_socio", ""),
                    "data_entrada": s.get("data_entrada_sociedade"),
                }
                for s in raw.get("qsa", [])
            ]
            cnaes = raw.get("cnaes_secundarios", [])
            return {
                "cnpj": cnpj,
                "razao_social": raw.get("razao_social", "").strip(),
                "nome_fantasia": raw.get("nome_fantasia", "").strip(),
                "situacao_cadastral": raw.get("descricao_situacao_cadastral", ""),
                "data_abertura": raw.get("data_inicio_atividade"),
                "cnae_principal": raw.get("cnae_fiscal", ""),
                "cnae_descricao": raw.get("cnae_fiscal_descricao", ""),
                "natureza_juridica": raw.get("descricao_natureza_juridica", ""),
                "porte": raw.get("descricao_porte", ""),
                "capital_social": raw.get("capital_social"),
                "logradouro": f"{raw.get('logradouro','')} {raw.get('numero','')}".strip(),
                "municipio": raw.get("municipio", ""),
                "uf": raw.get("uf", ""),
                "cep": self._limpar_cnpj(raw.get("cep", "")),
                "socios": socios,
            }

        # ReceitaWS format
        socios = [
            {
                "cnpj": cnpj,
                "cpf_cnpj_socio": "",
                "nome_socio": s.get("nome", "").strip(),
                "nome_normalizado": self._normalizar_nome(s.get("nome", "")),
                "qualificacao": s.get("qual", ""),
                "data_entrada": None,
            }
            for s in raw.get("qsa", [])
        ]
        return {
            "cnpj": cnpj,
            "razao_social": raw.get("nome", "").strip(),
            "nome_fantasia": raw.get("fantasia", "").strip(),
            "situacao_cadastral": raw.get("situacao", ""),
            "data_abertura": raw.get("abertura"),
            "cnae_principal": raw.get("atividade_principal", [{}])[0].get("code", "") if raw.get("atividade_principal") else "",
            "cnae_descricao": raw.get("atividade_principal", [{}])[0].get("text", "") if raw.get("atividade_principal") else "",
            "natureza_juridica": raw.get("natureza_juridica", ""),
            "porte": raw.get("porte", ""),
            "capital_social": None,
            "logradouro": raw.get("logradouro", ""),
            "municipio": raw.get("municipio", ""),
            "uf": raw.get("uf", ""),
            "cep": self._limpar_cnpj(raw.get("cep", "")),
            "socios": socios,
        }

    def _normalizar_nome(self, nome: str) -> str:
        """Remove acentos e coloca em uppercase para comparação."""
        import unicodedata
        if not nome:
            return ""
        nfkd = unicodedata.normalize("NFKD", nome.upper())
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    def detectar_empresa_fantasma(self, empresa: dict) -> bool:
        """
        Empresa aberta recentemente + capital social baixo +
        CNAE genérico são indicadores de empresa de fachada.
        """
        from datetime import date, datetime

        abertura = empresa.get("data_abertura")
        capital = empresa.get("capital_social") or 0

        if abertura:
            try:
                if isinstance(abertura, str):
                    # aceita formatos DD/MM/YYYY e YYYY-MM-DD
                    if "/" in abertura:
                        dt = datetime.strptime(abertura, "%d/%m/%Y").date()
                    else:
                        dt = datetime.fromisoformat(abertura).date()
                else:
                    dt = abertura
                anos = (date.today() - dt).days / 365
                if anos < 1 and capital < 10000:
                    return True
            except Exception:
                pass

        return False
