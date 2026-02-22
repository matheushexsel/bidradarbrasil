"""
Coletor CNPJ + QSA - Receita Federal via BrasilAPI + ReceitaWS
Estratégia: BrasilAPI primeiro (sem auth), ReceitaWS como fallback.
"""

import asyncio
import unicodedata
from datetime import date, datetime
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import httpx

BRASILAPI_URL = "https://brasilapi.com.br/api/cnpj/v1"
RECEITAWS_URL = "https://www.receitaws.com.br/v1/cnpj"


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

    def _normalizar_nome(self, nome: str) -> str:
        if not nome:
            return ""
        nfkd = unicodedata.normalize("NFKD", nome.upper())
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    def _para_date(self, valor) -> date | None:
        """Converte qualquer formato de data para date. Retorna None se inválido."""
        if not valor:
            return None
        if isinstance(valor, date):
            return valor
        if isinstance(valor, str):
            valor = valor.strip()
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(valor, fmt).date()
                except ValueError:
                    continue
        return None

    def _para_str(self, valor) -> str:
        """Garante que qualquer valor vira string."""
        if valor is None:
            return ""
        return str(valor).strip()

    def _para_float(self, valor) -> float | None:
        """Converte para float, retorna None se inválido."""
        if valor is None:
            return None
        try:
            return float(str(valor).replace(",", ".").replace(" ", ""))
        except (ValueError, TypeError):
            return None

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
        """
        Normaliza dados de qualquer fonte para schema interno.
        Todos os campos são convertidos para o tipo correto aqui —
        nunca depender do pipeline para fazer conversões de tipo.
        """

        # ── BrasilAPI ──────────────────────────────────────────────
        if "qsa" in raw or "cnae_fiscal" in raw:
            socios = [
                {
                    "cnpj": cnpj,
                    "cpf_cnpj_socio": self._limpar_cnpj(s.get("cnpj_cpf_do_socio", "")),
                    "nome_socio": self._para_str(s.get("nome_socio", "")),
                    "nome_normalizado": self._normalizar_nome(s.get("nome_socio", "")),
                    "qualificacao": self._para_str(s.get("qualificacao_socio", "")),
                    # data_entrada pode ser string ou None
                    "data_entrada": self._para_date(s.get("data_entrada_sociedade")),
                }
                for s in raw.get("qsa", [])
            ]
            return {
                "cnpj": cnpj,
                "razao_social": self._para_str(raw.get("razao_social", "")),
                "nome_fantasia": self._para_str(raw.get("nome_fantasia", "")),
                "situacao_cadastral": self._para_str(raw.get("descricao_situacao_cadastral", "")),
                # data_inicio_atividade vem como "YYYY-MM-DD" string
                "data_abertura": self._para_date(raw.get("data_inicio_atividade")),
                # cnae_fiscal vem como INT na BrasilAPI — converte para str
                "cnae_principal": self._para_str(raw.get("cnae_fiscal", "")),
                "cnae_descricao": self._para_str(raw.get("cnae_fiscal_descricao", "")),
                "natureza_juridica": self._para_str(raw.get("descricao_natureza_juridica", "")),
                "porte": self._para_str(raw.get("descricao_porte", "")),
                # capital_social vem como float ou int
                "capital_social": self._para_float(raw.get("capital_social")),
                "logradouro": self._para_str(
                    f"{raw.get('logradouro', '')} {raw.get('numero', '')}".strip()
                ),
                "municipio": self._para_str(raw.get("municipio", "")),
                "uf": self._para_str(raw.get("uf", "")),
                "cep": self._limpar_cnpj(raw.get("cep", "")),
                "socios": socios,
            }

        # ── ReceitaWS ──────────────────────────────────────────────
        socios = [
            {
                "cnpj": cnpj,
                "cpf_cnpj_socio": "",
                "nome_socio": self._para_str(s.get("nome", "")),
                "nome_normalizado": self._normalizar_nome(s.get("nome", "")),
                "qualificacao": self._para_str(s.get("qual", "")),
                "data_entrada": None,
            }
            for s in raw.get("qsa", [])
        ]

        atividade = raw.get("atividade_principal", [{}])
        atividade_item = atividade[0] if atividade else {}

        return {
            "cnpj": cnpj,
            "razao_social": self._para_str(raw.get("nome", "")),
            "nome_fantasia": self._para_str(raw.get("fantasia", "")),
            "situacao_cadastral": self._para_str(raw.get("situacao", "")),
            # ReceitaWS retorna abertura como "DD/MM/YYYY"
            "data_abertura": self._para_date(raw.get("abertura")),
            # code vem como "XX.XX-X" — já é string mas garante
            "cnae_principal": self._para_str(atividade_item.get("code", "")),
            "cnae_descricao": self._para_str(atividade_item.get("text", "")),
            "natureza_juridica": self._para_str(raw.get("natureza_juridica", "")),
            "porte": self._para_str(raw.get("porte", "")),
            "capital_social": self._para_float(raw.get("capital_social")),
            "logradouro": self._para_str(raw.get("logradouro", "")),
            "municipio": self._para_str(raw.get("municipio", "")),
            "uf": self._para_str(raw.get("uf", "")),
            "cep": self._limpar_cnpj(raw.get("cep", "")),
            "socios": socios,
        }

    def detectar_empresa_fantasma(self, empresa: dict) -> bool:
        """
        Empresa aberta recentemente + capital social baixo = possível empresa de fachada.
        """
        abertura = empresa.get("data_abertura")
        capital = empresa.get("capital_social") or 0

        if abertura:
            try:
                dt = abertura if isinstance(abertura, date) else None
                if dt:
                    anos = (date.today() - dt).days / 365
                    if anos < 1 and capital < 10000:
                        return True
            except Exception:
                pass

        return False
