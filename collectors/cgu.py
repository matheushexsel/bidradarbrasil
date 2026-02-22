"""
Coletor CGU - CEIS, CNEP, CEPIM
Empresas inidôneas, impedidas e sancionadas.
API: https://api.cgu.gov.br/
"""

import httpx
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

CGU_API = "https://api.cgu.gov.br"


class CGUCollector:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            follow_redirects=True,
            headers={
                "Accept": "application/json",
                "User-Agent": "LicitacaoRadar/1.0",
            },
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.client.aclose()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def _get(self, endpoint: str, params: dict = None) -> dict | None:
        try:
            r = await self.client.get(f"{CGU_API}{endpoint}", params=params)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.error(f"CGU API error {endpoint}: {e}")
            return None

    async def buscar_ceis(self, cnpj_cpf: str) -> list[dict]:
        """Cadastro de Empresas Inidôneas e Suspensas."""
        cnpj_cpf = "".join(filter(str.isdigit, cnpj_cpf or ""))
        data = await self._get("/ceis/v1/sancoes", params={"cpfCnpj": cnpj_cpf})
        sancoes = (data or {}).get("data", [])
        return [
            {
                "cnpj_cpf": cnpj_cpf,
                "nome_sancionado": s.get("nomeInfrator", ""),
                "tipo_pessoa": "PJ" if len(cnpj_cpf) == 14 else "PF",
                "tipo_sancao": s.get("tipoSancao", ""),
                "orgao_sancionador": s.get("orgaoSancionador", {}).get("nome", ""),
                "data_inicio": s.get("dataInicioSancao"),
                "data_fim": s.get("dataFimSancao"),
                "fundamentacao": s.get("fundamentacaoLegal", ""),
                "fonte": "CEIS",
            }
            for s in sancoes
        ]

    async def buscar_cnep(self, cnpj_cpf: str) -> list[dict]:
        """Cadastro Nacional de Empresas Punidas."""
        cnpj_cpf = "".join(filter(str.isdigit, cnpj_cpf or ""))
        data = await self._get("/cnep/v1/sancoes", params={"cpfCnpj": cnpj_cpf})
        sancoes = (data or {}).get("data", [])
        return [
            {
                "cnpj_cpf": cnpj_cpf,
                "nome_sancionado": s.get("nomeInfrator", ""),
                "tipo_pessoa": "PJ" if len(cnpj_cpf) == 14 else "PF",
                "tipo_sancao": s.get("tipoSancao", ""),
                "orgao_sancionador": s.get("orgaoSancionador", {}).get("nome", ""),
                "data_inicio": s.get("dataInicioSancao"),
                "data_fim": s.get("dataFimSancao"),
                "fundamentacao": s.get("fundamentacaoLegal", ""),
                "fonte": "CNEP",
            }
            for s in sancoes
        ]

    async def verificar_cnpj(self, cnpj: str) -> dict:
        """Verifica se um CNPJ está em qualquer lista de sanção."""
        ceis = await self.buscar_ceis(cnpj)
        cnep = await self.buscar_cnep(cnpj)
        return {
            "cnpj": cnpj,
            "sanctionado": len(ceis) > 0 or len(cnep) > 0,
            "sancoes": ceis + cnep,
        }

    async def verificar_em_lote(self, cnpjs: list[str]) -> dict[str, dict]:
        """Verifica lista de CNPJs. Retorna dict cnpj → resultado."""
        import asyncio
        resultados = {}
        for cnpj in set(cnpjs):
            resultados[cnpj] = await self.verificar_cnpj(cnpj)
            await asyncio.sleep(0.2)
        return resultados
