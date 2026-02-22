"""
PIPELINE DE INGESTÃO
Orquestra coleta → enriquecimento → análise → persistência.
"""

import asyncio
from datetime import date, timedelta
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from collectors.pncp import PNCPCollector
from collectors.cnpj import CNPJCollector
from collectors.tse import TSECollector
from collectors.cgu import CGUCollector
from engine.analyzer import AnalysisEngine


class Pipeline:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.engine = AnalysisEngine()

    # ----------------------------------------------------------
    # PERSISTÊNCIA
    # ----------------------------------------------------------

    async def _upsert_licitacao(self, dados: dict) -> str | None:
        """Insere ou atualiza licitação. Retorna UUID."""
        sql = text("""
            INSERT INTO licitacoes (
                fonte, numero_controle, orgao_cnpj, orgao_nome, orgao_esfera,
                orgao_uf, orgao_municipio, orgao_codigo_ibge, modalidade,
                tipo_objeto, objeto_descricao, objeto_categoria, valor_estimado,
                valor_homologado, data_abertura, data_homologacao, situacao,
                numero_participantes, raw_data
            ) VALUES (
                :fonte, :numero_controle, :orgao_cnpj, :orgao_nome, :orgao_esfera,
                :orgao_uf, :orgao_municipio, :orgao_codigo_ibge, :modalidade,
                :tipo_objeto, :objeto_descricao, :objeto_categoria, :valor_estimado,
                :valor_homologado, :data_abertura, :data_homologacao, :situacao,
                :numero_participantes, :raw_data
            )
            ON CONFLICT (numero_controle) DO UPDATE SET
                valor_homologado = EXCLUDED.valor_homologado,
                situacao = EXCLUDED.situacao,
                numero_participantes = EXCLUDED.numero_participantes,
                atualizado_em = NOW()
            RETURNING id
        """)
        import json
        params = {**dados}
        if "raw_data" in params and params["raw_data"] is not None:
            params["raw_data"] = json.dumps(params["raw_data"])
        params.setdefault("objeto_categoria", None)
        params.setdefault("numero_participantes", None)

        result = await self.db.execute(sql, params)
        row = result.fetchone()
        return str(row[0]) if row else None

    async def _upsert_empresa(self, empresa: dict):
        sql = text("""
            INSERT INTO empresas (
                cnpj, razao_social, nome_fantasia, situacao_cadastral,
                data_abertura, cnae_principal, cnae_descricao, natureza_juridica,
                porte, capital_social, logradouro, municipio, uf, cep
            ) VALUES (
                :cnpj, :razao_social, :nome_fantasia, :situacao_cadastral,
                :data_abertura, :cnae_principal, :cnae_descricao, :natureza_juridica,
                :porte, :capital_social, :logradouro, :municipio, :uf, :cep
            )
            ON CONFLICT (cnpj) DO UPDATE SET
                situacao_cadastral = EXCLUDED.situacao_cadastral,
                atualizado_em = NOW()
        """)
        await self.db.execute(sql, {
            k: empresa.get(k) for k in [
                "cnpj", "razao_social", "nome_fantasia", "situacao_cadastral",
                "data_abertura", "cnae_principal", "cnae_descricao", "natureza_juridica",
                "porte", "capital_social", "logradouro", "municipio", "uf", "cep",
            ]
        })

        # socios
        await self.db.execute(
            text("DELETE FROM socios WHERE cnpj = :cnpj"),
            {"cnpj": empresa["cnpj"]},
        )
        for socio in empresa.get("socios", []):
            await self.db.execute(
                text("""
                    INSERT INTO socios (cnpj, cpf_cnpj_socio, nome_socio, nome_normalizado, qualificacao, data_entrada)
                    VALUES (:cnpj, :cpf_cnpj_socio, :nome_socio, :nome_normalizado, :qualificacao, :data_entrada)
                """),
                socio,
            )

    async def _upsert_politico(self, politico: dict):
        sql = text("""
            INSERT INTO politicos (
                cpf, nome, nome_normalizado, nome_urna, partido, cargo,
                uf, municipio, codigo_ibge, situacao_candidatura, eleito,
                ano_eleicao, patrimonio_declarado
            ) VALUES (
                :cpf, :nome, :nome_normalizado, :nome_urna, :partido, :cargo,
                :uf, :municipio, :codigo_ibge, :situacao_candidatura, :eleito,
                :ano_eleicao, :patrimonio_declarado
            )
            ON CONFLICT (cpf) DO UPDATE SET
                nome = EXCLUDED.nome,
                cargo = EXCLUDED.cargo,
                eleito = EXCLUDED.eleito,
                patrimonio_declarado = EXCLUDED.patrimonio_declarado,
                atualizado_em = NOW()
        """)
        await self.db.execute(sql, {
            k: politico.get(k) for k in [
                "cpf", "nome", "nome_normalizado", "nome_urna", "partido", "cargo",
                "uf", "municipio", "codigo_ibge", "situacao_candidatura", "eleito",
                "ano_eleicao", "patrimonio_declarado",
            ]
        })

    async def _salvar_participante(self, licitacao_id: str, p: dict):
        sql = text("""
            INSERT INTO licitacao_participantes (licitacao_id, cnpj, razao_social, vencedor, valor_proposta)
            VALUES (:licitacao_id, :cnpj, :razao_social, :vencedor, :valor_proposta)
            ON CONFLICT DO NOTHING
        """)
        await self.db.execute(sql, {
            "licitacao_id": licitacao_id,
            "cnpj": p.get("cnpj", ""),
            "razao_social": p.get("razao_social", ""),
            "vencedor": p.get("vencedor", False),
            "valor_proposta": p.get("valor_proposta"),
        })

    async def _salvar_resultado_analise(self, licitacao_id: str, resultado):
        import json
        sql = text("""
            UPDATE licitacoes SET
                score_risco = :score,
                score_detalhes = :detalhes,
                objeto_categoria = :categoria,
                flag_preco_anomalo = :flag_preco,
                flag_relacionamento = :flag_rel,
                flag_objeto_inadequado = :flag_obj,
                flag_participante_unico = :flag_part,
                flag_prazo_suspeito = :flag_prazo,
                atualizado_em = NOW()
            WHERE id = :id
        """)
        await self.db.execute(sql, {
            "id": licitacao_id,
            "score": resultado.score_total,
            "detalhes": json.dumps(resultado.score_detalhes),
            "categoria": resultado.score_detalhes.get("categoria_objeto"),
            "flag_preco": resultado.flags.get("flag_preco_anomalo", False),
            "flag_rel": resultado.flags.get("flag_relacionamento", False),
            "flag_obj": resultado.flags.get("flag_objeto_inadequado", False),
            "flag_part": resultado.flags.get("flag_participante_unico", False),
            "flag_prazo": resultado.flags.get("flag_prazo_suspeito", False),
        })

        # relações detectadas
        for relacao in resultado.anomalias_relacao:
            await self.db.execute(
                text("""
                    INSERT INTO relacoes_detectadas (
                        licitacao_id, tipo_relacao, nome_socio, nome_politico,
                        cargo_politico, uf_politico, tipo_vinculo, similaridade, evidencia
                    ) VALUES (
                        :licitacao_id, :tipo_relacao, :nome_socio, :nome_politico,
                        :cargo_politico, :uf_politico, :tipo_vinculo, :similaridade, :evidencia
                    )
                """),
                {
                    "licitacao_id": licitacao_id,
                    "tipo_relacao": relacao.tipo,
                    "nome_socio": relacao.nome_socio,
                    "nome_politico": relacao.nome_politico,
                    "cargo_politico": relacao.cargo_politico,
                    "uf_politico": relacao.uf_politico,
                    "tipo_vinculo": relacao.tipo,
                    "similaridade": relacao.similaridade,
                    "evidencia": relacao.evidencia,
                },
            )

    # ----------------------------------------------------------
    # HISTÓRICO DE PREÇOS (benchmark interno)
    # ----------------------------------------------------------

    async def _buscar_historico_precos(self, categoria: str, uf: str) -> list[float]:
        """Busca valores históricos de licitações similares no próprio banco."""
        result = await self.db.execute(
            text("""
                SELECT valor_homologado FROM licitacoes
                WHERE objeto_categoria = :cat
                AND orgao_uf = :uf
                AND valor_homologado IS NOT NULL
                AND valor_homologado > 0
                ORDER BY data_abertura DESC
                LIMIT 200
            """),
            {"cat": categoria, "uf": uf},
        )
        return [float(r[0]) for r in result.fetchall()]

    # ----------------------------------------------------------
    # PIPELINE PRINCIPAL
    # ----------------------------------------------------------

    async def executar(
        self,
        uf: str = None,
        codigo_ibge: str = None,
        dias_retroativos: int = 90,
    ):
        logger.info(f"=== PIPELINE INICIADO | UF={uf} | IBGE={codigo_ibge} ===")

        # 1. Carrega políticos da UF
        politicos = []
        if uf:
            async with TSECollector() as tse:
                dados_tse = await tse.coletar_uf_completo(uf)
                politicos = dados_tse["politicos"]
                doacoes = dados_tse["doacoes"]

                # persiste políticos
                for p in politicos:
                    await self._upsert_politico(p)
                await self.db.commit()
                logger.info(f"Políticos persistidos: {len(politicos)}")
        else:
            doacoes = []

        # 2. Coleta licitações
        async with PNCPCollector(self.db) as pncp:
            licitacoes_raw = await pncp.coletar_tudo(
                uf=uf,
                codigo_ibge=codigo_ibge,
                dias_retroativos=dias_retroativos,
            )

        logger.info(f"Licitações coletadas: {len(licitacoes_raw)}")

        # 3. Processa em lotes
        BATCH = 20
        total_anomalias = 0

        async with CNPJCollector() as cnpj_col:
            async with CGUCollector() as cgu:
                for i in range(0, len(licitacoes_raw), BATCH):
                    lote = licitacoes_raw[i:i+BATCH]

                    for lic_raw in lote:
                        try:
                            # persiste licitação
                            lid = await self._upsert_licitacao(lic_raw)
                            if not lid:
                                continue

                            lic_raw["id"] = lid
                            categoria = self.engine.categorizar_objeto(
                                lic_raw.get("objeto_descricao", "")
                            )

                            # busca participantes no PNCP
                            # (simplificado: usa dados já normalizados)
                            participantes = []
                            if lic_raw.get("raw_data"):
                                raw = lic_raw["raw_data"]
                                vencedor_cnpj = raw.get("niFornecedor") or raw.get("cnpjFornecedor", "")
                                vencedor_nome = raw.get("nomeRazaoSocialFornecedor", "")
                                if vencedor_cnpj:
                                    participantes.append({
                                        "cnpj": "".join(filter(str.isdigit, vencedor_cnpj)),
                                        "razao_social": vencedor_nome,
                                        "vencedor": True,
                                        "valor_proposta": lic_raw.get("valor_homologado"),
                                    })

                            # busca dados das empresas
                            cnpjs = [p["cnpj"] for p in participantes if p.get("cnpj")]
                            empresas = {}
                            socios_por_cnpj = {}
                            sancoes_por_cnpj = {}

                            for cnpj in cnpjs:
                                empresa = await cnpj_col.buscar_empresa(cnpj)
                                if empresa:
                                    await self._upsert_empresa(empresa)
                                    empresas[cnpj] = empresa
                                    socios_por_cnpj[cnpj] = empresa.get("socios", [])

                                sancao = await cgu.verificar_cnpj(cnpj)
                                sancoes_por_cnpj[cnpj] = sancao

                                for p in participantes:
                                    if p["cnpj"] == cnpj:
                                        await self._salvar_participante(lid, p)

                            # histórico de preços
                            historico = await self._buscar_historico_precos(
                                categoria, lic_raw.get("orgao_uf", "")
                            )

                            # ANÁLISE
                            resultado = self.engine.analisar_licitacao(
                                licitacao=lic_raw,
                                participantes=participantes,
                                empresas=empresas,
                                socios_por_cnpj=socios_por_cnpj,
                                politicos=politicos,
                                historico_precos=historico,
                                doacoes=doacoes,
                                sancoes_por_cnpj=sancoes_por_cnpj,
                            )

                            if resultado.score_total > 0:
                                total_anomalias += 1

                            await self._salvar_resultado_analise(lid, resultado)

                        except Exception as e:
                            logger.error(f"Erro processando licitação: {e}")
                            continue

                    await self.db.commit()
                    logger.info(f"Lote {i//BATCH + 1}/{(len(licitacoes_raw)-1)//BATCH + 1} concluído")
                    await asyncio.sleep(0.5)

        logger.info(f"=== PIPELINE CONCLUÍDO | {len(licitacoes_raw)} licitações | {total_anomalias} com anomalias ===")
        return {
            "total_licitacoes": len(licitacoes_raw),
            "total_anomalias": total_anomalias,
        }
