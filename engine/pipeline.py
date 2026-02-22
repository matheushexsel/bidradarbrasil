"""
PIPELINE DE INGESTÃO
Orquestra coleta → enriquecimento → análise → persistência.
"""

import asyncio
import json
from datetime import datetime
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
                situacao         = EXCLUDED.situacao,
                atualizado_em    = NOW()
            RETURNING id
        """)

        CAMPOS_TABELA = {
            "fonte", "numero_controle", "orgao_cnpj", "orgao_nome", "orgao_esfera",
            "orgao_uf", "orgao_municipio", "orgao_codigo_ibge", "modalidade",
            "tipo_objeto", "objeto_descricao", "objeto_categoria", "valor_estimado",
            "valor_homologado", "data_abertura", "data_homologacao", "situacao",
            "numero_participantes", "raw_data",
        }
        params = {k: dados.get(k) for k in CAMPOS_TABELA}

        if params.get("raw_data") is not None and not isinstance(params["raw_data"], str):
            params["raw_data"] = json.dumps(params["raw_data"], ensure_ascii=False)

        params.setdefault("objeto_categoria", None)
        params.setdefault("numero_participantes", None)

        for campo_data in ("data_abertura", "data_homologacao"):
            v = params.get(campo_data)
            if v and isinstance(v, str):
                for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                    try:
                        params[campo_data] = datetime.strptime(v, fmt).date()
                        break
                    except ValueError:
                        pass
                else:
                    logger.warning(f"Data inválida '{v}' em {campo_data} — ignorando")
                    params[campo_data] = None

        try:
            result = await self.db.execute(sql, params)
            row = result.fetchone()
            return str(row[0]) if row else None
        except Exception as e:
            logger.error(f"Erro upsert licitacao {params.get('numero_controle')}: {e}")
            return None

    async def _upsert_empresa(self, empresa: dict):
        """
        Persiste empresa. Tipos já vêm corretos do CNPJCollector._normalizar():
        cnae_principal: str, data_abertura: date|None, capital_social: float|None
        """
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
                razao_social       = EXCLUDED.razao_social,
                situacao_cadastral = EXCLUDED.situacao_cadastral,
                cnae_principal     = EXCLUDED.cnae_principal,
                cnae_descricao     = EXCLUDED.cnae_descricao,
                capital_social     = EXCLUDED.capital_social,
                atualizado_em      = NOW()
        """)
        params = {k: empresa.get(k) for k in [
            "cnpj", "razao_social", "nome_fantasia", "situacao_cadastral",
            "data_abertura", "cnae_principal", "cnae_descricao", "natureza_juridica",
            "porte", "capital_social", "logradouro", "municipio", "uf", "cep",
        ]}

        # Defensive type guarantees (cnpj.py já entrega certo, mas por segurança)
        if params.get("cnae_principal") is not None:
            params["cnae_principal"] = str(params["cnae_principal"])
        if params.get("capital_social") is not None:
            try:
                params["capital_social"] = float(params["capital_social"])
            except (ValueError, TypeError):
                params["capital_social"] = None
        if params.get("data_abertura") and isinstance(params["data_abertura"], str):
            for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                try:
                    params["data_abertura"] = datetime.strptime(params["data_abertura"], fmt).date()
                    break
                except ValueError:
                    pass
            else:
                params["data_abertura"] = None

        await self.db.execute(sql, params)

        cnpj = empresa.get("cnpj", "")
        await self.db.execute(text("DELETE FROM socios WHERE cnpj = :cnpj"), {"cnpj": cnpj})

        for socio in empresa.get("socios", []):
            s = dict(socio)
            if s.get("data_entrada") and isinstance(s["data_entrada"], str):
                for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
                    try:
                        s["data_entrada"] = datetime.strptime(s["data_entrada"], fmt).date()
                        break
                    except ValueError:
                        pass
                else:
                    s["data_entrada"] = None
            await self.db.execute(
                text("""
                    INSERT INTO socios
                        (cnpj, cpf_cnpj_socio, nome_socio, nome_normalizado, qualificacao, data_entrada)
                    VALUES
                        (:cnpj, :cpf_cnpj_socio, :nome_socio, :nome_normalizado, :qualificacao, :data_entrada)
                    ON CONFLICT DO NOTHING
                """),
                s,
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
                nome                 = EXCLUDED.nome,
                cargo                = EXCLUDED.cargo,
                eleito               = EXCLUDED.eleito,
                patrimonio_declarado = EXCLUDED.patrimonio_declarado,
                partido              = EXCLUDED.partido,
                atualizado_em        = NOW()
        """)
        await self.db.execute(sql, {
            k: politico.get(k) for k in [
                "cpf", "nome", "nome_normalizado", "nome_urna", "partido", "cargo",
                "uf", "municipio", "codigo_ibge", "situacao_candidatura", "eleito",
                "ano_eleicao", "patrimonio_declarado",
            ]
        })

    async def _salvar_participante(self, licitacao_id: str, p: dict):
        await self.db.execute(
            text("""
                INSERT INTO licitacao_participantes
                    (licitacao_id, cnpj, razao_social, vencedor, valor_proposta)
                VALUES
                    (:licitacao_id, :cnpj, :razao_social, :vencedor, :valor_proposta)
                ON CONFLICT (licitacao_id, cnpj) DO NOTHING
            """),
            {
                "licitacao_id":   licitacao_id,
                "cnpj":           p.get("cnpj", ""),
                "razao_social":   p.get("razao_social", ""),
                "vencedor":       p.get("vencedor", False),
                "valor_proposta": p.get("valor_proposta"),
            },
        )

    async def _salvar_resultado_analise(self, licitacao_id: str, resultado):
        sql = text("""
            UPDATE licitacoes SET
                score_risco             = :score,
                score_detalhes          = :detalhes,
                objeto_categoria        = :categoria,
                flag_preco_anomalo      = :flag_preco,
                flag_relacionamento     = :flag_rel,
                flag_objeto_inadequado  = :flag_obj,
                flag_participante_unico = FALSE,
                flag_prazo_suspeito     = FALSE,
                atualizado_em           = NOW()
            WHERE id = :id
        """)
        await self.db.execute(sql, {
            "id":         licitacao_id,
            "score":      resultado.score_total,
            "detalhes":   json.dumps(resultado.score_detalhes, ensure_ascii=False),
            "categoria":  resultado.score_detalhes.get("categoria_objeto"),
            "flag_preco": resultado.flags.get("flag_preco_anomalo", False),
            "flag_rel":   resultado.flags.get("flag_relacionamento", False),
            "flag_obj":   resultado.flags.get("flag_objeto_inadequado", False),
        })

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
                    "licitacao_id":   licitacao_id,
                    "tipo_relacao":   relacao.tipo,
                    "nome_socio":     relacao.nome_socio,
                    "nome_politico":  relacao.nome_politico,
                    "cargo_politico": relacao.cargo_politico,
                    "uf_politico":    relacao.uf_politico,
                    "tipo_vinculo":   relacao.tipo,
                    "similaridade":   relacao.similaridade,
                    "evidencia":      relacao.evidencia,
                },
            )

    async def _buscar_historico_precos(self, categoria: str, uf: str) -> list[float]:
        result = await self.db.execute(
            text("""
                SELECT valor_homologado FROM licitacoes
                WHERE objeto_categoria  = :cat
                  AND orgao_uf          = :uf
                  AND valor_homologado IS NOT NULL
                  AND valor_homologado  > 0
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

        # 1. Políticos via TSE
        politicos: list[dict] = []
        doacoes:   list[dict] = []
        if uf:
            try:
                async with TSECollector() as tse:
                    dados_tse = await tse.coletar_uf_completo(uf)
                    politicos = dados_tse.get("politicos", [])
                    doacoes   = dados_tse.get("doacoes", [])

                if politicos:
                    for p in politicos:
                        await self._upsert_politico(p)
                    await self.db.commit()
                    logger.info(f"Políticos persistidos: {len(politicos)} | Doações: {len(doacoes)}")
                else:
                    logger.warning(f"TSE retornou 0 políticos para UF={uf}")
            except Exception as e:
                logger.warning(f"TSE falhou para UF={uf}: {e} — continuando sem dados TSE")

        # 2. Licitações do PNCP
        async with PNCPCollector(self.db) as pncp:
            licitacoes_raw = await pncp.coletar_tudo(
                uf=uf,
                codigo_ibge=codigo_ibge,
                dias_retroativos=dias_retroativos,
            )

        logger.info(f"Licitações coletadas: {len(licitacoes_raw)}")
        if not licitacoes_raw:
            logger.warning("Nenhuma licitação coletada — abortando")
            return {"total_licitacoes": 0, "total_anomalias": 0}

        com_cnpj = sum(1 for l in licitacoes_raw if l.get("cnpj_fornecedor"))
        logger.info(f"Com CNPJ fornecedor: {com_cnpj}/{len(licitacoes_raw)}")

        # 3. Processa em lotes
        BATCH = 20
        total_anomalias = 0
        total_empresas  = 0
        total_sem_cnpj  = 0

        async with CNPJCollector() as cnpj_col:
            async with CGUCollector() as cgu:
                for i in range(0, len(licitacoes_raw), BATCH):
                    lote = licitacoes_raw[i : i + BATCH]

                    for lic_raw in lote:
                        try:
                            lid = await self._upsert_licitacao(lic_raw)
                            if not lid:
                                continue

                            lic_raw["id"] = lid

                            # Vencedor vem direto do PNCPCollector (normalizar_contrato)
                            participantes: list[dict] = []
                            vencedor_cnpj = (lic_raw.get("cnpj_fornecedor") or "").strip()
                            vencedor_nome = (lic_raw.get("nome_fornecedor") or "").strip()

                            if vencedor_cnpj:
                                participantes.append({
                                    "cnpj":           vencedor_cnpj,
                                    "razao_social":   vencedor_nome,
                                    "vencedor":       True,
                                    "valor_proposta": lic_raw.get("valor_homologado"),
                                })
                            else:
                                total_sem_cnpj += 1

                            empresas:         dict = {}
                            socios_por_cnpj:  dict = {}
                            sancoes_por_cnpj: dict = {}

                            for p in participantes:
                                cnpj = p.get("cnpj", "")
                                if not cnpj:
                                    continue

                                empresa = await cnpj_col.buscar_empresa(cnpj)
                                if empresa:
                                    await self._upsert_empresa(empresa)
                                    empresas[cnpj]        = empresa
                                    socios_por_cnpj[cnpj] = empresa.get("socios", [])
                                    total_empresas += 1
                                    logger.debug(f"Empresa: {cnpj} {empresa.get('razao_social','')}")

                                sancao = await cgu.verificar_cnpj(cnpj)
                                sancoes_por_cnpj[cnpj] = sancao

                                await self._salvar_participante(lid, p)

                            categoria = self.engine.categorizar_objeto(
                                lic_raw.get("objeto_descricao", "")
                            )
                            historico = await self._buscar_historico_precos(
                                categoria, lic_raw.get("orgao_uf", "")
                            )

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
                            logger.error(
                                f"Erro licitação {lic_raw.get('numero_controle','?')}: {e}"
                            )
                            import traceback
                            logger.debug(traceback.format_exc())
                            # Rollback obrigatório — sem isso a transação fica em estado
                            # 'aborted' e todas as queries seguintes falham em cascata
                            try:
                                await self.db.rollback()
                            except Exception:
                                pass
                            continue

                    await self.db.commit()
                    logger.info(
                        f"Lote {i // BATCH + 1}/{(len(licitacoes_raw) - 1) // BATCH + 1} | "
                        f"empresas={total_empresas} sem_cnpj={total_sem_cnpj}"
                    )
                    await asyncio.sleep(0.3)

        logger.info(
            f"=== PIPELINE CONCLUÍDO | {len(licitacoes_raw)} licitações | "
            f"{total_anomalias} anomalias | {total_empresas} empresas | "
            f"{len(politicos)} políticos | {total_sem_cnpj} sem CNPJ ==="
        )
        return {
            "total_licitacoes": len(licitacoes_raw),
            "total_anomalias":  total_anomalias,
            "total_empresas":   total_empresas,
        }
