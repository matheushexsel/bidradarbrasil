"""
PIPELINE DE INGESTÃO
Orquestra coleta → enriquecimento → análise → persistência.

Regras de robustez aplicadas em todo o arquivo:
- Nunca usar ON CONFLICT com coluna sem UNIQUE constraint declarada no schema.
  Usar WHERE NOT EXISTS ou ON CONFLICT (coluna_com_pk_ou_unique).
- Todo try/except que captura erro de DB faz rollback antes de continuar,
  para não deixar a transação em estado 'aborted' que derruba as próximas queries.
- Socios: DELETE + INSERTs dentro de SAVEPOINT para garantir atomicidade.
- data_homologacao: nunca salvar se for anterior a data_abertura (dado corrompido do PNCP).
- raw_data: truncado a 50k chars para evitar problema com JSONs gigantes.
- relacoes_detectadas: WHERE NOT EXISTS para evitar duplicatas entre ciclos.
"""

import asyncio
import json
import traceback
from datetime import datetime
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from collectors.pncp import PNCPCollector
from collectors.cnpj import CNPJCollector
from collectors.tse import TSECollector
from collectors.cgu import CGUCollector
from engine.analyzer import AnalysisEngine

RAW_DATA_MAX_CHARS = 50_000  # limite de tamanho para raw_data no banco


def _parse_date(valor) -> "date | None":
    """Converte string ou date para date. Retorna None se inválido."""
    from datetime import date
    if not valor:
        return None
    if isinstance(valor, date):
        return valor
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(valor).strip(), fmt).date()
        except ValueError:
            continue
    return None


class Pipeline:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.engine = AnalysisEngine()

    # ----------------------------------------------------------
    # PERSISTÊNCIA
    # ----------------------------------------------------------

    async def _upsert_licitacao(self, dados: dict) -> str | None:
        """
        Upsert de licitação. ON CONFLICT em numero_controle (tem UNIQUE no schema).
        Retorna o UUID ou None se falhar.
        """
        CAMPOS_TABELA = {
            "fonte", "numero_controle", "orgao_cnpj", "orgao_nome", "orgao_esfera",
            "orgao_uf", "orgao_municipio", "orgao_codigo_ibge", "modalidade",
            "tipo_objeto", "objeto_descricao", "objeto_categoria", "valor_estimado",
            "valor_homologado", "data_abertura", "data_homologacao", "situacao",
            "numero_participantes", "raw_data",
        }
        params = {k: dados.get(k) for k in CAMPOS_TABELA}

        # Serializa raw_data com limite de tamanho
        if params.get("raw_data") is not None:
            if not isinstance(params["raw_data"], str):
                params["raw_data"] = json.dumps(params["raw_data"], ensure_ascii=False)
            if len(params["raw_data"]) > RAW_DATA_MAX_CHARS:
                params["raw_data"] = params["raw_data"][:RAW_DATA_MAX_CHARS]

        params.setdefault("objeto_categoria", None)
        params.setdefault("numero_participantes", None)

        # Conversão de datas
        data_abertura   = _parse_date(params.get("data_abertura"))
        data_homologacao = _parse_date(params.get("data_homologacao"))

        # Sanitiza data_homologacao: se for anterior à abertura, é dado corrompido
        if data_abertura and data_homologacao and data_homologacao < data_abertura:
            logger.warning(
                f"data_homologacao {data_homologacao} < data_abertura {data_abertura} "
                f"em {params.get('numero_controle')} — ignorando data_homologacao"
            )
            data_homologacao = None

        params["data_abertura"]    = data_abertura
        params["data_homologacao"] = data_homologacao

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

        try:
            result = await self.db.execute(sql, params)
            row = result.fetchone()
            return str(row[0]) if row else None
        except Exception as e:
            logger.error(f"Erro upsert licitacao {params.get('numero_controle')}: {e}")
            try:
                await self.db.rollback()
            except Exception:
                pass
            return None

    async def _upsert_empresa(self, empresa: dict):
        """
        Upsert de empresa + sócios.
        Sócios usam SAVEPOINT para atomicidade: se algum INSERT falhar,
        apenas os sócios são revertidos (não a empresa inteira).
        """
        sql_empresa = text("""
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

        # Defensive type coercion (cnpj.py já entrega certo, mas por segurança)
        if params.get("cnae_principal") is not None:
            params["cnae_principal"] = str(params["cnae_principal"])
        if params.get("capital_social") is not None:
            try:
                params["capital_social"] = float(params["capital_social"])
            except (ValueError, TypeError):
                params["capital_social"] = None
        params["data_abertura"] = _parse_date(params.get("data_abertura"))

        await self.db.execute(sql_empresa, params)

        # Sócios dentro de SAVEPOINT — se falhar, reverte só os sócios
        cnpj = empresa.get("cnpj", "")
        socios = empresa.get("socios", [])
        if socios:
            try:
                await self.db.execute(text("SAVEPOINT sp_socios"))
                await self.db.execute(
                    text("DELETE FROM socios WHERE cnpj = :cnpj"), {"cnpj": cnpj}
                )
                for socio in socios:
                    s = dict(socio)
                    s["data_entrada"] = _parse_date(s.get("data_entrada"))
                    # Trunca strings longas que podem estourar VARCHAR
                    s["nome_socio"]      = (s.get("nome_socio") or "")[:255]
                    s["nome_normalizado"] = (s.get("nome_normalizado") or "")[:255]
                    s["qualificacao"]    = (s.get("qualificacao") or "")[:100]
                    s["cpf_cnpj_socio"]  = (s.get("cpf_cnpj_socio") or "")[:18]
                    # WHERE NOT EXISTS: evita duplicata sem depender de constraint
                    s_params = {
                        "s_cnpj":        s.get("cnpj", ""),
                        "cpf_cnpj_socio": s.get("cpf_cnpj_socio", ""),
                        "nome_socio":    s.get("nome_socio", ""),
                        "s_nome_norm":   s.get("nome_normalizado", ""),
                        "qualificacao":  s.get("qualificacao", ""),
                        "data_entrada":  s.get("data_entrada"),
                    }
                    await self.db.execute(
                        text("""
                            INSERT INTO socios
                                (cnpj, cpf_cnpj_socio, nome_socio, nome_normalizado,
                                 qualificacao, data_entrada)
                            SELECT :s_cnpj, :cpf_cnpj_socio, :nome_socio, :s_nome_norm,
                                   :qualificacao, :data_entrada
                            WHERE NOT EXISTS (
                                SELECT 1 FROM socios
                                WHERE cnpj = :s_cnpj
                                  AND nome_normalizado = :s_nome_norm
                            )
                        """),
                        s_params,
                    )
                await self.db.execute(text("RELEASE SAVEPOINT sp_socios"))
            except Exception as e:
                logger.warning(f"Erro ao salvar sócios de {cnpj}: {e} — revertendo sócios")
                try:
                    await self.db.execute(text("ROLLBACK TO SAVEPOINT sp_socios"))
                    await self.db.execute(text("RELEASE SAVEPOINT sp_socios"))
                except Exception:
                    pass

    async def _upsert_politico(self, politico: dict):
        """ON CONFLICT em cpf (tem UNIQUE no schema)."""
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
        params = {k: politico.get(k) for k in [
            "cpf", "nome", "nome_normalizado", "nome_urna", "partido", "cargo",
            "uf", "municipio", "codigo_ibge", "situacao_candidatura", "eleito",
            "ano_eleicao", "patrimonio_declarado",
        ]}
        # Sanitiza CPF: sem CPF válido não insere (evita erro de constraint NOT NULL)
        cpf = "".join(filter(str.isdigit, str(params.get("cpf") or "")))
        if not cpf:
            return
        params["cpf"] = cpf
        await self.db.execute(sql, params)

    async def _salvar_participante(self, licitacao_id: str, p: dict):
        """
        WHERE NOT EXISTS: evita duplicatas sem depender de UNIQUE constraint.
        Funciona mesmo se a constraint ALTER TABLE ainda não foi rodada.
        """
        await self.db.execute(
            text("""
                INSERT INTO licitacao_participantes
                    (licitacao_id, cnpj, razao_social, vencedor, valor_proposta)
                SELECT :p_lid, :p_cnpj, :razao_social, :vencedor, :valor_proposta
                WHERE NOT EXISTS (
                    SELECT 1 FROM licitacao_participantes
                    WHERE licitacao_id = :p_lid AND cnpj = :p_cnpj
                )
            """),
            {
                "p_lid":          licitacao_id,
                "p_cnpj":         (p.get("cnpj") or "")[:18],
                "razao_social":   (p.get("razao_social") or "")[:255],
                "vencedor":       bool(p.get("vencedor", False)),
                "valor_proposta": p.get("valor_proposta"),
            },
        )

    async def _salvar_resultado_analise(self, licitacao_id: str, resultado):
        """
        Salva score e flags. flag_participante_unico e flag_prazo_suspeito
        são sempre FALSE — não temos dados confiáveis para eles.
        """
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
            "flag_preco": bool(resultado.flags.get("flag_preco_anomalo", False)),
            "flag_rel":   bool(resultado.flags.get("flag_relacionamento", False)),
            "flag_obj":   bool(resultado.flags.get("flag_objeto_inadequado", False)),
        })

        # Relações detectadas: WHERE NOT EXISTS para evitar duplicatas entre ciclos
        for relacao in resultado.anomalias_relacao:
            await self.db.execute(
                text("""
                    INSERT INTO relacoes_detectadas (
                        licitacao_id, tipo_relacao, nome_socio, nome_politico,
                        cargo_politico, uf_politico, tipo_vinculo, similaridade, evidencia
                    )
                    SELECT
                        :r_lid, :r_tipo, :r_socio, :r_politico,
                        :cargo_politico, :uf_politico, :tipo_vinculo, :similaridade, :evidencia
                    WHERE NOT EXISTS (
                        SELECT 1 FROM relacoes_detectadas
                        WHERE licitacao_id = :r_lid
                          AND tipo_relacao  = :r_tipo
                          AND nome_socio    = :r_socio
                          AND nome_politico = :r_politico
                    )
                """),
                {
                    "r_lid":          licitacao_id,
                    "r_tipo":         (relacao.tipo or "")[:50],
                    "r_socio":        (relacao.nome_socio or "")[:255],
                    "r_politico":     (relacao.nome_politico or "")[:255],
                    "cargo_politico": (relacao.cargo_politico or "")[:100],
                    "uf_politico":    (relacao.uf_politico or "")[:2],
                    "tipo_vinculo":   (relacao.tipo or "")[:50],
                    "similaridade":   float(relacao.similaridade or 0),
                    "evidencia":      (relacao.evidencia or "")[:500],
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
                    salvos = 0
                    for p in politicos:
                        try:
                            await self._upsert_politico(p)
                            salvos += 1
                        except Exception as e:
                            logger.warning(f"Erro politico {p.get('cpf','?')}: {e}")
                            try:
                                await self.db.rollback()
                            except Exception:
                                pass
                    await self.db.commit()
                    logger.info(f"Políticos: {salvos}/{len(politicos)} salvos | Doações: {len(doacoes)}")
                else:
                    logger.warning(f"TSE retornou 0 políticos para UF={uf}")
            except Exception as e:
                logger.warning(f"TSE falhou para UF={uf}: {e} — continuando sem dados TSE")
                try:
                    await self.db.rollback()
                except Exception:
                    pass

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
                        numero = lic_raw.get("numero_controle", "?")
                        try:
                            # ── 1. Persiste licitação ──────────────────────
                            lid = await self._upsert_licitacao(lic_raw)
                            if not lid:
                                # _upsert_licitacao já fez rollback internamente
                                continue

                            lic_raw["id"] = lid

                            # ── 2. Monta participantes ─────────────────────
                            participantes: list[dict] = []
                            vencedor_cnpj = "".join(filter(
                                str.isdigit,
                                str(lic_raw.get("cnpj_fornecedor") or "")
                            ))
                            vencedor_nome = (lic_raw.get("nome_fornecedor") or "")[:255]

                            if vencedor_cnpj:
                                participantes.append({
                                    "cnpj":           vencedor_cnpj,
                                    "razao_social":   vencedor_nome,
                                    "vencedor":       True,
                                    "valor_proposta": lic_raw.get("valor_homologado"),
                                })
                            else:
                                total_sem_cnpj += 1

                            # ── 3. Enriquece empresas ──────────────────────
                            empresas:         dict = {}
                            socios_por_cnpj:  dict = {}
                            sancoes_por_cnpj: dict = {}

                            for p in participantes:
                                cnpj = p.get("cnpj", "")
                                if not cnpj:
                                    continue

                                # Busca Receita Federal
                                try:
                                    empresa = await cnpj_col.buscar_empresa(cnpj)
                                    if empresa:
                                        await self._upsert_empresa(empresa)
                                        empresas[cnpj]        = empresa
                                        socios_por_cnpj[cnpj] = empresa.get("socios", [])
                                        total_empresas += 1
                                        logger.debug(
                                            f"Empresa: {cnpj} {empresa.get('razao_social','')}"
                                        )
                                except Exception as e:
                                    logger.warning(f"Erro empresa {cnpj}: {e}")
                                    try:
                                        await self.db.rollback()
                                    except Exception:
                                        pass

                                # Busca sanções CGU (bloqueado no Railway — falha silenciosa)
                                try:
                                    sancao = await cgu.verificar_cnpj(cnpj)
                                    sancoes_por_cnpj[cnpj] = sancao
                                except Exception:
                                    sancoes_por_cnpj[cnpj] = None

                                # Salva participante
                                try:
                                    await self._salvar_participante(lid, p)
                                except Exception as e:
                                    logger.warning(f"Erro participante {cnpj}: {e}")
                                    try:
                                        await self.db.rollback()
                                    except Exception:
                                        pass

                            # ── 4. Análise de risco ────────────────────────
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
                            logger.error(f"Erro crítico licitação {numero}: {e}")
                            logger.debug(traceback.format_exc())
                            try:
                                await self.db.rollback()
                            except Exception:
                                pass
                            continue

                    # Commit do lote
                    try:
                        await self.db.commit()
                    except Exception as e:
                        logger.error(f"Erro commit lote {i // BATCH + 1}: {e}")
                        try:
                            await self.db.rollback()
                        except Exception:
                            pass

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
