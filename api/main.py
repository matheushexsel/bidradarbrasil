"""
LICITACAO RADAR - API
Todos os endpoints necessários para o frontend Lovable.
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Query, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from pydantic import BaseModel
from typing import Optional
from loguru import logger

from database.db import get_db, init_db
from engine.pipeline import Pipeline


# ============================================================
# STARTUP
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Inicializando banco de dados...")
    await init_db()
    logger.info("API pronta.")
    yield


app = FastAPI(
    title="Licitação Radar API",
    description="Detecção de anomalias em licitações públicas brasileiras",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# SCHEMAS
# ============================================================

class ColettaRequest(BaseModel):
    uf: Optional[str] = None
    codigo_ibge: Optional[str] = None
    dias_retroativos: int = 90


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


# ============================================================
# DASHBOARD - Estatísticas gerais
# ============================================================

@app.get("/api/v1/dashboard")
async def dashboard(
    uf: Optional[str] = Query(None),
    esfera: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Retorna estatísticas agregadas para o painel principal."""

    filters = []
    params = {}
    if uf:
        filters.append("orgao_uf = :uf")
        params["uf"] = uf.upper()
    if esfera:
        filters.append("orgao_esfera = :esfera")
        params["esfera"] = esfera.upper()

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    stats = await db.execute(text(f"""
        SELECT
            COUNT(*) AS total_licitacoes,
            COUNT(*) FILTER (WHERE score_risco >= 50) AS alto_risco,
            COUNT(*) FILTER (WHERE score_risco >= 30) AS medio_risco,
            COUNT(*) FILTER (WHERE flag_relacionamento) AS com_relacionamento,
            COUNT(*) FILTER (WHERE flag_preco_anomalo) AS preco_anomalo,
            COUNT(*) FILTER (WHERE flag_participante_unico) AS participante_unico,
            COUNT(*) FILTER (WHERE flag_objeto_inadequado) AS objeto_inadequado,
            SUM(valor_homologado) AS valor_total,
            SUM(valor_homologado) FILTER (WHERE score_risco >= 50) AS valor_alto_risco,
            AVG(score_risco) AS score_medio
        FROM licitacoes {where}
    """), params)
    row = stats.fetchone()

    # distribuição por UF
    dist_uf = await db.execute(text(f"""
        SELECT orgao_uf, COUNT(*) as total, AVG(score_risco) as score_medio
        FROM licitacoes {where}
        WHERE orgao_uf IS NOT NULL
        GROUP BY orgao_uf
        ORDER BY score_medio DESC
        LIMIT 30
    """), params)

    # distribuição por categoria
    dist_cat = await db.execute(text(f"""
        SELECT objeto_categoria, COUNT(*) as total, AVG(score_risco) as score_medio
        FROM licitacoes {where}
        WHERE objeto_categoria IS NOT NULL
        GROUP BY objeto_categoria
        ORDER BY total DESC
        LIMIT 15
    """), params)

    # série temporal (30 dias)
    serie = await db.execute(text(f"""
        SELECT
            DATE_TRUNC('day', data_abertura) as dia,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE score_risco >= 50) as alto_risco
        FROM licitacoes {where}
        WHERE data_abertura >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY 1 ORDER BY 1
    """), params)

    return {
        "totais": dict(row._mapping) if row else {},
        "por_uf": [dict(r._mapping) for r in dist_uf.fetchall()],
        "por_categoria": [dict(r._mapping) for r in dist_cat.fetchall()],
        "serie_temporal": [
            {
                "dia": str(r[0])[:10] if r[0] else None,
                "total": r[1],
                "alto_risco": r[2],
            }
            for r in serie.fetchall()
        ],
    }


# ============================================================
# LICITAÇÕES - Listagem com filtros
# ============================================================

@app.get("/api/v1/licitacoes")
async def listar_licitacoes(
    uf: Optional[str] = Query(None),
    municipio: Optional[str] = Query(None),
    esfera: Optional[str] = Query(None),
    score_min: float = Query(0),
    flag: Optional[str] = Query(None, description="preco|relacionamento|objeto|participante"),
    categoria: Optional[str] = Query(None),
    busca: Optional[str] = Query(None),
    pagina: int = Query(1, ge=1),
    tamanho: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    filters = ["score_risco >= :score_min"]
    params: dict = {"score_min": score_min}

    if uf:
        filters.append("orgao_uf = :uf")
        params["uf"] = uf.upper()
    if municipio:
        filters.append("orgao_codigo_ibge = :ibge")
        params["ibge"] = municipio
    if esfera:
        filters.append("orgao_esfera = :esfera")
        params["esfera"] = esfera.upper()
    if categoria:
        filters.append("objeto_categoria = :cat")
        params["cat"] = categoria
    if busca:
        filters.append("objeto_descricao ILIKE :busca")
        params["busca"] = f"%{busca}%"
    if flag == "preco":
        filters.append("flag_preco_anomalo = TRUE")
    elif flag == "relacionamento":
        filters.append("flag_relacionamento = TRUE")
    elif flag == "objeto":
        filters.append("flag_objeto_inadequado = TRUE")
    elif flag == "participante":
        filters.append("flag_participante_unico = TRUE")

    where = "WHERE " + " AND ".join(filters)
    params["offset"] = (pagina - 1) * tamanho
    params["limit"] = tamanho

    total_result = await db.execute(
        text(f"SELECT COUNT(*) FROM licitacoes {where}"), params
    )
    total = total_result.scalar()

    result = await db.execute(
        text(f"""
            SELECT
                l.id, l.orgao_nome, l.orgao_uf, l.orgao_municipio, l.orgao_esfera,
                l.modalidade, l.objeto_descricao, l.objeto_categoria,
                l.valor_estimado, l.valor_homologado, l.data_abertura,
                l.score_risco, l.score_detalhes,
                l.flag_preco_anomalo, l.flag_relacionamento,
                l.flag_objeto_inadequado, l.flag_participante_unico, l.flag_prazo_suspeito,
                l.numero_participantes, l.situacao,
                MAX(p.razao_social) FILTER (WHERE p.vencedor) AS empresa_vencedora,
                MAX(p.cnpj) FILTER (WHERE p.vencedor) AS cnpj_vencedor
            FROM licitacoes l
            LEFT JOIN licitacao_participantes p ON p.licitacao_id = l.id
            {where}
            GROUP BY l.id
            ORDER BY l.score_risco DESC, l.data_abertura DESC
            LIMIT :limit OFFSET :offset
        """),
        params,
    )

    return {
        "total": total,
        "pagina": pagina,
        "total_paginas": (total + tamanho - 1) // tamanho,
        "dados": [dict(r._mapping) for r in result.fetchall()],
    }


# ============================================================
# LICITAÇÃO - Detalhe
# ============================================================

@app.get("/api/v1/licitacoes/{licitacao_id}")
async def detalhe_licitacao(
    licitacao_id: str,
    db: AsyncSession = Depends(get_db),
):
    lic = await db.execute(
        text("SELECT * FROM licitacoes WHERE id = :id"),
        {"id": licitacao_id},
    )
    row = lic.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Licitação não encontrada")

    # participantes
    parts = await db.execute(
        text("SELECT * FROM licitacao_participantes WHERE licitacao_id = :id"),
        {"id": licitacao_id},
    )

    # relações detectadas
    relacoes = await db.execute(
        text("SELECT * FROM relacoes_detectadas WHERE licitacao_id = :id ORDER BY similaridade DESC"),
        {"id": licitacao_id},
    )

    return {
        "licitacao": dict(row._mapping),
        "participantes": [dict(r._mapping) for r in parts.fetchall()],
        "relacoes": [dict(r._mapping) for r in relacoes.fetchall()],
    }


# ============================================================
# EMPRESAS
# ============================================================

@app.get("/api/v1/empresas/{cnpj}")
async def detalhe_empresa(cnpj: str, db: AsyncSession = Depends(get_db)):
    cnpj_limpo = "".join(filter(str.isdigit, cnpj))

    empresa = await db.execute(
        text("SELECT * FROM empresas WHERE cnpj = :cnpj"),
        {"cnpj": cnpj_limpo},
    )
    row = empresa.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")

    socios = await db.execute(
        text("SELECT * FROM socios WHERE cnpj = :cnpj"),
        {"cnpj": cnpj_limpo},
    )

    contratos = await db.execute(
        text("""
            SELECT l.id, l.orgao_nome, l.orgao_uf, l.objeto_descricao,
                   l.valor_homologado, l.data_abertura, l.score_risco,
                   p.valor_proposta
            FROM licitacao_participantes p
            JOIN licitacoes l ON l.id = p.licitacao_id
            WHERE p.cnpj = :cnpj AND p.vencedor = TRUE
            ORDER BY l.data_abertura DESC
            LIMIT 50
        """),
        {"cnpj": cnpj_limpo},
    )

    sancoes = await db.execute(
        text("SELECT * FROM sancoes WHERE cnpj_cpf = :cnpj"),
        {"cnpj": cnpj_limpo},
    )

    relacoes = await db.execute(
        text("SELECT DISTINCT nome_politico, cargo_politico, uf_politico, tipo_vinculo, evidencia FROM relacoes_detectadas WHERE cnpj_empresa = :cnpj"),
        {"cnpj": cnpj_limpo},
    )

    return {
        "empresa": dict(row._mapping),
        "socios": [dict(r._mapping) for r in socios.fetchall()],
        "contratos": [dict(r._mapping) for r in contratos.fetchall()],
        "sancoes": [dict(r._mapping) for r in sancoes.fetchall()],
        "relacoes_politicas": [dict(r._mapping) for r in relacoes.fetchall()],
    }


@app.get("/api/v1/empresas")
async def ranking_empresas_suspeitas(
    uf: Optional[str] = Query(None),
    pagina: int = Query(1, ge=1),
    tamanho: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    where = "WHERE e.uf = :uf" if uf else ""
    params = {"uf": uf.upper()} if uf else {}
    params["offset"] = (pagina - 1) * tamanho
    params["limit"] = tamanho

    result = await db.execute(
        text(f"""
            SELECT
                e.cnpj, e.razao_social, e.uf, e.municipio, e.cnae_descricao,
                e.flag_empresa_fantasma, e.flag_cnae_divergente, e.flag_sanctionada,
                COUNT(DISTINCT p.licitacao_id) AS total_vitorias,
                SUM(p.valor_proposta) AS total_contratado,
                COUNT(DISTINCT r.id) AS total_relacoes_politicas,
                AVG(l.score_risco) AS score_risco_medio
            FROM empresas e
            JOIN licitacao_participantes p ON p.cnpj = e.cnpj AND p.vencedor = TRUE
            JOIN licitacoes l ON l.id = p.licitacao_id
            LEFT JOIN relacoes_detectadas r ON r.cnpj_empresa = e.cnpj
            {where}
            GROUP BY e.cnpj, e.razao_social, e.uf, e.municipio, e.cnae_descricao,
                     e.flag_empresa_fantasma, e.flag_cnae_divergente, e.flag_sanctionada
            ORDER BY score_risco_medio DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """),
        params,
    )

    return {"dados": [dict(r._mapping) for r in result.fetchall()]}


# ============================================================
# POLÍTICOS
# ============================================================

@app.get("/api/v1/politicos/{cpf}")
async def detalhe_politico(cpf: str, db: AsyncSession = Depends(get_db)):
    cpf_limpo = "".join(filter(str.isdigit, cpf))

    pol = await db.execute(
        text("SELECT * FROM politicos WHERE cpf = :cpf"),
        {"cpf": cpf_limpo},
    )
    row = pol.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Político não encontrado")

    relacoes = await db.execute(
        text("""
            SELECT r.*, l.orgao_nome, l.valor_homologado, l.data_abertura, l.score_risco
            FROM relacoes_detectadas r
            JOIN licitacoes l ON l.id = r.licitacao_id
            WHERE r.cpf_politico = :cpf
            ORDER BY l.score_risco DESC
        """),
        {"cpf": cpf_limpo},
    )

    doacoes = await db.execute(
        text("SELECT * FROM doacoes_campanha WHERE cpf_candidato = :cpf ORDER BY data_doacao DESC"),
        {"cpf": cpf_limpo},
    )

    return {
        "politico": dict(row._mapping),
        "relacoes_em_licitacoes": [dict(r._mapping) for r in relacoes.fetchall()],
        "doacoes_recebidas": [dict(r._mapping) for r in doacoes.fetchall()],
    }


@app.get("/api/v1/politicos")
async def listar_politicos(
    uf: Optional[str] = Query(None),
    cargo: Optional[str] = Query(None),
    nome: Optional[str] = Query(None),
    pagina: int = Query(1, ge=1),
    tamanho: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    filters = []
    params = {}
    if uf:
        filters.append("p.uf = :uf")
        params["uf"] = uf.upper()
    if cargo:
        filters.append("p.cargo ILIKE :cargo")
        params["cargo"] = f"%{cargo}%"
    if nome:
        filters.append("p.nome ILIKE :nome")
        params["nome"] = f"%{nome}%"

    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    params["offset"] = (pagina - 1) * tamanho
    params["limit"] = tamanho

    result = await db.execute(
        text(f"""
            SELECT
                p.cpf, p.nome, p.partido, p.cargo, p.uf, p.municipio,
                p.eleito, p.patrimonio_declarado,
                COUNT(DISTINCT r.licitacao_id) AS total_licitacoes_suspeitas,
                AVG(l.score_risco) AS score_medio_licitacoes
            FROM politicos p
            LEFT JOIN relacoes_detectadas r ON r.cpf_politico = p.cpf
            LEFT JOIN licitacoes l ON l.id = r.licitacao_id
            {where}
            GROUP BY p.cpf, p.nome, p.partido, p.cargo, p.uf, p.municipio,
                     p.eleito, p.patrimonio_declarado
            ORDER BY total_licitacoes_suspeitas DESC, score_medio_licitacoes DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        """),
        params,
    )

    return {"dados": [dict(r._mapping) for r in result.fetchall()]}


# ============================================================
# MAPA DE RISCO - por município
# ============================================================

@app.get("/api/v1/mapa-risco")
async def mapa_risco(
    uf: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Retorna score médio e total por município para heatmap."""
    where = "WHERE orgao_uf = :uf" if uf else ""
    params = {"uf": uf.upper()} if uf else {}

    result = await db.execute(
        text(f"""
            SELECT
                orgao_codigo_ibge AS codigo_ibge,
                orgao_municipio AS municipio,
                orgao_uf AS uf,
                COUNT(*) AS total_licitacoes,
                COUNT(*) FILTER (WHERE score_risco >= 50) AS alto_risco,
                AVG(score_risco) AS score_medio,
                SUM(valor_homologado) AS valor_total
            FROM licitacoes
            {where}
            GROUP BY orgao_codigo_ibge, orgao_municipio, orgao_uf
            ORDER BY score_medio DESC
        """),
        params,
    )

    return {"dados": [dict(r._mapping) for r in result.fetchall()]}


# ============================================================
# COLETA MANUAL (trigger via API)
# ============================================================

@app.post("/api/v1/coletar")
async def iniciar_coleta(
    req: ColettaRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
):
    """Dispara coleta em background. Retorna imediatamente."""

    async def _executar():
        pipeline = Pipeline(db)
        await pipeline.executar(
            uf=req.uf,
            codigo_ibge=req.codigo_ibge,
            dias_retroativos=req.dias_retroativos,
        )

    background_tasks.add_task(_executar)
    return {
        "status": "iniciado",
        "uf": req.uf,
        "codigo_ibge": req.codigo_ibge,
        "dias_retroativos": req.dias_retroativos,
    }


# ============================================================
# STATUS DA COLETA
# ============================================================

@app.get("/api/v1/coletas")
async def status_coletas(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("""
            SELECT * FROM coletas_log
            ORDER BY iniciado_em DESC
            LIMIT 20
        """)
    )
    return {"coletas": [dict(r._mapping) for r in result.fetchall()]}
