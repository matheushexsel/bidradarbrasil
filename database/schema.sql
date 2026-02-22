-- ============================================================
-- LICITACAO RADAR - SCHEMA CENTRAL
-- ============================================================
-- CPF/CNPJ são os elos que ligam tudo.
-- Estruturado para cruzamento eficiente entre licitações,
-- empresas, sócios e políticos.
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- busca fuzzy de nomes
CREATE EXTENSION IF NOT EXISTS unaccent;

-- Wrapper IMMUTABLE necessário para usar unaccent em índices GIN
CREATE OR REPLACE FUNCTION immutable_unaccent(text)
  RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS
$func$ SELECT unaccent($1) $func$;

-- ============================================================
-- 1. LICITAÇÕES
-- ============================================================

CREATE TABLE IF NOT EXISTS licitacoes (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fonte                   VARCHAR(50) NOT NULL,          -- 'PNCP', 'COMPRASNET', 'PORTAL_ESTADUAL', etc.
    numero_controle         VARCHAR(200) UNIQUE NOT NULL,  -- identificador único da fonte
    orgao_cnpj              VARCHAR(14),
    orgao_nome              VARCHAR(500),
    orgao_esfera            VARCHAR(20),                   -- 'FEDERAL', 'ESTADUAL', 'MUNICIPAL'
    orgao_uf                CHAR(2),
    orgao_municipio         VARCHAR(200),
    orgao_codigo_ibge       VARCHAR(10),
    modalidade              VARCHAR(100),
    tipo_objeto             VARCHAR(100),
    objeto_descricao        TEXT,
    objeto_categoria        VARCHAR(100),                  -- categoria normalizada pelo engine
    valor_estimado          NUMERIC(18,2),
    valor_homologado        NUMERIC(18,2),
    data_abertura           DATE,
    data_homologacao        DATE,
    situacao                VARCHAR(100),
    numero_participantes    INTEGER,
    -- flags de anomalia
    flag_preco_anomalo      BOOLEAN DEFAULT FALSE,
    flag_relacionamento     BOOLEAN DEFAULT FALSE,
    flag_objeto_inadequado  BOOLEAN DEFAULT FALSE,
    flag_participante_unico BOOLEAN DEFAULT FALSE,
    flag_prazo_suspeito     BOOLEAN DEFAULT FALSE,
    score_risco             NUMERIC(5,2) DEFAULT 0,        -- 0 a 100
    score_detalhes          JSONB,
    -- controle
    raw_data                JSONB,
    coletado_em             TIMESTAMP DEFAULT NOW(),
    atualizado_em           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_licitacoes_orgao_cnpj    ON licitacoes(orgao_cnpj);
CREATE INDEX idx_licitacoes_uf            ON licitacoes(orgao_uf);
CREATE INDEX idx_licitacoes_municipio     ON licitacoes(orgao_codigo_ibge);
CREATE INDEX idx_licitacoes_score         ON licitacoes(score_risco DESC);
CREATE INDEX idx_licitacoes_flags         ON licitacoes(flag_preco_anomalo, flag_relacionamento);
CREATE INDEX idx_licitacoes_data          ON licitacoes(data_abertura);
CREATE INDEX idx_licitacoes_objeto_trgm   ON licitacoes USING GIN (objeto_descricao gin_trgm_ops);

-- ============================================================
-- 2. VENCEDORES / PARTICIPANTES
-- ============================================================

CREATE TABLE IF NOT EXISTS licitacao_participantes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    licitacao_id    UUID REFERENCES licitacoes(id) ON DELETE CASCADE,
    cnpj            VARCHAR(14) NOT NULL,
    razao_social    VARCHAR(500),
    vencedor        BOOLEAN DEFAULT FALSE,
    valor_proposta  NUMERIC(18,2),
    desclassificado BOOLEAN DEFAULT FALSE,
    motivo_desclass TEXT,
    raw_data        JSONB
);

CREATE INDEX idx_participantes_licitacao ON licitacao_participantes(licitacao_id);
CREATE INDEX idx_participantes_cnpj      ON licitacao_participantes(cnpj);
CREATE INDEX idx_participantes_vencedor  ON licitacao_participantes(cnpj) WHERE vencedor = TRUE;

-- ============================================================
-- 3. EMPRESAS (CNPJ - Receita Federal)
-- ============================================================

CREATE TABLE IF NOT EXISTS empresas (
    cnpj                VARCHAR(14) PRIMARY KEY,
    razao_social        VARCHAR(500),
    nome_fantasia       VARCHAR(500),
    situacao_cadastral  VARCHAR(50),
    data_abertura       DATE,
    cnae_principal      VARCHAR(10),
    cnae_descricao      VARCHAR(500),
    natureza_juridica   VARCHAR(200),
    porte               VARCHAR(50),
    capital_social      NUMERIC(18,2),
    logradouro          VARCHAR(500),
    municipio           VARCHAR(200),
    uf                  CHAR(2),
    cep                 VARCHAR(8),
    -- flags
    flag_empresa_fantasma   BOOLEAN DEFAULT FALSE, -- abertura recente + sem funcionários
    flag_cnae_divergente    BOOLEAN DEFAULT FALSE, -- CNAE não condiz com objeto licitado
    flag_sanctionada        BOOLEAN DEFAULT FALSE, -- presente em CEIS/CNEP
    coletado_em             TIMESTAMP DEFAULT NOW(),
    atualizado_em           TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_empresas_uf     ON empresas(uf);
CREATE INDEX idx_empresas_cnae   ON empresas(cnae_principal);
CREATE INDEX idx_empresas_nome   ON empresas USING GIN (immutable_unaccent(razao_social) gin_trgm_ops);

-- ============================================================
-- 4. SÓCIOS (QSA - Receita Federal)
-- ============================================================

CREATE TABLE IF NOT EXISTS socios (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cnpj            VARCHAR(14) NOT NULL,
    cpf_cnpj_socio  VARCHAR(14),
    nome_socio      VARCHAR(500) NOT NULL,
    nome_normalizado VARCHAR(500),  -- sem acento, uppercase
    qualificacao    VARCHAR(100),
    data_entrada    DATE,
    FOREIGN KEY (cnpj) REFERENCES empresas(cnpj) ON DELETE CASCADE
);

CREATE INDEX idx_socios_cnpj         ON socios(cnpj);
CREATE INDEX idx_socios_cpf          ON socios(cpf_cnpj_socio);
CREATE INDEX idx_socios_nome         ON socios USING GIN (nome_normalizado gin_trgm_ops);

-- ============================================================
-- 5. POLÍTICOS (TSE)
-- ============================================================

CREATE TABLE IF NOT EXISTS politicos (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cpf                 VARCHAR(11) UNIQUE,
    nome                VARCHAR(500) NOT NULL,
    nome_normalizado    VARCHAR(500),  -- sem acento, uppercase
    nome_urna           VARCHAR(200),
    partido             VARCHAR(50),
    cargo               VARCHAR(100),
    uf                  CHAR(2),
    municipio           VARCHAR(200),
    codigo_ibge         VARCHAR(10),
    situacao_candidatura VARCHAR(100),
    eleito              BOOLEAN,
    ano_eleicao         INTEGER,
    -- patrimônio TSE
    patrimonio_declarado NUMERIC(18,2),
    -- controle
    coletado_em         TIMESTAMP DEFAULT NOW(),
    atualizado_em       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_politicos_cpf       ON politicos(cpf);
CREATE INDEX idx_politicos_uf        ON politicos(uf);
CREATE INDEX idx_politicos_municipio ON politicos(codigo_ibge);
CREATE INDEX idx_politicos_nome      ON politicos USING GIN (nome_normalizado gin_trgm_ops);

-- ============================================================
-- 6. RELAÇÕES DETECTADAS (resultado do engine)
-- ============================================================

CREATE TABLE IF NOT EXISTS relacoes_detectadas (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    licitacao_id    UUID REFERENCES licitacoes(id) ON DELETE CASCADE,
    tipo_relacao    VARCHAR(100) NOT NULL,
    -- lado A: empresa
    cnpj_empresa    VARCHAR(14),
    nome_empresa    VARCHAR(500),
    nome_socio      VARCHAR(500),
    -- lado B: político
    cpf_politico    VARCHAR(11),
    nome_politico   VARCHAR(500),
    cargo_politico  VARCHAR(100),
    uf_politico     CHAR(2),
    -- evidência
    tipo_vinculo    VARCHAR(100),  -- 'SOBRENOME_IDENTICO', 'CPF_DIRETO', 'FAMILIAR', 'DOACAO_CAMPANHA'
    similaridade    NUMERIC(5,4),  -- 0 a 1 para match de nome
    evidencia       TEXT,
    detectado_em    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_relacoes_licitacao  ON relacoes_detectadas(licitacao_id);
CREATE INDEX idx_relacoes_cnpj       ON relacoes_detectadas(cnpj_empresa);
CREATE INDEX idx_relacoes_politico   ON relacoes_detectadas(cpf_politico);

-- ============================================================
-- 7. PREÇOS DE REFERÊNCIA (benchmark de mercado)
-- ============================================================

CREATE TABLE IF NOT EXISTS precos_referencia (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    categoria_objeto    VARCHAR(100) NOT NULL,
    descricao_tipo      VARCHAR(500),
    unidade             VARCHAR(50),
    preco_mediana       NUMERIC(18,2),
    preco_p25           NUMERIC(18,2),
    preco_p75           NUMERIC(18,2),
    preco_p95           NUMERIC(18,2),
    total_amostras      INTEGER,
    periodo_referencia  VARCHAR(20),  -- 'YYYY-MM'
    fonte               VARCHAR(100), -- 'HISTORICO_PNCP', 'PAINEL_PRECOS', 'COMPRASNET'
    atualizado_em       TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_precos_categoria ON precos_referencia(categoria_objeto);

-- ============================================================
-- 8. SANÇÕES (CEIS/CNEP - CGU)
-- ============================================================

CREATE TABLE IF NOT EXISTS sancoes (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cnpj_cpf        VARCHAR(14) NOT NULL,
    nome_sancionado VARCHAR(500),
    tipo_pessoa     VARCHAR(10),  -- 'PJ' ou 'PF'
    tipo_sancao     VARCHAR(200),
    orgao_sancionador VARCHAR(500),
    data_inicio     DATE,
    data_fim        DATE,
    fundamentacao   TEXT,
    fonte           VARCHAR(50),  -- 'CEIS', 'CNEP', 'CEPIM'
    coletado_em     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_sancoes_cnpj_cpf ON sancoes(cnpj_cpf);

-- ============================================================
-- 9. DOAÇÕES DE CAMPANHA (TSE)
-- ============================================================

CREATE TABLE IF NOT EXISTS doacoes_campanha (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cpf_cnpj_doador     VARCHAR(14) NOT NULL,
    nome_doador         VARCHAR(500),
    cpf_candidato       VARCHAR(11),
    nome_candidato      VARCHAR(500),
    partido             VARCHAR(50),
    cargo               VARCHAR(100),
    uf                  CHAR(2),
    ano_eleicao         INTEGER,
    valor               NUMERIC(18,2),
    data_doacao         DATE,
    coletado_em         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_doacoes_doador    ON doacoes_campanha(cpf_cnpj_doador);
CREATE INDEX idx_doacoes_candidato ON doacoes_campanha(cpf_candidato);

-- ============================================================
-- 10. LOG DE COLETAS
-- ============================================================

CREATE TABLE IF NOT EXISTS coletas_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    fonte           VARCHAR(100) NOT NULL,
    escopo          VARCHAR(200),  -- UF, municipio, etc.
    status          VARCHAR(50),   -- 'SUCESSO', 'ERRO', 'PARCIAL'
    registros       INTEGER DEFAULT 0,
    erro_msg        TEXT,
    iniciado_em     TIMESTAMP DEFAULT NOW(),
    finalizado_em   TIMESTAMP
);

-- ============================================================
-- VIEW: ranking de risco consolidado
-- ============================================================

CREATE OR REPLACE VIEW vw_licitacoes_risco AS
SELECT
    l.id,
    l.orgao_nome,
    l.orgao_uf,
    l.orgao_municipio,
    l.orgao_esfera,
    l.objeto_descricao,
    l.objeto_categoria,
    l.valor_estimado,
    l.valor_homologado,
    l.data_abertura,
    l.score_risco,
    l.score_detalhes,
    l.flag_preco_anomalo,
    l.flag_relacionamento,
    l.flag_objeto_inadequado,
    l.flag_participante_unico,
    l.flag_prazo_suspeito,
    COUNT(DISTINCT r.id) AS total_relacoes,
    COUNT(DISTINCT p.cnpj) FILTER (WHERE p.vencedor) AS cnpj_vencedor,
    MAX(p.razao_social) FILTER (WHERE p.vencedor) AS empresa_vencedora
FROM licitacoes l
LEFT JOIN relacoes_detectadas r ON r.licitacao_id = l.id
LEFT JOIN licitacao_participantes p ON p.licitacao_id = l.id
GROUP BY l.id
ORDER BY l.score_risco DESC;

-- ============================================================
-- VIEW: empresas com mais vitórias suspeitas
-- ============================================================

CREATE OR REPLACE VIEW vw_empresas_suspeitas AS
SELECT
    e.cnpj,
    e.razao_social,
    e.uf,
    e.municipio,
    e.cnae_descricao,
    e.flag_empresa_fantasma,
    e.flag_cnae_divergente,
    e.flag_sanctionada,
    COUNT(DISTINCT p.licitacao_id) AS total_vitorias,
    SUM(p.valor_proposta) AS total_contratado,
    COUNT(DISTINCT r.id) AS total_relacoes_politicas,
    AVG(l.score_risco) AS score_risco_medio
FROM empresas e
JOIN licitacao_participantes p ON p.cnpj = e.cnpj AND p.vencedor = TRUE
JOIN licitacoes l ON l.id = p.licitacao_id
LEFT JOIN relacoes_detectadas r ON r.cnpj_empresa = e.cnpj
GROUP BY e.cnpj, e.razao_social, e.uf, e.municipio, e.cnae_descricao,
         e.flag_empresa_fantasma, e.flag_cnae_divergente, e.flag_sanctionada
HAVING COUNT(DISTINCT r.id) > 0 OR AVG(l.score_risco) > 50
ORDER BY score_risco_medio DESC;
