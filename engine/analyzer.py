"""
ENGINE DE ANÁLISE - Coração do sistema.

Três eixos de detecção:
1. PREÇO  → valor está fora do padrão histórico/mercado?
2. RELAÇÃO → empresa tem vínculo com político da região?
3. OBJETO  → CNAE da empresa condiz com o objeto licitado?

Cada anomalia gera um score parcial. Score final = soma ponderada.
"""

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional
from loguru import logger


# ============================================================
# SCORES (pesos de cada anomalia no score final)
# ============================================================

SCORES = {
    "preco_acima_3x":          40,
    "preco_acima_2x":          25,
    "preco_acima_1_5x":        15,
    "participante_unico":      20,
    "empresa_aberta_recente":  20,
    "empresa_sanctionada":     35,
    "cnae_divergente":         25,
    "sobrenome_identico":      35,
    "cpf_direto":              50,
    "doador_campanha":         30,
    "prazo_edital_curto":      15,   # edital publicado < 3 dias antes do prazo
    "objeto_generico":         10,   # descrição vaga demais
}

# ============================================================
# MAPEAMENTO CNAE → CATEGORIAS DE OBJETO
# ============================================================

# key = categoria normalizada | value = lista de prefixos CNAE válidos
CNAE_POR_CATEGORIA = {
    "OBRAS_CONSTRUCAO": ["41", "42", "43"],
    "TECNOLOGIA_TI": ["62", "63"],
    "SAUDE_EQUIPAMENTOS": ["32.5", "46.4", "47.4"],
    "SAUDE_SERVICOS": ["86", "87", "88"],
    "ALIMENTACAO": ["10", "11", "56", "47.2"],
    "LIMPEZA_CONSERVACAO": ["81.2"],
    "SEGURANCA": ["80.1"],
    "CONSULTORIA": ["69", "70", "71", "73", "74"],
    "TRANSPORTE": ["49", "50", "51"],
    "EDUCACAO": ["85"],
    "COMBUSTIVEL": ["47.3", "46.8"],
    "MOBILIARIO": ["31", "46.5"],
    "MEDICAMENTOS": ["21", "46.4"],
    "PUBLICIDADE": ["73.1"],
    "EVENTOS": ["82.3", "90", "91"],
}

# Palavras-chave no objeto → categoria
PALAVRAS_CATEGORIA = {
    "OBRAS_CONSTRUCAO": [
        "obra", "construção", "reforma", "pavimentação", "infraestrutura",
        "edificação", "engenharia", "calçada", "drenagem", "saneamento",
    ],
    "TECNOLOGIA_TI": [
        "software", "sistema", "tecnologia", "ti", "hardware", "computador",
        "servidor", "rede", "informática", "licença", "suporte técnico",
    ],
    "SAUDE_EQUIPAMENTOS": [
        "equipamento médico", "hospitalar", "odontológico", "ambulância",
        "leito", "uti", "laboratório",
    ],
    "SAUDE_SERVICOS": [
        "saúde", "médico", "enfermagem", "upa", "hospital", "sus",
        "consulta", "exame", "cirurgia",
    ],
    "ALIMENTACAO": [
        "alimento", "merenda", "refeição", "gênero alimentício", "cesta",
        "alimentação escolar", "lanche",
    ],
    "LIMPEZA_CONSERVACAO": [
        "limpeza", "conservação", "zeladoria", "higienização", "coleta",
    ],
    "SEGURANCA": [
        "segurança", "vigilância", "monitoramento", "câmera",
    ],
    "CONSULTORIA": [
        "consultoria", "assessoria", "elaboração de plano", "planejamento",
        "diagnóstico", "auditoria", "contabilidade", "advocacia",
    ],
    "TRANSPORTE": [
        "transporte", "veículo", "ônibus", "frete", "logística", "ambulância",
    ],
    "EDUCACAO": [
        "educação", "ensino", "escola", "material didático", "capacitação",
        "treinamento", "curso",
    ],
    "COMBUSTIVEL": [
        "combustível", "gasolina", "diesel", "álcool", "etanol", "lubrificante",
    ],
    "MOBILIARIO": [
        "mobiliário", "móvel", "cadeira", "mesa", "armário", "estante",
    ],
    "MEDICAMENTOS": [
        "medicamento", "remédio", "farmácia", "insumo farmacêutico",
    ],
    "PUBLICIDADE": [
        "publicidade", "propaganda", "comunicação", "mídia", "jornal",
        "anúncio", "divulgação",
    ],
    "EVENTOS": [
        "evento", "cerimônia", "show", "espetáculo", "festa", "solenidade",
    ],
}


# ============================================================
# DATACLASSES DE RESULTADO
# ============================================================

@dataclass
class AnomaliaPreco:
    valor_licitacao: float
    valor_referencia: float
    razao: float           # valor_licitacao / valor_referencia
    percentil: float       # posição histórica
    score: float
    descricao: str


@dataclass
class AnomaliaRelacao:
    tipo: str              # 'SOBRENOME', 'CPF_DIRETO', 'DOACAO'
    nome_socio: str
    nome_politico: str
    cargo_politico: str
    uf_politico: str
    similaridade: float
    evidencia: str
    score: float


@dataclass
class AnomaliaObjeto:
    cnae_empresa: str
    cnae_descricao: str
    categoria_objeto: str
    compativel: bool
    score: float
    descricao: str


@dataclass
class ResultadoAnalise:
    licitacao_id: str
    score_total: float
    score_detalhes: dict
    anomalias_preco: list[AnomaliaPreco] = field(default_factory=list)
    anomalias_relacao: list[AnomaliaRelacao] = field(default_factory=list)
    anomalias_objeto: list[AnomaliaObjeto] = field(default_factory=list)
    flags: dict = field(default_factory=dict)


# ============================================================
# ENGINE
# ============================================================

class AnalysisEngine:

    def normalizar_texto(self, texto: str) -> str:
        if not texto:
            return ""
        nfkd = unicodedata.normalize("NFKD", texto.upper().strip())
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    # ----------------------------------------------------------
    # 1. ANÁLISE DE PREÇO
    # ----------------------------------------------------------

    def analisar_preco(
        self,
        valor: float,
        historico: list[float],
        categoria: str = None,
    ) -> Optional[AnomaliaPreco]:
        """
        Compara valor com histórico de licitações similares.
        Se não há histórico, usa heurística por categoria.
        """
        if not valor or valor <= 0:
            return None

        if len(historico) < 5:
            return None  # amostra insuficiente

        import statistics
        mediana = statistics.median(historico)
        if mediana <= 0:
            return None

        razao = valor / mediana

        # percentil simples
        percentil = sum(1 for h in historico if h <= valor) / len(historico) * 100

        score = 0
        if razao >= 3:
            score = SCORES["preco_acima_3x"]
            descricao = f"Valor {razao:.1f}x acima da mediana histórica (R$ {mediana:,.2f})"
        elif razao >= 2:
            score = SCORES["preco_acima_2x"]
            descricao = f"Valor {razao:.1f}x acima da mediana histórica (R$ {mediana:,.2f})"
        elif razao >= 1.5:
            score = SCORES["preco_acima_1_5x"]
            descricao = f"Valor {razao:.1f}x acima da mediana histórica (R$ {mediana:,.2f})"
        else:
            return None

        return AnomaliaPreco(
            valor_licitacao=valor,
            valor_referencia=mediana,
            razao=razao,
            percentil=percentil,
            score=score,
            descricao=descricao,
        )

    def analisar_preco_painel_precos(
        self,
        valor: float,
        preco_referencia: dict,
    ) -> Optional[AnomaliaPreco]:
        """
        Usa dados do Painel de Preços do governo federal como referência.
        preco_referencia = {"mediana": X, "p75": Y, "p95": Z, "amostras": N}
        """
        if not valor or not preco_referencia:
            return None

        mediana = preco_referencia.get("preco_mediana", 0)
        p95 = preco_referencia.get("preco_p95", 0)
        if not mediana or mediana <= 0:
            return None

        razao = valor / mediana

        score = 0
        if razao >= 3:
            score = SCORES["preco_acima_3x"]
        elif razao >= 2:
            score = SCORES["preco_acima_2x"]
        elif razao >= 1.5:
            score = SCORES["preco_acima_1_5x"]
        else:
            return None

        return AnomaliaPreco(
            valor_licitacao=valor,
            valor_referencia=mediana,
            razao=razao,
            percentil=95 if valor > (p95 or mediana * 2) else 80,
            score=score,
            descricao=(
                f"Preço {razao:.1f}x acima da mediana do Painel de Preços federal "
                f"(ref: R$ {mediana:,.2f}, N={preco_referencia.get('total_amostras', '?')})"
            ),
        )

    # ----------------------------------------------------------
    # 2. ANÁLISE DE RELACIONAMENTO
    # ----------------------------------------------------------

    def _extrair_sobrenomes(self, nome: str) -> list[str]:
        """Extrai sobrenomes relevantes (ignora preposições)."""
        ignorar = {"DE", "DA", "DO", "DAS", "DOS", "E", "EM", "NA", "NO"}
        partes = self.normalizar_texto(nome).split()
        return [p for p in partes[1:] if p not in ignorar and len(p) > 2]

    def _similaridade_nomes(self, nome1: str, nome2: str) -> float:
        """
        Calcula similaridade de sobrenomes.
        Retorna 1.0 se compartilham sobrenome idêntico.
        """
        sobs1 = set(self._extrair_sobrenomes(nome1))
        sobs2 = set(self._extrair_sobrenomes(nome2))

        if not sobs1 or not sobs2:
            return 0.0

        intersecao = sobs1 & sobs2
        if not intersecao:
            return 0.0

        # Jaccard
        return len(intersecao) / len(sobs1 | sobs2)

    def analisar_relacionamentos(
        self,
        socios: list[dict],          # [{nome_socio, cpf_cnpj_socio, cnpj}, ...]
        politicos: list[dict],        # [{nome, cpf, cargo, uf, municipio}, ...]
        doacoes: list[dict] = None,   # [{cpf_cnpj_doador, cnpj_empresa, ...}, ...]
        uf_licitacao: str = None,
        municipio_licitacao: str = None,
    ) -> list[AnomaliaRelacao]:

        anomalias = []
        doacoes = doacoes or []

        # índice de doadores por CNPJ
        cnpjs_doadores = {
            d["cpf_cnpj_doador"]: d
            for d in doacoes
            if d.get("cpf_cnpj_doador")
        }

        for socio in socios:
            nome_socio = socio.get("nome_socio", "")
            cpf_socio = "".join(filter(str.isdigit, socio.get("cpf_cnpj_socio", "") or ""))
            cnpj_empresa = socio.get("cnpj", "")

            for politico in politicos:
                cpf_politico = politico.get("cpf", "")
                nome_politico = politico.get("nome", "")
                uf_pol = politico.get("uf", "")
                municipio_pol = politico.get("municipio", "")

                # Filtro geográfico: só alerta se político é da mesma UF ou município
                if uf_licitacao and uf_pol and uf_pol != uf_licitacao:
                    continue

                # --- MATCH CPF DIRETO ---
                if cpf_socio and cpf_politico and cpf_socio == cpf_politico:
                    anomalias.append(AnomaliaRelacao(
                        tipo="CPF_DIRETO",
                        nome_socio=nome_socio,
                        nome_politico=nome_politico,
                        cargo_politico=politico.get("cargo", ""),
                        uf_politico=uf_pol,
                        similaridade=1.0,
                        evidencia=f"Sócio da empresa é o próprio político (CPF {cpf_politico[:3]}***{cpf_politico[-2:]})",
                        score=SCORES["cpf_direto"],
                    ))
                    continue

                # --- MATCH SOBRENOME ---
                sim = self._similaridade_nomes(nome_socio, nome_politico)
                if sim >= 0.4:
                    tipo = "SOBRENOME_IDENTICO" if sim >= 0.7 else "SOBRENOME_SIMILAR"
                    anomalias.append(AnomaliaRelacao(
                        tipo=tipo,
                        nome_socio=nome_socio,
                        nome_politico=nome_politico,
                        cargo_politico=politico.get("cargo", ""),
                        uf_politico=uf_pol,
                        similaridade=sim,
                        evidencia=(
                            f"Sócio '{nome_socio}' compartilha sobrenome com "
                            f"{politico.get('cargo','')} '{nome_politico}' ({uf_pol})"
                        ),
                        score=SCORES["sobrenome_identico"] * sim,
                    ))

            # --- MATCH DOAÇÃO DE CAMPANHA ---
            if cnpj_empresa in cnpjs_doadores:
                doacao = cnpjs_doadores[cnpj_empresa]
                politico_doado = next(
                    (p for p in politicos if p.get("cpf") == doacao.get("cpf_candidato")),
                    None,
                )
                if politico_doado:
                    anomalias.append(AnomaliaRelacao(
                        tipo="DOADOR_CAMPANHA",
                        nome_socio=socio.get("nome_socio", ""),
                        nome_politico=politico_doado.get("nome", doacao.get("nome_candidato", "")),
                        cargo_politico=politico_doado.get("cargo", ""),
                        uf_politico=politico_doado.get("uf", ""),
                        similaridade=1.0,
                        evidencia=(
                            f"Empresa doou R$ {doacao.get('valor', 0):,.2f} para campanha de "
                            f"'{doacao.get('nome_candidato')}' em {doacao.get('ano_eleicao')}"
                        ),
                        score=SCORES["doador_campanha"],
                    ))

        # remove duplicatas mantendo maior score
        vistos = {}
        for a in anomalias:
            chave = (a.nome_politico, a.tipo)
            if chave not in vistos or vistos[chave].score < a.score:
                vistos[chave] = a

        return list(vistos.values())

    # ----------------------------------------------------------
    # 3. ANÁLISE DE OBJETO vs. CNAE
    # ----------------------------------------------------------

    def categorizar_objeto(self, descricao_objeto: str) -> str:
        """Infere categoria do objeto pela descrição textual."""
        descricao_norm = self.normalizar_texto(descricao_objeto)

        melhor_cat = "OUTROS"
        melhor_score = 0

        for categoria, palavras in PALAVRAS_CATEGORIA.items():
            hits = sum(
                1 for p in palavras
                if self.normalizar_texto(p) in descricao_norm
            )
            if hits > melhor_score:
                melhor_score = hits
                melhor_cat = categoria

        return melhor_cat

    def analisar_objeto_cnae(
        self,
        cnae_empresa: str,
        cnae_descricao: str,
        objeto_descricao: str,
        categoria_objeto: str = None,
    ) -> AnomaliaObjeto:
        """Verifica se o CNAE da empresa é compatível com o objeto licitado."""
        if not categoria_objeto:
            categoria_objeto = self.categorizar_objeto(objeto_descricao)

        cnaes_validos = CNAE_POR_CATEGORIA.get(categoria_objeto, [])

        compativel = True
        score = 0

        if cnaes_validos and cnae_empresa:
            cnae_norm = cnae_empresa.replace(".", "").replace("-", "")[:4]
            compativel = any(
                cnae_norm.startswith(c.replace(".", "").replace("-", ""))
                for c in cnaes_validos
            )
            if not compativel:
                score = SCORES["cnae_divergente"]

        descricao = (
            f"CNAE {cnae_empresa} ({cnae_descricao}) não é compatível "
            f"com objeto da categoria '{categoria_objeto}'"
            if not compativel else "CNAE compatível com objeto"
        )

        return AnomaliaObjeto(
            cnae_empresa=cnae_empresa or "",
            cnae_descricao=cnae_descricao or "",
            categoria_objeto=categoria_objeto,
            compativel=compativel,
            score=score,
            descricao=descricao,
        )

    # ----------------------------------------------------------
    # 4. ANÁLISE DE PRAZO
    # ----------------------------------------------------------

    def analisar_prazo(self, data_publicacao, data_abertura) -> float:
        """Edital com prazo muito curto pode ser direcionado."""
        from datetime import datetime, date

        if not data_publicacao or not data_abertura:
            return 0.0

        def to_date(d):
            if isinstance(d, date):
                return d
            for fmt in ["%Y-%m-%d", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(d, fmt).date()
                except (ValueError, TypeError):
                    pass
            return None

        pub = to_date(data_publicacao)
        abert = to_date(data_abertura)

        if not pub or not abert:
            return 0.0

        dias = (abert - pub).days
        if dias < 3:
            return SCORES["prazo_edital_curto"]
        return 0.0

    # ----------------------------------------------------------
    # 5. ANÁLISE CONSOLIDADA
    # ----------------------------------------------------------

    def analisar_licitacao(
        self,
        licitacao: dict,
        participantes: list[dict],       # vencedor incluso
        empresas: dict[str, dict],       # cnpj → dados empresa
        socios_por_cnpj: dict[str, list],  # cnpj → lista sócios
        politicos: list[dict],
        historico_precos: list[float] = None,
        preco_referencia: dict = None,
        doacoes: list[dict] = None,
        sancoes_por_cnpj: dict[str, dict] = None,
    ) -> ResultadoAnalise:

        licitacao_id = licitacao.get("id", "")
        valor = licitacao.get("valor_homologado") or licitacao.get("valor_estimado") or 0
        score_total = 0
        score_detalhes = {}
        todas_relacoes = []
        todas_anomalias_objeto = []

        # --- PREÇO ---
        anomalia_preco = None
        if preco_referencia:
            anomalia_preco = self.analisar_preco_painel_precos(valor, preco_referencia)
        elif historico_precos:
            anomalia_preco = self.analisar_preco(valor, historico_precos)

        if anomalia_preco:
            score_total += anomalia_preco.score
            score_detalhes["preco"] = {
                "score": anomalia_preco.score,
                "descricao": anomalia_preco.descricao,
                "razao": anomalia_preco.razao,
            }

        # --- PARTICIPANTE ÚNICO ---
        num_participantes = licitacao.get("numero_participantes") or len(participantes)
        score_participante = 0
        if num_participantes == 1:
            score_participante = SCORES["participante_unico"]
            score_total += score_participante
            score_detalhes["participante_unico"] = {
                "score": score_participante,
                "descricao": "Apenas 1 participante na licitação",
            }

        # --- PRAZO ---
        score_prazo = self.analisar_prazo(
            licitacao.get("data_publicacao") or licitacao.get("data_abertura"),
            licitacao.get("data_abertura"),
        )
        if score_prazo:
            score_total += score_prazo
            score_detalhes["prazo"] = {
                "score": score_prazo,
                "descricao": "Prazo entre publicação e abertura menor que 3 dias",
            }

        # --- POR CADA VENCEDOR / PARTICIPANTE ---
        categoria_objeto = self.categorizar_objeto(licitacao.get("objeto_descricao", ""))

        for participante in participantes:
            cnpj = participante.get("cnpj", "")
            if not cnpj:
                continue

            empresa = empresas.get(cnpj, {})
            socios = socios_por_cnpj.get(cnpj, [])
            sancao = (sancoes_por_cnpj or {}).get(cnpj, {})

            # --- EMPRESA FANTASMA ---
            from collectors.cnpj import CNPJCollector
            col = CNPJCollector()
            if col.detectar_empresa_fantasma(empresa):
                score_total += SCORES["empresa_aberta_recente"]
                score_detalhes.setdefault("empresa_suspeita", {
                    "score": SCORES["empresa_aberta_recente"],
                    "cnpj": cnpj,
                    "descricao": "Empresa aberta recentemente com capital social baixo",
                })

            # --- SANÇÃO ---
            if sancao.get("sanctionado"):
                score_total += SCORES["empresa_sanctionada"]
                score_detalhes.setdefault("sancao", {
                    "score": SCORES["empresa_sanctionada"],
                    "cnpj": cnpj,
                    "descricao": f"Empresa presente em lista de sanções (CEIS/CNEP)",
                })

            # --- CNAE vs. OBJETO ---
            if participante.get("vencedor") and empresa:
                anomalia_obj = self.analisar_objeto_cnae(
                    cnae_empresa=empresa.get("cnae_principal", ""),
                    cnae_descricao=empresa.get("cnae_descricao", ""),
                    objeto_descricao=licitacao.get("objeto_descricao", ""),
                    categoria_objeto=categoria_objeto,
                )
                todas_anomalias_objeto.append(anomalia_obj)
                if not anomalia_obj.compativel:
                    score_total += anomalia_obj.score
                    score_detalhes["cnae_divergente"] = {
                        "score": anomalia_obj.score,
                        "descricao": anomalia_obj.descricao,
                    }

            # --- RELACIONAMENTO ---
            if socios:
                relacoes = self.analisar_relacionamentos(
                    socios=socios,
                    politicos=politicos,
                    doacoes=doacoes or [],
                    uf_licitacao=licitacao.get("orgao_uf"),
                )
                todas_relacoes.extend(relacoes)

                if relacoes:
                    score_relacao = min(sum(r.score for r in relacoes), 50)
                    score_total += score_relacao
                    score_detalhes["relacionamento"] = {
                        "score": score_relacao,
                        "total_relacoes": len(relacoes),
                        "descricao": f"{len(relacoes)} vínculo(s) detectado(s) com políticos",
                    }

        # normaliza score em 0-100
        score_total = min(round(score_total, 2), 100.0)

        return ResultadoAnalise(
            licitacao_id=licitacao_id,
            score_total=score_total,
            score_detalhes=score_detalhes,
            anomalias_preco=[anomalia_preco] if anomalia_preco else [],
            anomalias_relacao=todas_relacoes,
            anomalias_objeto=todas_anomalias_objeto,
            flags={
                "flag_preco_anomalo": anomalia_preco is not None,
                "flag_relacionamento": len(todas_relacoes) > 0,
                "flag_objeto_inadequado": any(not a.compativel for a in todas_anomalias_objeto),
                "flag_participante_unico": num_participantes == 1,
                "flag_prazo_suspeito": score_prazo > 0,
            },
        )
