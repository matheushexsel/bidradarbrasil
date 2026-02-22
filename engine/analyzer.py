"""
ENGINE DE ANÁLISE
Classifica objetos, detecta anomalias e calcula scores de risco.
"""

import unicodedata
from dataclasses import dataclass, field
from statistics import median
from loguru import logger


# ----------------------------------------------------------
# SCORES (pontos por anomalia)
# ----------------------------------------------------------
SCORES = {
    # Preço
    "preco_acima_1_5x": 15,
    "preco_acima_2x":   25,
    "preco_acima_3x":   40,
    # Relacionamento político
    "sobrenome_coincide":    35,
    "cpf_direto":            50,
    "doacao_campanha":       30,
    # Empresa
    "empresa_sancionada":    35,
    "cnae_incompativel":     25,
    # Prazo — desabilitado pois PNCP não fornece data_publicacao separada
    # "prazo_edital_curto": 15,
}

# ----------------------------------------------------------
# CATEGORIAS DE OBJETO
# Ordem importa: mais específico primeiro
# ----------------------------------------------------------
CATEGORIAS = {
    "SAUDE": [
        "medicamento", "farmacia", "remedio", "vacina", "insumo farmaceutico",
        "material hospitalar", "material medico", "equipamento medico",
        "equipamento hospitalar", "ambulancia", "odontologico", "odontologia",
        "dental", "protese", "ortopedico", "laboratorial", "laboratorio",
        "saneamento", "saude", "enfermagem", "cirurgico", "hospitalar",
        "material de saude", "produto de saude", "kit medico", "kit hospitalar",
        "leito", "upa", "sus", "ubs", "vigilancia sanitaria",
        "exame", "consulta medica", "internacao", "tratamento medico",
        "residuos de saude", "epi", "equipamento de protecao",
        "material odontologico", "insumo odontologico", "equipamento odontologico",
    ],
    "OBRAS": [
        "obra", "construcao", "reforma", "ampliacao", "pavimentacao",
        "asfalto", "calcamento", "drenagem", "esgoto", "saneamento basico",
        "engenharia", "infraestrutura", "ponte", "viaduto", "galeria",
        "alvenaria", "fundacao", "estrutura", "cobertura", "telhado",
        "pintura predial", "revitalizacao", "restauracao predial",
        "piso", "revestimento", "demolição", "terraplenagem",
    ],
    "SERVICOS": [
        "servico", "manutencao", "limpeza", "conservacao", "vigilancia",
        "seguranca", "portaria", "recepcionista", "motorista", "transporte",
        "jardinagem", "paisagismo", "dedetizacao", "desinsetizacao",
        "lavanderia", "cozinheiro", "copeiro", "garçom", "auxiliar",
        "assistencia tecnica", "suporte tecnico", "helpdesk",
        "locacao de mao de obra", "terceirizacao",
        "coleta de lixo", "gestao de residuos",
        "prestacao de servicos", "fornecimento de mao de obra",
    ],
    "TECNOLOGIA_TI": [
        "software", "hardware", "sistema de informacao", "tecnologia da informacao",
        "ti ", " ti,", "computador", "notebook", "servidor", "rede",
        "infraestrutura de ti", "datacenter", "licenca de software",
        "sistema informatizado", "suporte de ti", "desenvolvimento de sistema",
        "aplicativo", "aplicacao web", "portal web", "site",
        "telecomunicacao", "internet", "fibra optica", "link de internet",
        "impressora", "scanner", "nobreak", "storage", "backup",
        "seguranca da informacao", "firewall",
    ],
    "ALIMENTACAO": [
        "alimentacao", "alimento", "merenda", "genero alimenticio",
        "produto alimenticio", "refeicao", "cesta basica", "kit alimentar",
        "gênero alimenticio", "mantimento", "hortifruti", "fruta", "verdura",
        "legume", "carne", "frango", "peixe", "laticinio", "leite",
        "pao", "padaria", "suco", "agua mineral", "cafe", "açucar",
    ],
    "EDUCACAO": [
        "educacao", "ensino", "escola", "professor", "material escolar",
        "material didatico", "livro didatico", "uniforme escolar",
        "mochila escolar", "capacitacao", "treinamento", "curso",
        "formacao", "qualificacao profissional", "bolsa de estudo",
        "creche", "pre-escola", "educacao infantil", "alfabetizacao",
    ],
    "VEICULOS": [
        "veiculo", "onibus", "caminhao", "van", "carro", "automovel",
        "moto", "motocicleta", "frota", "combustivel", "gasolina",
        "diesel", "etanol", "peca automotiva", "pneu", "borracha",
        "locacao de veiculo", "aluguel de veiculo",
    ],
    "MOBILIARIO": [
        "mobiliario", "movel", "cadeira", "mesa", "armario", "estante",
        "arquivo", "gaveta", "sofá", "poltrona", "banco",
        "equipamento de escritorio", "material de escritorio",
    ],
    "LICITACAO_PUBLICA": [
        # Fallback genérico para contratos governamentais sem categoria clara
    ],
}

# Mapeamento CNAE → categoria compatível
# Chave: prefixo CNAE (4 dígitos), valor: lista de categorias compatíveis
CNAE_CATEGORIAS = {
    # Saúde
    "4644": ["SAUDE"],
    "4645": ["SAUDE"],
    "4646": ["SAUDE"],
    "4771": ["SAUDE", "FARMACIA"],
    "8610": ["SAUDE"],
    "8621": ["SAUDE"],
    "8622": ["SAUDE"],
    "8630": ["SAUDE"],
    "8640": ["SAUDE"],
    "8650": ["SAUDE"],
    "8660": ["SAUDE"],
    # Construção/Obras
    "4120": ["OBRAS"],
    "4211": ["OBRAS"],
    "4212": ["OBRAS"],
    "4213": ["OBRAS"],
    "4221": ["OBRAS"],
    "4222": ["OBRAS"],
    "4291": ["OBRAS"],
    "4292": ["OBRAS"],
    "4299": ["OBRAS"],
    "4311": ["OBRAS"],
    "4312": ["OBRAS"],
    "4313": ["OBRAS"],
    "4319": ["OBRAS"],
    "4321": ["OBRAS"],
    "4322": ["OBRAS"],
    "4329": ["OBRAS"],
    "4330": ["OBRAS"],
    "4391": ["OBRAS"],
    "4399": ["OBRAS"],
    # Serviços
    "8111": ["SERVICOS"],
    "8112": ["SERVICOS"],
    "8121": ["SERVICOS"],
    "8122": ["SERVICOS"],
    "8129": ["SERVICOS"],
    "8011": ["SERVICOS"],
    "8012": ["SERVICOS"],
    "8020": ["SERVICOS"],
    # TI
    "6201": ["TECNOLOGIA_TI"],
    "6202": ["TECNOLOGIA_TI"],
    "6203": ["TECNOLOGIA_TI"],
    "6204": ["TECNOLOGIA_TI"],
    "6209": ["TECNOLOGIA_TI"],
    "6311": ["TECNOLOGIA_TI"],
    "6319": ["TECNOLOGIA_TI"],
    "6399": ["TECNOLOGIA_TI"],
    # Alimentação
    "4711": ["ALIMENTACAO"],
    "4712": ["ALIMENTACAO"],
    "4721": ["ALIMENTACAO"],
    "4722": ["ALIMENTACAO"],
    "4723": ["ALIMENTACAO"],
    "4724": ["ALIMENTACAO"],
    "4729": ["ALIMENTACAO"],
    # Educação
    "8511": ["EDUCACAO"],
    "8512": ["EDUCACAO"],
    "8513": ["EDUCACAO"],
    "8520": ["EDUCACAO"],
    "8531": ["EDUCACAO"],
    "8532": ["EDUCACAO"],
    "8541": ["EDUCACAO"],
    "8542": ["EDUCACAO"],
    "8550": ["EDUCACAO"],
    # Veículos
    "4511": ["VEICULOS"],
    "4512": ["VEICULOS"],
    "4530": ["VEICULOS"],
    "4541": ["VEICULOS"],
    "4542": ["VEICULOS"],
    "4543": ["VEICULOS"],
    "7711": ["VEICULOS"],
    "7719": ["VEICULOS"],
    # Atacadistas genéricos — compatíveis com SAUDE, ALIMENTACAO, MOBILIARIO
    "4647": ["SAUDE", "MOBILIARIO"],
    "4649": ["SAUDE", "MOBILIARIO", "SERVICOS"],
    "4789": ["SAUDE", "ALIMENTACAO", "MOBILIARIO", "EDUCACAO"],
    "4744": ["OBRAS", "MOBILIARIO"],
    "4742": ["OBRAS"],
    "4743": ["OBRAS"],
    # Comércio varejista genérico — amplo
    "4754": ["MOBILIARIO"],
    "4755": ["MOBILIARIO", "EDUCACAO"],
    "4761": ["EDUCACAO"],
    "4762": ["EDUCACAO"],
    "4763": ["EDUCACAO", "SERVICOS"],
    "4773": ["SAUDE"],
    # Transporte
    "4921": ["VEICULOS", "SERVICOS"],
    "4922": ["VEICULOS", "SERVICOS"],
    "4923": ["VEICULOS"],
    "4929": ["VEICULOS", "SERVICOS"],
    "4930": ["VEICULOS", "OBRAS"],
    # Serviços de organização de eventos
    "8230": ["SERVICOS", "EDUCACAO"],
    # Outros serviços — amplo para evitar falso positivo
    "9001": ["SERVICOS", "EDUCACAO"],
    "9002": ["SERVICOS"],
    "9003": ["SERVICOS"],
    "9101": ["SERVICOS"],
    "9102": ["SERVICOS"],
    "9103": ["SERVICOS"],
    "9200": ["SERVICOS"],
    "9311": ["SERVICOS"],
    "9312": ["SERVICOS"],
    "9313": ["SERVICOS"],
    "9319": ["SERVICOS"],
    "9321": ["SERVICOS"],
    "9329": ["SERVICOS"],
    "9430": ["SERVICOS", "EDUCACAO"],
    "9491": ["SERVICOS"],
    "9492": ["SERVICOS"],
    "9493": ["SERVICOS"],
    "9499": ["SERVICOS"],
    "9601": ["SERVICOS"],
    "9602": ["SERVICOS"],
    "9603": ["SERVICOS"],
    "9609": ["SERVICOS"],
}


@dataclass
class RelacaoDetectada:
    tipo: str
    nome_socio: str
    nome_politico: str
    cargo_politico: str
    uf_politico: str
    similaridade: float
    evidencia: str


@dataclass
class ResultadoAnalise:
    score_total: int = 0
    score_detalhes: dict = field(default_factory=dict)
    flags: dict = field(default_factory=dict)
    anomalias_relacao: list = field(default_factory=list)


class AnalysisEngine:

    def _normalizar_texto(self, texto: str) -> str:
        if not texto:
            return ""
        nfkd = unicodedata.normalize("NFKD", texto.upper())
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    def categorizar_objeto(self, descricao: str) -> str:
        """
        Classifica o objeto da licitação em uma categoria.
        Usa matching de palavras-chave normalizadas.
        Sem fallback para TECNOLOGIA_TI — usa LICITACAO_PUBLICA como genérico.
        """
        if not descricao:
            return "LICITACAO_PUBLICA"

        texto = self._normalizar_texto(descricao)

        # Pontuação por categoria — ganha a que tiver mais matches
        pontos: dict[str, int] = {cat: 0 for cat in CATEGORIAS}

        for categoria, keywords in CATEGORIAS.items():
            for kw in keywords:
                kw_norm = self._normalizar_texto(kw)
                if kw_norm and kw_norm in texto:
                    # Palavras mais específicas (mais longas) valem mais
                    pontos[categoria] += len(kw_norm.split())

        melhor = max(pontos, key=lambda c: pontos[c])
        if pontos[melhor] > 0:
            return melhor

        return "LICITACAO_PUBLICA"

    def _cnae_compativel(self, cnae: str, categoria: str) -> bool:
        """
        Verifica se o CNAE da empresa é compatível com a categoria do objeto.
        Retorna True se compatível ou se não há dados suficientes para decidir.
        """
        if not cnae or not categoria:
            return True
        if categoria == "LICITACAO_PUBLICA":
            # Categoria genérica — nunca sinalizar incompatibilidade
            return True

        cnae_str = str(cnae).replace(".", "").replace("-", "").replace("/", "")
        prefixo = cnae_str[:4]

        cats_compativeis = CNAE_CATEGORIAS.get(prefixo)
        if cats_compativeis is None:
            # CNAE não mapeado — não sinalizar para evitar falso positivo
            return True

        return categoria in cats_compativeis

    def analisar_preco(
        self, valor: float | None, historico: list[float]
    ) -> tuple[int, str]:
        """Retorna (score, descricao)."""
        if not valor or valor <= 0 or len(historico) < 5:
            return 0, ""
        med = median(historico)
        if med <= 0:
            return 0, ""
        ratio = valor / med
        if ratio >= 3:
            return SCORES["preco_acima_3x"], f"{ratio:.1f}x acima da mediana"
        if ratio >= 2:
            return SCORES["preco_acima_2x"], f"{ratio:.1f}x acima da mediana"
        if ratio >= 1.5:
            return SCORES["preco_acima_1_5x"], f"{ratio:.1f}x acima da mediana"
        return 0, ""

    def detectar_relacionamentos(
        self,
        socios: list[dict],
        politicos: list[dict],
        uf_licitacao: str,
        doacoes: list[dict],
        cnpj_empresa: str,
    ) -> list[RelacaoDetectada]:
        """
        Detecta vínculos entre sócios da empresa e políticos da UF.
        Tipos: CPF_DIRETO (50pts), SOBRENOME (35pts), DOACAO (30pts).
        """
        relacoes: list[RelacaoDetectada] = []

        if not socios or not politicos:
            return relacoes

        # Filtra políticos da mesma UF
        politicos_uf = [
            p for p in politicos
            if not uf_licitacao or p.get("uf", "").upper() == uf_licitacao.upper()
        ]

        # Índice de doações por CNPJ
        cnpjs_doadores = {
            d.get("cnpj_doador", ""): d for d in (doacoes or []) if d.get("cnpj_doador")
        }

        for socio in socios:
            cpf_socio = socio.get("cpf_cnpj_socio", "")
            nome_socio_norm = socio.get("nome_normalizado") or self._normalizar_texto(
                socio.get("nome_socio", "")
            )
            if not nome_socio_norm:
                continue

            sobrenome_socio = nome_socio_norm.split()[-1] if nome_socio_norm else ""
            # Filtra sobrenomes muito curtos ou comuns demais
            if len(sobrenome_socio) < 4:
                continue

            for pol in politicos_uf:
                nome_pol_norm = pol.get("nome_normalizado") or self._normalizar_texto(
                    pol.get("nome", "")
                )
                if not nome_pol_norm:
                    continue

                # 1. CPF direto
                cpf_pol = pol.get("cpf", "")
                if cpf_socio and cpf_pol and cpf_socio == cpf_pol:
                    relacoes.append(RelacaoDetectada(
                        tipo="CPF_DIRETO",
                        nome_socio=socio.get("nome_socio", ""),
                        nome_politico=pol.get("nome", ""),
                        cargo_politico=pol.get("cargo", ""),
                        uf_politico=pol.get("uf", ""),
                        similaridade=1.0,
                        evidencia=f"CPF {cpf_socio} coincide com político {pol.get('nome','')}",
                    ))
                    continue

                # 2. Sobrenome coincide
                sobrenome_pol = nome_pol_norm.split()[-1] if nome_pol_norm else ""
                if (
                    len(sobrenome_pol) >= 4
                    and sobrenome_socio == sobrenome_pol
                    # Evita sobrenomes ultra-comuns
                    and sobrenome_socio not in {
                        "SILVA", "SANTOS", "OLIVEIRA", "SOUZA", "LIMA",
                        "PEREIRA", "FERREIRA", "COSTA", "RODRIGUES", "ALMEIDA",
                        "NASCIMENTO", "LIMA", "ARAUJO", "FERNANDES", "CAVALCANTI",
                        "BARBOSA", "RIBEIRO", "MARTINS", "CARVALHO", "MENDES",
                    }
                ):
                    relacoes.append(RelacaoDetectada(
                        tipo="SOBRENOME",
                        nome_socio=socio.get("nome_socio", ""),
                        nome_politico=pol.get("nome", ""),
                        cargo_politico=pol.get("cargo", ""),
                        uf_politico=pol.get("uf", ""),
                        similaridade=0.8,
                        evidencia=(
                            f"Sobrenome '{sobrenome_socio}' coincide entre "
                            f"sócio e {pol.get('cargo','')} {pol.get('nome','')}"
                        ),
                    ))

        # 3. Doação de campanha
        if cnpj_empresa and cnpj_empresa in cnpjs_doadores:
            doacao = cnpjs_doadores[cnpj_empresa]
            relacoes.append(RelacaoDetectada(
                tipo="DOACAO_CAMPANHA",
                nome_socio="",
                nome_politico=doacao.get("nome_candidato", ""),
                cargo_politico=doacao.get("cargo_candidato", ""),
                uf_politico=doacao.get("uf", ""),
                similaridade=0.9,
                evidencia=(
                    f"Empresa CNPJ {cnpj_empresa} doou R$ "
                    f"{doacao.get('valor_doacao', 0):.0f} para campanha de "
                    f"{doacao.get('nome_candidato', '')}"
                ),
            ))

        return relacoes

    def analisar_licitacao(
        self,
        licitacao: dict,
        participantes: list[dict],
        empresas: dict,
        socios_por_cnpj: dict,
        politicos: list[dict],
        historico_precos: list[float],
        doacoes: list[dict],
        sancoes_por_cnpj: dict,
    ) -> ResultadoAnalise:

        resultado = ResultadoAnalise()
        score = 0
        detalhes: dict = {}
        flags: dict = {}
        anomalias: list[RelacaoDetectada] = []

        # Categoria do objeto — sempre salva
        categoria = self.categorizar_objeto(licitacao.get("objeto_descricao", ""))
        detalhes["categoria_objeto"] = categoria

        valor = licitacao.get("valor_homologado") or licitacao.get("valor_estimado")
        uf = licitacao.get("orgao_uf", "")

        # ── Preço anômalo ─────────────────────────────────────
        score_preco, desc_preco = self.analisar_preco(valor, historico_precos)
        if score_preco > 0:
            score += score_preco
            detalhes["preco_anomalo"] = desc_preco
            flags["flag_preco_anomalo"] = True
        else:
            flags["flag_preco_anomalo"] = False

        # ── Análise por empresa ────────────────────────────────
        flag_objeto_inadequado = False
        flag_sancionada = False
        flag_relacionamento = False

        for p in participantes:
            cnpj = p.get("cnpj", "")
            if not cnpj:
                continue

            empresa = empresas.get(cnpj, {})
            socios = socios_por_cnpj.get(cnpj, [])
            sancao = sancoes_por_cnpj.get(cnpj)

            # CNAE incompatível com objeto
            cnae = empresa.get("cnae_principal", "")
            cnae_desc = empresa.get("cnae_descricao", "")
            if cnae and not self._cnae_compativel(cnae, categoria):
                score += SCORES["cnae_incompativel"]
                detalhes["cnae_divergente"] = (
                    f"CNAE {cnae} ({cnae_desc}) não é compatível com "
                    f"objeto da categoria '{categoria}'"
                )
                flag_objeto_inadequado = True

            # Empresa sancionada (CGU)
            if sancao and sancao.get("sancionada"):
                score += SCORES["empresa_sancionada"]
                detalhes["empresa_sancionada"] = (
                    f"Empresa {cnpj} consta em {sancao.get('lista', 'lista CGU')}"
                )
                flag_sancionada = True

            # Relacionamentos políticos
            rels = self.detectar_relacionamentos(
                socios=socios,
                politicos=politicos,
                uf_licitacao=uf,
                doacoes=doacoes,
                cnpj_empresa=cnpj,
            )
            for rel in rels:
                if rel.tipo == "CPF_DIRETO":
                    score += SCORES["cpf_direto"]
                elif rel.tipo == "SOBRENOME":
                    score += SCORES["sobrenome_coincide"]
                elif rel.tipo == "DOACAO_CAMPANHA":
                    score += SCORES["doacao_campanha"]
                anomalias.append(rel)
                flag_relacionamento = True

        flags["flag_objeto_inadequado"] = flag_objeto_inadequado
        flags["flag_relacionamento"] = flag_relacionamento
        flags["flag_empresa_sancionada"] = flag_sancionada
        # flag_participante_unico removido — não temos dados confiáveis
        flags["flag_participante_unico"] = False
        # flag_prazo_suspeito desabilitado — PNCP não fornece data_publicacao
        flags["flag_prazo_suspeito"] = False

        resultado.score_total = min(score, 100)
        resultado.score_detalhes = detalhes
        resultado.flags = flags
        resultado.anomalias_relacao = anomalias

        return resultado
