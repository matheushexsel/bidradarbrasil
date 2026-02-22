# 🔍 Licitação Radar

Sistema de detecção de anomalias em licitações públicas brasileiras.
Cobre federal, estadual e municipal via PNCP + TSE + CNPJ/QSA + CGU.

---

## Como funciona

Três eixos de análise, cada um gerando um score parcial (0–100):

| Eixo | O que detecta | Score máx |
|------|--------------|-----------|
| **Preço** | Valor 1.5x–3x+ acima da mediana histórica ou Painel de Preços federal | 40 |
| **Relacionamento** | Sócio com sobrenome idêntico ao político da região, CPF direto, doação de campanha | 50 |
| **Objeto** | CNAE da empresa incompatível com o que foi licitado | 25 |
| **Estrutural** | Participante único, prazo edital < 3 dias, empresa aberta recentemente | 35 |
| **Sanção** | Empresa presente no CEIS ou CNEP (CGU) | 35 |

O **score final** é a soma ponderada, cap em 100. Licitações com score ≥ 50 são marcadas como **alto risco**.

---

## Stack

- **Backend:** Python 3.11 + FastAPI + SQLAlchemy async
- **Banco:** PostgreSQL 15 (pg_trgm para busca fuzzy de nomes)
- **Cache/Queue:** Redis
- **Scheduler:** APScheduler (coleta diária/semanal automática)
- **Frontend:** Lovable (conecta via API REST)

---

## Fontes de dados

| Fonte | O que coleta |
|-------|-------------|
| **PNCP** | Todas as licitações (federal/estadual/municipal) desde 2023 |
| **BrasilAPI / ReceitaWS** | CNPJ + QSA (sócios) das empresas vencedoras |
| **TSE** | Candidatos eleitos, bens declarados, doações de campanha |
| **CGU** | CEIS (inidôneas) + CNEP (punidas) |

---

## Setup rápido

### 1. Clone e configure

```bash
git clone <repo>
cd licitacao-radar
cp .env.example .env
# edite .env com sua chave do Portal da Transparência (opcional)
```

### 2. Suba os serviços

```bash
docker-compose up -d
```

A API estará em: `http://localhost:8000`  
Docs automáticos: `http://localhost:8000/docs`

### 3. Dispare a primeira coleta

```bash
curl -X POST http://localhost:8000/api/v1/coletar \
  -H "Content-Type: application/json" \
  -d '{"uf": "SP", "dias_retroativos": 90}'
```

---

## Configuração via .env

```env
ESTADO_ALVO=SP                    # UF para coleta diária automática
DB_PASSWORD=anticorrupcao123
TRANSPARENCIA_API_KEY=            # opcional, aumenta limites
UPDATE_INTERVAL_HOURS=24
ALLOWED_ORIGINS=https://seu-projeto.lovable.app
```

---

## Endpoints da API

### Dashboard
```
GET /api/v1/dashboard?uf=SP
```

### Licitações (com filtros)
```
GET /api/v1/licitacoes?uf=SP&score_min=50&flag=relacionamento&pagina=1
GET /api/v1/licitacoes/{id}
```

**Parâmetros de filtro:**
- `uf` — sigla do estado
- `municipio` — código IBGE
- `esfera` — FEDERAL | ESTADUAL | MUNICIPAL
- `score_min` — score mínimo (0–100)
- `flag` — `preco` | `relacionamento` | `objeto` | `participante`
- `categoria` — OBRAS_CONSTRUCAO | TECNOLOGIA_TI | SAUDE_SERVICOS | etc.
- `busca` — texto livre no objeto

### Empresas
```
GET /api/v1/empresas?uf=SP
GET /api/v1/empresas/{cnpj}
```

### Políticos
```
GET /api/v1/politicos?uf=SP&cargo=Prefeito
GET /api/v1/politicos/{cpf}
```

### Mapa de risco
```
GET /api/v1/mapa-risco?uf=SP
```
Retorna score médio por município — pronto para montar heatmap no Lovable.

### Coleta manual
```
POST /api/v1/coletar
{"uf": "SP", "dias_retroativos": 90}
```

---

## Integração com Lovable

No Lovable, configure a URL base da API como variável de ambiente:

```
VITE_API_URL=https://sua-api.railway.app
```

Exemplo de fetch no componente:

```typescript
const { data } = await fetch(`${import.meta.env.VITE_API_URL}/api/v1/dashboard?uf=SP`)
```

### Deploy recomendado

| Serviço | Custo estimado |
|---------|---------------|
| Railway (API + Postgres + Redis) | ~$10–20/mês |
| Render | gratuito com limitações |
| Fly.io | ~$5–15/mês |

---

## Estrutura do projeto

```
licitacao-radar/
├── main.py                  # entrypoint uvicorn
├── scheduler.py             # coleta automática
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
│
├── database/
│   ├── schema.sql           # todas as tabelas + views
│   └── db.py                # conexão async
│
├── collectors/
│   ├── pncp.py              # Portal Nacional de Contratações
│   ├── cnpj.py              # Receita Federal (BrasilAPI)
│   ├── tse.py               # TSE candidatos/bens/doações
│   └── cgu.py               # CGU sanções (CEIS/CNEP)
│
├── engine/
│   ├── analyzer.py          # detecção de anomalias
│   └── pipeline.py          # orquestração coleta→análise→persistência
│
└── api/
    └── main.py              # FastAPI endpoints
```

---

## Expandindo o sistema

### Adicionar Painel de Preços federal
A API `https://paineldeprecos.planejamento.gov.br` permite buscar preços de referência por item. Integrar no `analyzer.py` → `analisar_preco_painel_precos()`.

### Adicionar Portal da Transparência
Para contratos federais já executados (vs. apenas licitações abertas):
`GET https://api.portaldatransparencia.gov.br/api-de-dados/contratos`

### Adicionar TCEs estaduais
Cada TCE tem API própria. Os com melhor cobertura: TCE-SP, TCE-RS, TCE-MG.

### Detecção de consórcio fictício
Checar se empresas "concorrentes" têm sócios em comum ou mesmo endereço — cartel em licitação.
