"""
SCHEDULER - COBERTURA NACIONAL COMPLETA
Coleta todos os 27 estados automaticamente.

Inclui servidor HTTP mínimo na porta 8080 para o healthcheck do Railway.
"""

import asyncio
import os
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger
from database.db import AsyncSessionLocal
from engine.pipeline import Pipeline

UFS_BRASIL = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

UFS_PRIORITARIOS = ["SP", "RJ", "MG", "BA", "PR", "RS", "GO", "PE", "CE", "DF"]

_rotacao_index = 0


# ─── Servidor de health para Railway ────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/health", "/"):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok","service":"scheduler"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # silencia logs de acesso


def _start_health_server():
    port = int(os.environ.get("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info(f"Health server rodando na porta {port}")
    server.serve_forever()


# ─── Coleta ──────────────────────────────────────────────────────────────────

async def _coletar_uf(uf: str, dias: int):
    logger.info(f"[SCHEDULER] ▶ Coletando UF={uf} | {dias} dias retroativos")
    async with AsyncSessionLocal() as db:
        pipeline = Pipeline(db)
        try:
            resultado = await pipeline.executar(uf=uf, dias_retroativos=dias)
            logger.info(
                f"[SCHEDULER] ✓ UF={uf} | "
                f"{resultado.get('total_licitacoes', 0)} licitações | "
                f"{resultado.get('total_anomalias', 0)} anomalias"
            )
        except Exception as e:
            logger.error(f"[SCHEDULER] ✗ UF={uf}: {type(e).__name__}: {e}")


async def carga_inicial_nacional():
    logger.info("=" * 60)
    logger.info("[SCHEDULER] CARGA INICIAL NACIONAL — todos os 27 estados")
    logger.info("=" * 60)

    # Fase 1 — prioritários em paralelo de 3
    logger.info(f"[SCHEDULER] Fase 1: estados prioritários (paralelo x3)")
    for i in range(0, len(UFS_PRIORITARIOS), 3):
        lote = UFS_PRIORITARIOS[i:i + 3]
        await asyncio.gather(*[_coletar_uf(uf, 90) for uf in lote])
        await asyncio.sleep(3)

    # Fase 2 — restantes
    restantes = [uf for uf in UFS_BRASIL if uf not in UFS_PRIORITARIOS]
    logger.info(f"[SCHEDULER] Fase 2: {len(restantes)} estados restantes")
    for i in range(0, len(restantes), 3):
        lote = restantes[i:i + 3]
        await asyncio.gather(*[_coletar_uf(uf, 90) for uf in lote])
        await asyncio.sleep(3)

    logger.info("[SCHEDULER] Carga inicial concluída!")


async def job_atualizacao_diaria():
    logger.info("[SCHEDULER] Job diário — estados prioritários")
    for uf in UFS_PRIORITARIOS:
        await _coletar_uf(uf, 7)
        await asyncio.sleep(1)


async def job_rotacao():
    global _rotacao_index
    logger.info(f"[SCHEDULER] Rotação 12h — lote a partir do índice {_rotacao_index}")
    lote = UFS_BRASIL[_rotacao_index:_rotacao_index + 5]
    await asyncio.gather(*[_coletar_uf(uf, 14) for uf in lote])
    _rotacao_index = (_rotacao_index + 5) % len(UFS_BRASIL)


async def job_retroativo_semanal():
    logger.info("[SCHEDULER] Varredura semanal 180 dias")
    for i in range(0, len(UFS_BRASIL), 3):
        lote = UFS_BRASIL[i:i + 3]
        await asyncio.gather(*[_coletar_uf(uf, 180) for uf in lote])
        await asyncio.sleep(5)


def main():
    # Inicia health server em thread separada (não bloqueia o event loop)
    health_thread = threading.Thread(target=_start_health_server, daemon=True)
    health_thread.start()

    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        job_atualizacao_diaria,
        CronTrigger(hour=6, minute=0),
        id="diario_prioritarios",
        name="Atualização diária estados prioritários",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    scheduler.add_job(
        job_rotacao,
        IntervalTrigger(hours=12),
        id="rotacao_12h",
        name="Rotação 12h todos os estados",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    scheduler.add_job(
        job_retroativo_semanal,
        CronTrigger(day_of_week="sun", hour=1, minute=0),
        id="retroativo_semanal",
        name="Varredura semanal 180 dias",
        replace_existing=True,
        misfire_grace_time=7200,
    )

    scheduler.start()
    logger.info("Scheduler iniciado — cobertura nacional completa (27 estados)")
    logger.info(f"Jobs: {[job.name for job in scheduler.get_jobs()]}")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(carga_inicial_nacional())

    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler encerrado.")


if __name__ == "__main__":
    main()
