"""
SCHEDULER - COBERTURA NACIONAL COMPLETA
Coleta todos os 27 estados automaticamente.
Sem necessidade de configurar ESTADO_ALVO.

Estratégia de rotação:
- Carga inicial: todos os estados, retroativo 90 dias (roda uma vez ao iniciar)
- Job diário: rotaciona pelos estados em lotes, cada estado atualizado ~a cada 2 dias
- Job semanal: retroativo estendido (180 dias) para pegar licitações antigas
"""

import asyncio
import os
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger
from database.db import AsyncSessionLocal
from engine.pipeline import Pipeline

# Todos os 27 estados + DF
UFS_BRASIL = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

# Estados com maior volume — coletados com mais frequência
UFS_PRIORITARIOS = ["SP", "RJ", "MG", "BA", "PR", "RS", "GO", "PE", "CE", "DF"]

# Controle de rotação em memória
_rotacao_index = 0


async def _coletar_uf(uf: str, dias: int):
    logger.info(f"[SCHEDULER] ▶ Coletando UF={uf} | {dias} dias retroativos")
    async with AsyncSessionLocal() as db:
        pipeline = Pipeline(db)
        try:
            resultado = await pipeline.executar(uf=uf, dias_retroativos=dias)
            logger.info(f"[SCHEDULER] ✓ UF={uf} | {resultado.get('total_licitacoes', 0)} licitações | {resultado.get('total_anomalias', 0)} anomalias")
        except Exception as e:
            logger.error(f"[SCHEDULER] ✗ UF={uf}: {e}")


async def carga_inicial_nacional():
    """
    Roda uma única vez ao iniciar.
    Coleta todos os 27 estados com 90 dias retroativos.
    Paralelo em lotes de 3 para não sobrecarregar as APIs.
    """
    logger.info("=" * 60)
    logger.info("[SCHEDULER] CARGA INICIAL NACIONAL — todos os 27 estados")
    logger.info("=" * 60)

    # Prioritários primeiro, em paralelo
    logger.info("[SCHEDULER] Fase 1: estados prioritários (paralelo x3)")
    for i in range(0, len(UFS_PRIORITARIOS), 3):
        lote = UFS_PRIORITARIOS[i:i+3]
        await asyncio.gather(*[_coletar_uf(uf, 90) for uf in lote])
        await asyncio.sleep(3)

    # Demais estados
    restantes = [uf for uf in UFS_BRASIL if uf not in UFS_PRIORITARIOS]
    logger.info(f"[SCHEDULER] Fase 2: {len(restantes)} estados restantes")
    for i in range(0, len(restantes), 3):
        lote = restantes[i:i+3]
        await asyncio.gather(*[_coletar_uf(uf, 90) for uf in lote])
        await asyncio.sleep(3)

    logger.info("[SCHEDULER] ✓ Carga inicial concluída")


async def job_rotacao_diaria():
    """
    Roda a cada 12 horas.
    Atualiza um lote de ~5 estados em rotação circular.
    Em ~3 dias todos os estados são atualizados.
    """
    global _rotacao_index
    LOTE = 5

    uf_lote = UFS_BRASIL[_rotacao_index: _rotacao_index + LOTE]
    if not uf_lote:
        _rotacao_index = 0
        uf_lote = UFS_BRASIL[:LOTE]

    _rotacao_index = (_rotacao_index + LOTE) % len(UFS_BRASIL)

    logger.info(f"[SCHEDULER] Rotação diária — lote: {uf_lote}")
    for uf in uf_lote:
        await _coletar_uf(uf, 30)
        await asyncio.sleep(2)


async def job_prioritarios_diario():
    """
    Roda todo dia às 6h.
    Mantém os 10 maiores estados sempre frescos (últimos 7 dias).
    """
    logger.info("[SCHEDULER] Atualizando estados prioritários")
    for i in range(0, len(UFS_PRIORITARIOS), 3):
        lote = UFS_PRIORITARIOS[i:i+3]
        await asyncio.gather(*[_coletar_uf(uf, 7) for uf in lote])
        await asyncio.sleep(2)


async def job_retroativo_semanal():
    """
    Roda todo domingo às 1h.
    Retroativo de 180 dias em todos os estados — pega licitações antigas.
    """
    logger.info("[SCHEDULER] Varredura semanal retroativa (180 dias)")
    for i in range(0, len(UFS_BRASIL), 3):
        lote = UFS_BRASIL[i:i+3]
        await asyncio.gather(*[_coletar_uf(uf, 180) for uf in lote])
        await asyncio.sleep(5)


def main():
    scheduler = AsyncIOScheduler(timezone="America/Sao_Paulo")

    # Estados prioritários — atualização diária às 6h
    scheduler.add_job(
        job_prioritarios_diario,
        CronTrigger(hour=6, minute=0),
        id="prioritarios_diario",
        name="Atualização diária estados prioritários",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Rotação dos demais — a cada 12 horas
    scheduler.add_job(
        job_rotacao_diaria,
        IntervalTrigger(hours=12),
        id="rotacao_12h",
        name="Rotação 12h todos os estados",
        replace_existing=True,
        misfire_grace_time=3600,
    )

    # Varredura retroativa semanal — domingo 1h
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

    # Carga inicial ao subir o container
    loop.run_until_complete(carga_inicial_nacional())

    try:
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler encerrado.")


if __name__ == "__main__":
    main()
