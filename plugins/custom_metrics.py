from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.stats import Stats
import os
import logging
import hashlib

logger = logging.getLogger(__name__)


def get_run_id_hash(run_id):
    """
    Genera un hash corto del run_id para usar como identificador único
    sin incluir caracteres especiales que causen problemas en StatsD
    """
    return hashlib.md5(run_id.encode()).hexdigest()[:8]


class DagRunMetricsListener:
    """
    Listener que envía métricas cuando un DAG Run se completa.

    IMPORTANTE: Este plugin envía métricas por DAG, NO por run_id,
    para evitar duplicados y explosión de cardinalidad en Prometheus.
    """

    @hookimpl
    def on_dag_run_success(self, dag_run, msg):
        """Ejecutado cuando un DAG Run es exitoso"""
        try:
            # Emitir métricas solo si este proceso está habilitado
            if os.getenv("METRICS_EMITTER") != "1":
                return

            dag_id = dag_run.dag_id
            run_id = getattr(dag_run, "run_id", "unknown")
            run_id_hash = get_run_id_hash(run_id)

            if dag_run.start_date and dag_run.end_date:
                duration = (dag_run.end_date - dag_run.start_date).total_seconds()
                start_timestamp = dag_run.start_date.timestamp()
                end_timestamp = dag_run.end_date.timestamp()

                # ============================================================
                # MÉTRICAS AGREGADAS POR DAG (sin run_id en el nombre)
                # ============================================================

                # 1. Contador de ejecuciones exitosas (incrementa en cada run)
                Stats.incr(f"dagrun.execution.success.{dag_id}")

                # 2. Duración de la ejecución (histogram para estadísticas)
                Stats.timing(f"dagrun.duration.success.{dag_id}", duration)

                # ============================================================
                # MÉTRICAS DETALLADAS POR RUN (para historial)
                # ============================================================
                # Usamos un hash del run_id para tener un identificador único
                # pero sin los caracteres especiales del run_id original

                # 3. Información del run (gauge con valor 1 para indicar existencia)
                Stats.gauge(f"dagrun.info.{dag_id}.{run_id_hash}.success", 1)

                # 4. Timestamps para reconstruir el historial
                Stats.gauge(f"dagrun.start_timestamp.{dag_id}.{run_id_hash}", start_timestamp)
                Stats.gauge(f"dagrun.end_timestamp.{dag_id}.{run_id_hash}", end_timestamp)

                logger.info(
                    f"✅ Métricas enviadas para {dag_id}: "
                    f"duration={duration}s, run_id_hash={run_id_hash}, "
                    f"start={start_timestamp}, end={end_timestamp}"
                )
        except Exception as e:
            logger.error(f"❌ Error enviando métrica de éxito: {e}", exc_info=True)

    @hookimpl
    def on_dag_run_failed(self, dag_run, msg):
        """Ejecutado cuando un DAG Run falla"""
        try:
            # Emitir métricas solo si este proceso está habilitado
            if os.getenv("METRICS_EMITTER") != "1":
                return

            dag_id = dag_run.dag_id
            run_id = getattr(dag_run, "run_id", "unknown")
            run_id_hash = get_run_id_hash(run_id)

            if dag_run.start_date and dag_run.end_date:
                duration = (dag_run.end_date - dag_run.start_date).total_seconds()
                start_timestamp = dag_run.start_date.timestamp()
                end_timestamp = dag_run.end_date.timestamp()

                # ============================================================
                # MÉTRICAS AGREGADAS POR DAG (sin run_id en el nombre)
                # ============================================================

                # 1. Contador de ejecuciones fallidas (incrementa en cada run)
                Stats.incr(f"dagrun.execution.failed.{dag_id}")

                # 2. Duración de la ejecución (histogram para estadísticas)
                Stats.timing(f"dagrun.duration.failed.{dag_id}", duration)

                # ============================================================
                # MÉTRICAS DETALLADAS POR RUN (para historial)
                # ============================================================

                # 3. Información del run (gauge con valor 1 para indicar existencia)
                Stats.gauge(f"dagrun.info.{dag_id}.{run_id_hash}.failed", 1)

                # 4. Timestamps para reconstruir el historial
                Stats.gauge(f"dagrun.start_timestamp.{dag_id}.{run_id_hash}", start_timestamp)
                Stats.gauge(f"dagrun.end_timestamp.{dag_id}.{run_id_hash}", end_timestamp)

                logger.info(
                    f"❌ Métricas enviadas para {dag_id}: "
                    f"duration={duration}s, run_id_hash={run_id_hash}, "
                    f"start={start_timestamp}, end={end_timestamp}"
                )
        except Exception as e:
            logger.error(f"❌ Error enviando métrica de fallo: {e}", exc_info=True)


# Plugin de Airflow - esto es lo que Airflow cargará automáticamente
class CustomMetricsPlugin(AirflowPlugin):
    name = "custom_metrics_plugin"
    listeners = [DagRunMetricsListener()]
