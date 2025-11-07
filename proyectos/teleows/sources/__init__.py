"""
Fuentes ETL para los pipelines de Teleows.

Cada submódulo sigue el patrón:
    - stractor.py      → extracción / scraping
    - transformer.py   → transformaciones previas a la carga
    - loader.py        → inserción en el destino (BD, SFTP, etc.)
    - run_sp.py        → ejecuciones de stored procedures/post-procesos
    - main_temp.py     → script local para pruebas manuales
"""

__all__ = ["dynamic_checklist", "gde"]
