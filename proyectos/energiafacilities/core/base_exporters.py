from __future__ import annotations
import os
import logging
import tempfile
import shutil
from typing import  Union
from types import SimpleNamespace
import pandas as pd
import paramiko

logger = logging.getLogger(__name__)


class FileExporter:
    """
    Clase universal para exportación y movimiento de archivos locales o remotos (SFTP).

    Funcionalidades:
    - Exportar DataFrame a CSV/Excel (local o remoto)
    - Mover archivos entre rutas locales (con o sin renombrar)
    - Subir/descargar archivos entre local y remoto
    - Mover archivos entre hosts SFTP (mismo host o distintos)
    """

    # =========================================================
    # UTILS
    # =========================================================
    def _sftp_connect(self, cfg: dict) -> paramiko.SFTPClient:
        """Crea una conexión SFTP y devuelve el cliente."""
        cfg = SimpleNamespace(**cfg)
        transport = paramiko.Transport((cfg.host, cfg.port))
        try:
            transport.connect(username=cfg.username, password=cfg.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logger.debug(f"Conectado a {cfg.host} via SFTP.")
            return sftp
        except Exception as e:
            logger.error(f"Error conectando a {cfg.host}: {e}")
            transport.close()
            raise

    def _ensure_dir(self, path: str):
        """Crea la carpeta local si no existe."""
        logger.debug("Creando la carpeta local si no existe")
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def _mkdirs_remote(self, sftp: paramiko.SFTPClient, remote_dir: str):
        """Crea recursivamente directorios en el servidor remoto vía SFTP."""
        dirs = remote_dir.strip("/").split("/")
        path = ""
        for d in dirs:
            path = f"{path}/{d}"
            try:
                sftp.stat(path)
                logger.debug(f"ya existe la carpeta: {path}")
            except FileNotFoundError:
                sftp.mkdir(path)
                logger.debug(f"Directorio remoto creado: {path}")

    # =========================================================
    # EXPORTAR DATAFRAME LOCAL
    # =========================================================
    def export_dataframe(
        self,
        df: pd.DataFrame,
        destination_path: str,
        index: bool = False,
        target_tz: str = "America/Lima"
    ) -> str:
        """
        Exporta DataFrame a CSV/Excel asegurando compatibilidad para Excel.
        - Convierte datetimes con timezone (tz-aware) a tz-naive.
        - Por defecto convierte primero a America/Lima antes de quitar el tz.
        """

        try:
            # ===============================
            # Validaciones
            # ===============================
            if not isinstance(df, pd.DataFrame):
                logger.error("El parámetro 'df' no es un DataFrame.")
                raise TypeError("El parámetro 'df' debe ser un DataFrame")

            if not isinstance(destination_path, str):
                logger.error("El parámetro 'destination_path' no es str.")
                raise TypeError("El parámetro 'destination_path' debe ser un string")
            logger.info("Iniciando el proceso de extracción")

            # ===============================
            # Convertir datetimes tz-aware
            # ===============================
            # Creamos copia para no modificar el dataframe original
            df_export = df.copy()

            tz_cols = df_export.select_dtypes(include=["datetimetz"]).columns

            if len(tz_cols) > 0:
                logger.debug(f"Normalizando columnas datetime con timezone: {list(tz_cols)}")

            for col in tz_cols:
                try:
                    df_export[col] = (
                        df_export[col]
                        .dt.tz_convert(target_tz)   # Convertimos a TZ deseado
                        .dt.tz_localize(None)       # Quitamos tz
                    )
                except Exception:
                    # Caso: ya está tz-naive pero dtype incorrecto, o formato raro
                    df_export[col] = pd.to_datetime(
                        df_export[col],
                        errors="coerce"
                    ).dt.tz_localize(None)

            # ===============================
            # Crear directorio si no existe
            # ===============================
            self._ensure_dir(destination_path)

            # ===============================
            # Exportar según extensión
            # ===============================
            ext = destination_path.lower()

            if ext.endswith(".csv"):
                df_export.to_csv(destination_path, index=index, encoding="utf-8-sig")

            elif ext.endswith((".xlsx", ".xls")):
                df_export.to_excel(destination_path, index=index, engine="openpyxl")

            else:
                raise ValueError("Formato no soportado. Usa .csv, .xlsx o .xls")

            logger.info(f" Archivo exportado correctamente → {destination_path}")
            return destination_path

        except Exception as e:
            logger.error(f" Error exportando DataFrame: {e}", exc_info=True)
            raise
    # =========================================================
    # MOVER ARCHIVOS LOCALES O DF
    # =========================================================
    def move_local(
        self,
        src: Union[str, pd.DataFrame],
        dst: str,
        index: bool = False
    ):
        """
        Mueve o exporta un archivo o DataFrame a una nueva ubicación local.
        - Si src es un DataFrame, se exporta a CSV/Excel.
        - Si src es una ruta, se mueve o renombra el archivo.
        """
        try:
            self._ensure_dir(dst)

            # Caso 1: DataFrame
            if isinstance(src, pd.DataFrame):
                if dst.lower().endswith(".csv"):
                    src.to_csv(dst, index=index, encoding="utf-8-sig")
                elif dst.lower().endswith((".xlsx", ".xls")):
                    src.to_excel(dst, index=index, engine="openpyxl")
                else:
                    logger.error("Formato no soportado. Usa .csv o .xlsx para guardar el DataFrame.")
                    raise ValueError("Formato no soportado. Usa .csv o .xlsx para guardar el DataFrame") 
                logger.info(f"DataFrame exportado localmente a {dst}")

            # Caso 2: archivo existente
            elif isinstance(src, str):
                if not os.path.exists(src):
                    logger.error(f"No se encontró el archivo origen: {src}")
                    raise FileNotFoundError(f"No se encontró el archivo origen: {src}") 
                shutil.move(src, dst)
                logger.info(f"Archivo movido localmente: {src} → {dst}")

            else:
                logger.error("El parámetro data:'src' debe ser un str (ruta) o un pandas.DataFrame.")
                raise TypeError("El parámetro 'src' debe ser un str (ruta) o un pandas.DataFrame") 

        except Exception as e:
            logger.error(f"Error moviendo o exportando localmente: {e}")
            raise

    # =========================================================
    # LOCAL ↔ REMOTO
    # =========================================================
    def upload_to_remote(self, conn: dict, local_path: str, remote_path: str):
        """Sube un archivo local a un servidor remoto, creando directorios si no existen."""
        sftp = self._sftp_connect(conn)
        logger.info(f"Subiendo archivos a {remote_path}")
        try:
            remote_dir = os.path.dirname(remote_path)
            self._mkdirs_remote(sftp, remote_dir)
            sftp.put(local_path, remote_path)
            logger.info(f"Archivo subido: {local_path} → {conn['host']}:{remote_path}")
        except Exception as e:
            logger.error(f"Error subiendo archivo a remoto: {e}")
            raise
        finally:
            sftp.close()

    def download_from_remote(self, conn: dict, remote_path: str, local_path: str):
        """Descarga un archivo remoto a local, creando carpetas locales si no existen."""
        sftp = self._sftp_connect(conn)
        logger.info(f"Inicio de descarga en proceso {local_path}")
        try:
            self._ensure_dir(local_path)
            sftp.get(remote_path, local_path)
            logger.info(f"Archivo descargado: {conn['host']}:{remote_path} → {local_path}")
        except Exception as e:
            logger.error(f"Error descargando desde remoto: {e}")
            raise
        finally:
            sftp.close()

    # =========================================================
    # REMOTO ↔ REMOTO
    # =========================================================
    def move_file_between_hosts(
        self,
        conn_src: dict,
        conn_dst: dict,
        src_path: str,
        dst_path: str
    ):
        """
        Copia un archivo desde un host remoto a otro host remoto (pueden ser distintos).
        Usa una transferencia temporal local y crea las carpetas remotas si no existen.
        """
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_path = temp_file.name
        temp_file.close()
        logger.info(f"Transfiriendo archivo remoto -> remoto")
        try:
            # 1. Descargar desde origen
            self.download_from_remote(conn_src, src_path, temp_path)
            # 2. Subir al destino (crea directorios si no existen)
            self.upload_to_remote(conn_dst, temp_path, dst_path)
            logger.info(f"Archivo transferido entre hosts: {conn_src['host']} → {conn_dst['host']}")
        except Exception as e:
            logger.error(f"Error al mover entre hosts: {e}")
            raise
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    # =========================================================
    # EXPORTAR DF → HOST REMOTO
    # =========================================================
    def export_dataframe_to_remote(
        self,
        df: pd.DataFrame,
        conn: dict,
        remote_dir: str,
        filename: str,
        index: bool = False
    ):
        """
        Exporta un DataFrame directamente a un host remoto, creando las carpetas si no existen.
        """
        if not isinstance(df, pd.DataFrame):
            logger.error("El parámetro 'df' debe ser un pandas.DataFrame.")
            raise TypeError("El parámetro 'df' debe ser un pandas.DataFrame")
        if not filename:
            logger.error("Debe proporcionar un nombre de archivo válido para el DataFrame remoto.")
            raise ValueError("Debe proporcionar un nombre de archivo válido para el DataFrame remoto") 

        # Ruta completa final
        if not remote_dir.endswith("/"):
            remote_dir += "/"
        remote_path = remote_dir + filename
        logger.debug(f"Exportando DataFrame a remoto: {remote_path}")
        # Extensión
        ext = os.path.splitext(filename)[1].lower() or ".xlsx"
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
        temp_file.close()

        try:
            # Exportar temporal local
            self.export_dataframe(df, temp_file.name, index=index)
            # Conectar y crear directorios
            sftp = self._sftp_connect(conn)
            logger.debug(f"Creando directorios remotos si no existen: {remote_dir}")
            self._mkdirs_remote(sftp, remote_dir)

            # Subir archivo
            sftp.put(temp_file.name, remote_path)
            logger.info(f"DataFrame exportado a remoto: {conn['host']}:{remote_path}")

        except Exception as e:
            logger.error(f"Error exportando DataFrame a remoto: {e}")
            raise

        finally:
            if os.path.exists(temp_file.name):
                os.remove(temp_file.name)
            if 'sftp' in locals():
                sftp.close()





