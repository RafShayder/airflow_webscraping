from __future__ import annotations
from typing import Any, Dict, List
import os
import paramiko
from types import SimpleNamespace
from core.utils import asegurar_directorio_sftp
import logging
from datetime import datetime
logger = logging.getLogger(__name__)

class BaseExtractorSFTP:
    """
    Clase base para extracción de datos desde SFTP.
    Permite:
    - Validar parámetros de conexión y rutas.
    - Establecer conexión SFTP reutilizable.
    - Listar archivos remotos.
    - Descargar o mover archivos.
    """

    def __init__(self, config_connect: dict, config_paths: dict):
        """
        Inicializa el extractor dividiendo la configuración en:
        - config_connect: parámetros de conexión (host, port, username, password)
        - config_paths: rutas de archivos (remote_dir, local_dir, specific_filename, etc.)
        """
        if not isinstance(config_connect, dict) or not isinstance(config_paths, dict):
            logger.error("config_connect y config_paths deben ser diccionarios válidos, Parámetros de configuración inválidos")
            raise ValueError("config_connect y config_paths deben ser diccionarios válidos")

        # Configuración separada
        self._cfg_connect: Dict[str, Any] = config_connect
        self._cfg_paths: Dict[str, Any] = config_paths

        # Objetos de acceso por atributos
        self._connect = SimpleNamespace(**config_connect)
        self._paths = SimpleNamespace(**config_paths)

    # ----------
    # VALIDAR CONFIGURACIÓN
    # ----------
    def validate(self) -> Dict[str, Any]:
        conn = self._cfg_connect
        paths = self._cfg_paths

        required_conn = ["host", "port", "username"]
        required_paths = ["remote_dir", "local_dir"]

        missing_conn = [k for k in required_conn if k not in conn or not conn[k]]
        missing_paths = [k for k in required_paths if k not in paths or not paths[k]]

        if missing_conn or missing_paths:
            msg = f"Faltan campos: conexión={missing_conn}, rutas={missing_paths}"
            logger.error(msg)
            raise ValueError(msg)

        retornoinfo = {
            "status": "success",
            "code": 200,
            "etl_msg": "Configuraciones de conexión y rutas válidas"
        }
        logger.debug("Validación completa de configuración exitosa")
        return retornoinfo

    # ----------
    # PROPIEDADES DE ACCESO
    # ----------
    @property
    def conn(self) -> SimpleNamespace:
        """Acceso a los parámetros de conexión (self.conn.host, self.conn.username, etc.)"""
        return self._connect

    @property
    def paths(self) -> SimpleNamespace:
        """Acceso a los parámetros de rutas (self.paths.remote_dir, self.paths.local_dir, etc.)"""
        return self._paths

    # ----------
    # CONEXIÓN REUTILIZABLE
    # ----------
    def conectar_sftp(self) -> paramiko.SFTPClient:
        """Devuelve un cliente SFTP activo listo para usar."""
        try:
 
            transport = paramiko.Transport((self.conn.host, self.conn.port))
            transport.connect(username=self.conn.username, password=self.conn.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logger.debug(f"Conexión SFTP establecida con {self.conn.host}")
            return sftp
        except Exception as e:
            logger.error(f"Error al conectar con SFTP: {e}")
            raise ConnectionError(f"No se pudo conectar al SFTP: {e}")

    # ----------
    # VALIDAR CONEXIÓN
    # ----------
    def validar_conexion(self) -> Dict[str, Any]:
        try:
            sftp = self.conectar_sftp()
            sftp.close()
            retornoinfo = {
                "status": "success",
                "code": 200,
                "etl_msg": f"Conexión exitosa a {self.conn.host}"
            }
            logger.debug(retornoinfo["etl_msg"])
            return retornoinfo
        except Exception as e:
            retornoinfo = {
                "status": "error",
                "code": 401,
                "etl_msg": f"Error de conectividad: {e}"
            }
            logger.error(retornoinfo["etl_msg"],extra=retornoinfo)
            raise

    # ----------
    # LISTAR ARCHIVOS EN DIRECTORIO REMOTO
    # ----------
    def listar_archivos(self, ruta_remota: str | None = None) -> List[str]:
        ruta = ruta_remota or self.paths.remote_dir
        try:
            sftp = self.conectar_sftp()
            archivos = sftp.listdir(ruta)
            sftp.close()
            logger.debug(f"Archivos encontrados en {ruta}: {archivos}")
            return archivos
        except Exception as e:
            logger.error(f"Error al listar archivos en {ruta}: {e}")
            raise
    # Funcion que trae el nombre de archivo(como hace list_dir) pero tambien la fecha de modificacion y otros atributos en una lista de objetos json
    def listar_archivos_atributos(self, ruta_remota: str | None = None) -> List[paramiko.SFTPAttributes]:
        ruta = ruta_remota or self.paths.remote_dir
        try:
            sftp = self.conectar_sftp()
            archivos_atributos = sftp.listdir_attr(ruta)
            archivos = []
            for attr in archivos_atributos:
                fecha = datetime.fromtimestamp(attr.st_mtime)
                archivos.append({
                    "nombre": attr.filename,
                    "fecha_modificacion": fecha,
                    "tipo": attr.filename.split(".")[-1].lower() if "." in attr.filename else ""
                })
            sftp.close()
            logger.debug(f"Atributos de archivos encontrados en {ruta}")
            return archivos
        except Exception as e:
            logger.error(f"Error al listar atributos de archivos en {ruta}: {e}")
            raise
        
       
    # ----------
    # EXTRAER / MOVER ARCHIVO
    # ----------
    def extract(self, remotetransfere: bool = False, specific_file: str | None = None) -> Dict[str, Any]:
        try:
            sftp = self.conectar_sftp()
            remote_dir = self.paths.remote_dir
            local_dir = self.paths.local_dir
            archivo = specific_file or getattr(self.paths, "specific_filename", None)
    
            if not archivo:
                raise ValueError("Debe especificarse un archivo para la extracción.")

            if remotetransfere:
                asegurar_directorio_sftp(sftp, local_dir)
               
                sftp.rename(f"{remote_dir}/{archivo}", f"{local_dir}/{archivo}")
                msg = f"Archivo movido con éxito de {remote_dir}/{archivo} a {local_dir}"
                logger.info(msg)
            else:
                os.makedirs(local_dir, exist_ok=True)
                sftp.get(f"{remote_dir}/{archivo}", f"{local_dir}/{archivo}")
                msg = f"Archivo descargado correctamente a {local_dir}/{archivo}"
                logger.info(msg)

            sftp.close()
            retornoinfo = {
                "status": "success",
                "code": 200,
                "etl_msg": msg,
                "ruta": f"{local_dir}/{archivo}"
            }
            return retornoinfo
        except Exception as e:
            retornoinfo = {
                "status": "error",
                "code": 500,
                "etl_msg": f"Error de extracción: {e}"
            }
            logger.error(retornoinfo["etl_msg"])
            raise
