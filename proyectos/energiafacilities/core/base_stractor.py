from __future__ import annotations
from typing import Any, Dict, List
import os
import io
import pandas as pd
import paramiko
from types import SimpleNamespace
from core.helpers import asegurar_directorio_sftp
import logging
from datetime import datetime
import stat

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
        transport = None
        try:
            transport = paramiko.Transport((self.conn.host, self.conn.port))
            transport.connect(username=self.conn.username, password=self.conn.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logger.debug(f"Conexión SFTP establecida con {self.conn.host}")
            return sftp
        except Exception as e:
            if transport:
                transport.close()
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
    def listar_archivos(self, ruta_remota: str | None = None, only_files: bool = True) -> List[str]:
        ruta = ruta_remota or self.paths.remote_dir
        sftp = None
        try:
            sftp = self.conectar_sftp()
            archivos = sftp.listdir_attr(ruta)
            if only_files:
                archivos=[
                item.filename
                for item in archivos
                if stat.S_ISREG(item.st_mode)
            ]

            else:
                archivos = [item.filename for item in archivos]
                
                
            logger.debug(f"Archivos encontrados en {ruta}: {archivos}")
            
            return archivos
        except Exception as e:
            logger.error(f"Error al listar archivos en {ruta}: {e}")
            raise
        finally:
            if sftp:
                sftp.close()
    # Funcion que trae el nombre de archivo(como hace list_dir) pero tambien la fecha de modificacion y otros atributos en una lista de objetos json
    def listar_archivos_atributos(self, ruta_remota: str | None = None) -> List[paramiko.SFTPAttributes]:
        ruta = ruta_remota or self.paths.remote_dir
        sftp = None
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
            logger.debug(f"Atributos de archivos encontrados en {ruta}")
            return archivos
        except Exception as e:
            logger.error(f"Error al listar atributos de archivos en {ruta}: {e}")
            raise
        finally:
            if sftp:
                sftp.close()
        
       
    # ----------
    # EXTRAER / MOVER ARCHIVO
    # ----------
    def extract(self, remotetransfere: bool = False, specific_file: str | None = None) -> Dict[str, Any]:
        sftp = None
        try:
            sftp = self.conectar_sftp()
            remote_dir = self.paths.remote_dir
            local_dir = self.paths.local_dir
            archivo = specific_file or getattr(self.paths, "specific_filename", None)

            if not archivo:
                logger.error("Debe especificarse un archivo para la extracción.")
                raise 

            if remotetransfere:
                asegurar_directorio_sftp(sftp, local_dir)
                destino = f"{local_dir}/{archivo}"

                # Validar si el archivo destino ya existe
                try:
                    sftp.stat(destino)
                    logger.warning(f"El archivo {destino} ya existe y será sobrescrito")
                except FileNotFoundError:
                    pass  # Archivo no existe, OK para mover

                sftp.rename(f"{remote_dir}/{archivo}", destino)
                msg = f"Archivo movido con éxito de {remote_dir}/{archivo} a {local_dir}"
                logger.debug(msg)
            else:
                os.makedirs(local_dir, exist_ok=True)
                sftp.get(f"{remote_dir}/{archivo}", f"{local_dir}/{archivo}")
                msg = f"Archivo descargado correctamente a {local_dir}/{archivo}"
                logger.debug(msg)

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
        finally:
            if sftp:
                sftp.close()
                


    def estract_archivos_excel(
        self,
        archivos: List[str] | str,
        nombre_salida_local: str,
        hoja: str = None,
        fila_inicio: int = None,
        local_dir: str=None,
        subsetname: str = None
        
    ) -> Dict[str, Any]:
        """
        Lee uno o varios archivos Excel desde SFTP, los unifica, agrega columna 'archivo',
        mueve procesados a processed_dir y con errores a error_dir,
        y guarda un Excel final en local_dir.
        """

        # Normalizar parámetro a lista
        if isinstance(archivos, str):
            archivos = [archivos]

        sftp = None
        dfs = []

        def rename_overwrite(sftp, origen, destino):
            """Elimina destino si existe y luego renombra."""
            # crear carpeta si no existe
            carpeta = os.path.dirname(destino)
            try:
                sftp.stat(carpeta)
            except IOError:
                sftp.mkdir(carpeta, 0o775)
                sftp.chmod(carpeta, 0o775)

            # borrar destino si existe
            try:
                sftp.stat(destino)
                sftp.remove(destino)
            except IOError:
                pass  # No existe, OK

            sftp.rename(origen, destino)

        try:
            sftp = self.conectar_sftp()

            # Crear carpetas si no existen (solo si se quiere)
            asegurar_directorio_sftp(sftp, self.paths.processed_dir)
            asegurar_directorio_sftp(sftp, self.paths.error_dir)

            for archivo in archivos:
                ruta_remota = f"{self.paths.remote_dir}/{archivo}"

                try:
                    # Descargar a memoria
                    buffer = io.BytesIO()
                    sftp.getfo(ruta_remota, buffer)
                    buffer.seek(0)

                    # Leer Excel
                    df = pd.read_excel(
                        buffer,
                        sheet_name = hoja or self.paths.default_sheet,
                        header     = fila_inicio or self.paths.fila_inicial
                    )
                    subsetname =subsetname or  self.paths.subsetna
                    df = df.dropna(subset=[subsetname])
                    
                    df["archivo"] = archivo
                    dfs.append(df)
                    #borramos las filas que sean vacias
                    # MOVIMIENTO A PROCESSED → CON OVERWRITE
                    destino_ok = f"{self.paths.processed_dir}/{archivo}"
                    rename_overwrite(sftp, ruta_remota, destino_ok)

                    logger.debug(f"Archivo procesado con éxito: {archivo}")

                except Exception as e:
                    try:
                        destino_error = f"{self.paths.error_dir}/{archivo}"
                        rename_overwrite(sftp, ruta_remota, destino_error)
                    except Exception:
                        logger.error(f"No se pudo mover {archivo} a carpeta de errores")

                    logger.error(f"Error procesando {archivo}: {e}")
                    continue

            if not dfs:
                raise Exception("Ningún archivo pudo ser procesado correctamente.")

            # Unificar
            df_final = pd.concat(dfs, ignore_index=True)

            # Guardar salida local
            salida_local = f"{local_dir or self.paths.local_dir}/{nombre_salida_local}"
            os.makedirs(os.path.dirname(salida_local), exist_ok=True)

            df_final.to_excel(salida_local, index=False)

            logger.debug(f"Archivo consolidado guardado en {salida_local}")

            return {
                "status": "success",
                "code": 200,
                "etl_msg": "Archivos procesados y consolidados correctamente",
                "ruta_local": salida_local,
            }

        finally:
            if sftp:
                sftp.close()
