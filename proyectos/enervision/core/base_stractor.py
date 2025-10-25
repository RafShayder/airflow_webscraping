from __future__ import annotations
from typing import Any, Dict
import os
import paramiko
from types import SimpleNamespace
from core.exceptions import RetryableExtractError, NonRetryableExtractError
from core.utils import asegurar_directorio_sftp

class BaseExtractorSFTP():
    """
      Clase estandar de extracción de datos
      - variables: config(parametros de conexión al sftp) 
      - soporta ->  extrae todo tipo de archivo
      - permite verificar conectividad y parametros necesarios para conexion
    """
    
    def __init__(self, config: dict):
        
        super().__init__()
        if not isinstance(config, dict):
            raise TypeError("config debe ser un dict con las claves esperadas")
        
        self._cfg: Dict[str, Any] = config
        self._cfg_obj = SimpleNamespace(**config)
    # ----------
    #  VALIDA CAMPOS OBLIGATORIOS
    # ----------   
    def validate(self) -> None:
        c = self._cfg
        required = ["host", "port", "username", "remote_dir", "specific_filename", "local_dir"]
        missing = [k for k in required if k not in c or c[k] in (None, "")]
        if missing:
            return {
                "status": "error",
                "code": 500,
                "message": f"Flata campos {missing}"
            }
        print("campos minimos necesarios comprobado")
        return {
                "status": "success",
                "code": 200,
                "message": f"Todo correcto"
            }
       
    @property
    def config(self) -> SimpleNamespace:
        "Acceso por atributos: e.g. self.config.host"
        return self._cfg_obj
    
    # ----------
    #  VALIDAR CONEXION
    # ----------
    def validar_conexion(self):
        try:
            transport = paramiko.Transport((self.config.host, self.config.port))
            usuario = self.config.username
            password = self.config.password
            transport.connect(username=usuario, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.close()
            transport.close()
            print('conexion exitosa')
            return {
            "status": "success",
            "code": 200,
            "message": "Conexión exitosa"
            }
        except Exception as e:
            print('error de conexion: -- ', e) 
            return {
                "status": "error",
                "code": 401,
                "message": f"Conexión exitosa:  {str(e)}"
            }
       
    # ----------
    #  EXTRAE DATOS
    # ----------
    def extract(self,remotetransfere=False) -> str:
        """
            Tiene dos formas
            1: remotetransfere: Falso, descarga la data en el ruta lacal que se pasa
            2: remotetransfere: True, transfiere la data a la ruta en el host, tomando como ruta local_dir
        """
        try:
            
            transport = paramiko.Transport((self.config.host, self.config.port))
            usuario=self.config.username
            password=self.config.password
            rutasftp=self.config.remote_dir
            archivo=self.config.specific_filename
            ruta_local=self.config.local_dir
            transport.connect(username=usuario, password=password)
            sftp = paramiko.SFTPClient.from_transport(transport)
        
            if(remotetransfere):

                asegurar_directorio_sftp(sftp, ruta_local)
                sftp.rename(rutasftp + '/' + archivo, ruta_local + '/' + archivo)
                print(f"Archivo movido con éxito de {rutasftp+'/'+archivo} a {ruta_local}")


            else:    
                try:
                    os.makedirs(ruta_local, exist_ok=True)
    
                    print("se creó : ",ruta_local)
                except:
                    print('la carpeta ya existe')
                    
                sftp.get(rutasftp+'/'+archivo, ruta_local+'/'+archivo)
            
            sftp.close()    
            transport.close()
            print("se extrajo correctamente")
            return {
            "status": "success",
            "code": 200,
            "message": "se extrajo correctamente en "+ ruta_local+'/'+archivo ,
            "ruta": ruta_local+'/'+archivo
            }
        
        except Exception as e:
            print('error de extracción', e)
            return {
            "status": "error",
            "code": 500,
            "message": f"Error de estracción, error->: {e}"
            }
       


