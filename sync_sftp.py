#!/usr/bin/env python3
"""
Script para sincronizar archivos locales con un servidor SFTP.
Respeta el .gitignore y sincroniza en modo mirror (elimina archivos huérfanos).

Uso:
    python sync_sftp.py [--dry-run] [--no-delete] [--verbose]

Configuración:
    Crear archivo .env.sftp con:
        SFTP_HOST=servidor.ejemplo.com
        SFTP_PORT=22
        SFTP_USER=usuario
        SFTP_PASS=contraseña
        SFTP_REMOTE_PATH=/ruta/remota/destino
"""

import os
import sys
import argparse
import fnmatch
import stat
from pathlib import Path
from datetime import datetime
from typing import Set, List, Tuple

try:
    import paramiko
except ImportError:
    print("Error: paramiko no está instalado. Ejecuta: pip install paramiko")
    sys.exit(1)


# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

SCRIPT_DIR = Path(__file__).parent.resolve()
ENV_FILE = SCRIPT_DIR / ".env.sftp"

# Patrones adicionales a ignorar siempre (además de .gitignore)
ALWAYS_IGNORE = [
    ".git/",
    ".git",
    ".env.sftp",
    "sync_sftp.py",
    "__pycache__/",
    "*.pyc",
    ".DS_Store",
]

# Solo sincronizar estas carpetas (None = todo el proyecto)
INCLUDE_ONLY = [
    "dags",
    "proyectos",
]


# ==============================================================================
# FUNCIONES DE UTILIDAD
# ==============================================================================

def load_env_file(env_path: Path) -> dict:
    """Carga variables de un archivo .env."""
    env_vars = {}
    if not env_path.exists():
        return env_vars

    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                # Remover comillas si las tiene
                value = value.strip().strip('"').strip("'")
                env_vars[key.strip()] = value
    return env_vars


def load_gitignore_patterns(base_path: Path) -> List[str]:
    """Carga patrones del .gitignore."""
    patterns = []
    gitignore_path = base_path / ".gitignore"

    if not gitignore_path.exists():
        return patterns

    with open(gitignore_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Ignorar comentarios y líneas vacías
            if not line or line.startswith("#"):
                continue
            # Ignorar negaciones (!)
            if line.startswith("!"):
                continue
            patterns.append(line)

    return patterns


def should_ignore(path: str, patterns: List[str]) -> bool:
    """Determina si un path debe ser ignorado según los patrones."""
    # Normalizar path
    path = path.replace("\\", "/")

    for pattern in patterns:
        pattern = pattern.replace("\\", "/")

        # Patrón de directorio (termina en /)
        if pattern.endswith("/"):
            dir_pattern = pattern.rstrip("/")
            if path == dir_pattern or path.startswith(dir_pattern + "/"):
                return True
            # También verificar si algún componente del path coincide
            parts = path.split("/")
            for part in parts:
                if fnmatch.fnmatch(part, dir_pattern):
                    return True

        # Patrón con /**/ (cualquier subdirectorio)
        elif "**" in pattern:
            # Simplificar: convertir **/ a */
            simple_pattern = pattern.replace("**/", "*/").replace("/**", "/*")
            if fnmatch.fnmatch(path, simple_pattern):
                return True

        # Patrón simple
        else:
            # Verificar el path completo
            if fnmatch.fnmatch(path, pattern):
                return True
            # Verificar el nombre del archivo/directorio
            if fnmatch.fnmatch(Path(path).name, pattern):
                return True
            # Verificar si algún componente coincide
            parts = path.split("/")
            for part in parts:
                if fnmatch.fnmatch(part, pattern):
                    return True

    return False


def get_local_files(base_path: Path, ignore_patterns: List[str], include_only: List[str] = None) -> Set[str]:
    """Obtiene lista de archivos locales a sincronizar."""
    files = set()

    # Si hay filtro de carpetas, solo escanear esas
    if include_only:
        dirs_to_scan = [base_path / d for d in include_only if (base_path / d).exists()]
    else:
        dirs_to_scan = [base_path]

    for scan_dir in dirs_to_scan:
        for root, dirs, filenames in os.walk(scan_dir):
            # Calcular path relativo desde base_path
            rel_root = Path(root).relative_to(base_path)
            rel_root_str = str(rel_root) if str(rel_root) != "." else ""

            # Filtrar directorios a ignorar (modifica dirs in-place para os.walk)
            dirs[:] = [
                d for d in dirs
                if not should_ignore(
                    f"{rel_root_str}/{d}".lstrip("/") if rel_root_str else d,
                    ignore_patterns
                )
            ]

            for filename in filenames:
                if rel_root_str:
                    rel_path = f"{rel_root_str}/{filename}"
                else:
                    rel_path = filename

                if not should_ignore(rel_path, ignore_patterns):
                    files.add(rel_path)

    return files


def get_remote_files(sftp: paramiko.SFTPClient, remote_path: str, base_path: str = "", include_only: List[str] = None) -> Set[str]:
    """Obtiene lista de archivos en el servidor SFTP recursivamente."""
    files = set()

    # Si estamos en la raíz y hay filtro, solo escanear esas carpetas
    if not base_path and include_only:
        for folder in include_only:
            folder_path = f"{remote_path}/{folder}"
            try:
                sftp.stat(folder_path)
                files.update(get_remote_files(sftp, folder_path, folder, None))
            except IOError:
                pass  # Carpeta no existe en remoto
        return files

    try:
        items = sftp.listdir_attr(remote_path)
    except IOError:
        return files

    for item in items:
        item_path = f"{remote_path}/{item.filename}"
        rel_path = f"{base_path}/{item.filename}".lstrip("/") if base_path else item.filename

        if stat.S_ISDIR(item.st_mode if item.st_mode else 0):
            # Recursión para directorios
            files.update(get_remote_files(sftp, item_path, rel_path, None))
        else:
            files.add(rel_path)

    return files


def ensure_remote_dir(sftp: paramiko.SFTPClient, remote_path: str):
    """Crea directorio remoto si no existe (recursivo)."""
    dirs_to_create = []
    current_path = remote_path

    while current_path and current_path != "/":
        try:
            sftp.stat(current_path)
            break  # Directorio existe
        except IOError:
            dirs_to_create.append(current_path)
            current_path = str(Path(current_path).parent)

    # Crear directorios en orden (de padre a hijo)
    for dir_path in reversed(dirs_to_create):
        try:
            sftp.mkdir(dir_path)
        except IOError:
            pass  # Puede que ya exista


def sync_file(sftp: paramiko.SFTPClient, local_path: Path, remote_path: str, verbose: bool = False) -> bool:
    """Sincroniza un archivo local al servidor SFTP."""
    try:
        # Verificar si necesita actualización
        try:
            remote_stat = sftp.stat(remote_path)
            local_stat = local_path.stat()

            # Comparar tamaño y tiempo de modificación
            if (remote_stat.st_size == local_stat.st_size and
                remote_stat.st_mtime >= local_stat.st_mtime):
                if verbose:
                    print(f"  [SKIP] {local_path.name} (sin cambios)")
                return False
        except IOError:
            pass  # Archivo no existe en remoto

        # Asegurar que el directorio existe
        remote_dir = str(Path(remote_path).parent)
        ensure_remote_dir(sftp, remote_dir)

        # Subir archivo
        sftp.put(str(local_path), remote_path)
        return True

    except Exception as e:
        print(f"  [ERROR] {local_path}: {e}")
        return False


def delete_remote_file(sftp: paramiko.SFTPClient, remote_path: str) -> bool:
    """Elimina un archivo del servidor SFTP."""
    try:
        sftp.remove(remote_path)
        return True
    except Exception as e:
        print(f"  [ERROR] No se pudo eliminar {remote_path}: {e}")
        return False


# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Sincroniza archivos locales con servidor SFTP respetando .gitignore"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Mostrar qué se haría sin ejecutar cambios"
    )
    parser.add_argument(
        "--no-delete",
        action="store_true",
        help="No eliminar archivos huérfanos en destino"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Mostrar más detalles"
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=str(ENV_FILE),
        help=f"Archivo de configuración (default: {ENV_FILE})"
    )

    args = parser.parse_args()

    # Cargar configuración
    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Error: No se encontró el archivo de configuración: {env_path}")
        print(f"\nCrea el archivo {env_path.name} con el siguiente contenido:")
        print("  SFTP_HOST=servidor.ejemplo.com")
        print("  SFTP_PORT=22")
        print("  SFTP_USER=usuario")
        print("  SFTP_PASS=contraseña")
        print("  SFTP_REMOTE_PATH=/ruta/remota")
        sys.exit(1)

    env_vars = load_env_file(env_path)

    # Validar configuración
    required = ["SFTP_HOST", "SFTP_USER", "SFTP_PASS", "SFTP_REMOTE_PATH"]
    missing = [k for k in required if k not in env_vars or not env_vars[k]]
    if missing:
        print(f"Error: Faltan variables en {env_path.name}: {', '.join(missing)}")
        sys.exit(1)

    host = env_vars["SFTP_HOST"]
    port = int(env_vars.get("SFTP_PORT", 22))
    user = env_vars["SFTP_USER"]
    password = env_vars["SFTP_PASS"]
    remote_base = env_vars["SFTP_REMOTE_PATH"].rstrip("/")

    print("=" * 60)
    print("SINCRONIZACIÓN SFTP")
    print("=" * 60)
    print(f"Host: {host}:{port}")
    print(f"Usuario: {user}")
    print(f"Destino: {remote_base}")
    print(f"Modo: {'DRY-RUN (sin cambios)' if args.dry_run else 'REAL'}")
    print(f"Eliminar huérfanos: {'No' if args.no_delete else 'Sí'}")
    print(f"Carpetas a sincronizar: {', '.join(INCLUDE_ONLY) if INCLUDE_ONLY else 'Todo'}")
    print("=" * 60)

    # Cargar patrones de exclusión
    ignore_patterns = ALWAYS_IGNORE + load_gitignore_patterns(SCRIPT_DIR)
    if args.verbose:
        print(f"\nPatrones de exclusión: {len(ignore_patterns)}")

    # Obtener archivos locales
    print("\n[1/4] Escaneando archivos locales...")
    local_files = get_local_files(SCRIPT_DIR, ignore_patterns, INCLUDE_ONLY)
    print(f"      Archivos a sincronizar: {len(local_files)}")

    # Conectar a SFTP
    print("\n[2/4] Conectando a SFTP...")
    transport = None
    sftp = None

    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print("      Conexión establecida")

        # Asegurar directorio base existe
        ensure_remote_dir(sftp, remote_base)

        # Obtener archivos remotos
        print("\n[3/4] Escaneando archivos remotos...")
        remote_files = get_remote_files(sftp, remote_base, "", INCLUDE_ONLY)
        print(f"      Archivos en servidor: {len(remote_files)}")

        # Calcular diferencias
        to_upload = local_files - remote_files
        to_update = local_files & remote_files
        to_delete = remote_files - local_files if not args.no_delete else set()

        print(f"\n[4/4] Sincronizando...")
        print(f"      Nuevos: {len(to_upload)}")
        print(f"      Actualizar: {len(to_update)}")
        print(f"      Eliminar: {len(to_delete)}")

        # Subir archivos nuevos
        uploaded = 0
        if to_upload:
            print(f"\n  Subiendo {len(to_upload)} archivos nuevos...")
            for rel_path in sorted(to_upload):
                local_path = SCRIPT_DIR / rel_path
                remote_path = f"{remote_base}/{rel_path}"

                if args.dry_run:
                    print(f"    [DRY] Subir: {rel_path}")
                else:
                    if sync_file(sftp, local_path, remote_path, args.verbose):
                        print(f"    [OK] {rel_path}")
                        uploaded += 1

        # Actualizar archivos existentes
        updated = 0
        if to_update:
            print(f"\n  Verificando {len(to_update)} archivos existentes...")
            for rel_path in sorted(to_update):
                local_path = SCRIPT_DIR / rel_path
                remote_path = f"{remote_base}/{rel_path}"

                if args.dry_run:
                    if args.verbose:
                        print(f"    [DRY] Verificar: {rel_path}")
                else:
                    if sync_file(sftp, local_path, remote_path, args.verbose):
                        print(f"    [UPD] {rel_path}")
                        updated += 1

        # Eliminar archivos huérfanos
        deleted = 0
        if to_delete:
            print(f"\n  Eliminando {len(to_delete)} archivos huérfanos...")
            for rel_path in sorted(to_delete):
                remote_path = f"{remote_base}/{rel_path}"

                if args.dry_run:
                    print(f"    [DRY] Eliminar: {rel_path}")
                else:
                    if delete_remote_file(sftp, remote_path):
                        print(f"    [DEL] {rel_path}")
                        deleted += 1

        # Resumen
        print("\n" + "=" * 60)
        print("RESUMEN")
        print("=" * 60)
        if args.dry_run:
            print("Modo DRY-RUN: No se realizaron cambios")
            print(f"  Se subirían: {len(to_upload)} archivos")
            print(f"  Se verificarían: {len(to_update)} archivos")
            print(f"  Se eliminarían: {len(to_delete)} archivos")
        else:
            print(f"  Subidos: {uploaded}")
            print(f"  Actualizados: {updated}")
            print(f"  Eliminados: {deleted}")
        print("=" * 60)

    except paramiko.AuthenticationException:
        print("Error: Autenticación fallida. Verifica usuario/contraseña.")
        sys.exit(1)
    except paramiko.SSHException as e:
        print(f"Error SSH: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


if __name__ == "__main__":
    main()
