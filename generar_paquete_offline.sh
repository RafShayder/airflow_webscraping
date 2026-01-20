#!/bin/bash

# Genera un único paquete offline con la imagen personalizada y dependencias oficiales.
# Produce scraper-integratel-offline.tar.gz listo para importar en servidores sin Internet.

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

TARGET_PLATFORM="${TARGET_PLATFORM:-linux/amd64}"
TARGET_OS="${TARGET_PLATFORM%%/*}"
TARGET_ARCH="${TARGET_PLATFORM##*/}"
IMAGE_NAME="${IMAGE_NAME:-scraper-integratel}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
APP_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"
ARCHIVE_NAME="${ARCHIVE_NAME:-scraper-integratel-offline.tar.gz}"
DEPENDENCIES=(
    "postgres:16"
    "redis:7.2-bookworm"
    "prom/statsd-exporter:latest"
    "prom/prometheus:latest"
)

export DOCKER_DEFAULT_PLATFORM="${TARGET_PLATFORM}"

print_header() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  Generador de paquete offline${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "Plataforma objetivo: ${YELLOW}${TARGET_PLATFORM}${NC}"
    echo -e "Imagen de aplicación: ${YELLOW}${APP_IMAGE}${NC}"
    echo -e "Archivo de salida   : ${YELLOW}${ARCHIVE_NAME}${NC}"
    echo ""
}

ensure_buildx() {
    if ! docker buildx ls >/dev/null 2>&1; then
        echo -e "${RED}✗ docker buildx no está disponible. Actualiza Docker Desktop/Engine.${NC}"
        exit 1
    fi

    if ! docker buildx ls | grep -q '\*'; then
        echo -e "${YELLOW}No hay builder activo. Creando uno temporal...${NC}"
        docker buildx create --use --name tmp-buildx >/dev/null
    fi
}

build_app_image() {
    if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
        echo -e "${YELLOW}[1/5] Omitiendo build, se reutilizará ${APP_IMAGE}${NC}"
        return
    fi

    echo -e "${YELLOW}[1/5] Construyendo ${APP_IMAGE} para ${TARGET_PLATFORM}...${NC}"
    docker buildx build \
        --platform "${TARGET_PLATFORM}" \
        -t "${APP_IMAGE}" \
        -f Dockerfile \
        --load \
        .

    local arch
    arch=$(docker image inspect "${APP_IMAGE}" --format '{{.Architecture}}')
    if [[ "${arch}" != "${TARGET_ARCH}" ]]; then
        echo -e "${RED}✗ La imagen generada es ${arch}; se esperaba ${TARGET_ARCH}.${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Imagen construida correctamente (${arch})${NC}"
    echo ""
}

check_arch() {
    local image="$1"
    docker image inspect "$image" --format '{{.Architecture}}' 2>/dev/null || echo "desconocida"
}

resolve_digest() {
    local image="$1"
    if ! command -v python3 >/dev/null 2>&1; then
        echo -e "${RED}✗ python3 no está disponible para resolver el digest de ${image}.${NC}" >&2
        return 1
    fi

    local manifest_json
    if ! manifest_json=$(docker manifest inspect "$image" 2>/dev/null); then
        echo -e "${RED}✗ No se pudo obtener el manifest remoto de ${image}.${NC}" >&2
        return 1
    fi

    python3 - "$TARGET_OS" "$TARGET_ARCH" "$manifest_json" <<'PY'
import json
import sys

os_name = sys.argv[1]
arch = sys.argv[2]
payload = sys.argv[3]

try:
    data = json.loads(payload)
except json.JSONDecodeError as exc:
    print(f"Error leyendo manifest: {exc}", file=sys.stderr)
    sys.exit(1)

for manifest in data.get("manifests", []):
    platform = manifest.get("platform", {})
    if platform.get("os") == os_name and platform.get("architecture") == arch:
        print(manifest["digest"])
        sys.exit(0)

print(f"No se encontró digest para {os_name}/{arch}", file=sys.stderr)
sys.exit(1)
PY
}

ensure_dependency() {
    local image="$1"

    if docker image inspect "$image" >/dev/null 2>&1; then
        local arch
        arch=$(check_arch "$image")
        if [[ "$arch" == "$TARGET_ARCH" ]]; then
            echo -e "✓ ${image} (${arch})"
            return
        fi

        echo -e "${YELLOW}↺ ${image} está en arquitectura ${arch}. Se descargará la variante ${TARGET_ARCH}.${NC}"
        docker image rm "$image" >/dev/null 2>&1 || true
    else
        echo -e "${YELLOW}✗ ${image} no está disponible localmente. Se descargará.${NC}"
    fi

    local reference="${image%%@*}"
    local digest
    digest=$(resolve_digest "$reference") || {
        echo -e "${RED}✗ No se pudo resolver el digest para ${image}.${NC}"
        exit 1
    }

    echo -e "  → Descargando ${reference}@${digest}"
    docker pull "${reference}@${digest}" >/dev/null
    docker tag "${reference}@${digest}" "$image"
    echo -e "✓ ${image} (${TARGET_ARCH})"
}

ensure_dependencies() {
    echo -e "${YELLOW}[2/5] Preparando imágenes oficiales (${TARGET_PLATFORM})...${NC}"
    echo ""
    for image in "${DEPENDENCIES[@]}"; do
        ensure_dependency "$image"
    done
    echo ""
}

verify_images() {
    echo -e "${YELLOW}[3/5] Verificando arquitecturas...${NC}"
    echo ""

    local images=("${APP_IMAGE}" "${DEPENDENCIES[@]}")
    for image in "${images[@]}"; do
        local arch
        arch=$(check_arch "$image")
        if [[ "$arch" != "${TARGET_ARCH}" ]]; then
            echo -e "${RED}✗ ${image} quedó en ${arch}.${NC}"
            exit 1
        fi
        echo -e "✓ ${image} -> ${arch}"
    done

    echo ""
}

create_archive() {
    echo -e "${YELLOW}[4/5] Generando ${ARCHIVE_NAME}...${NC}"
    echo ""

    rm -f "${ARCHIVE_NAME}"
    docker save \
        "${APP_IMAGE}" \
        "${DEPENDENCIES[@]}" | gzip > "${ARCHIVE_NAME}"

    local size
    size=$(du -h "${ARCHIVE_NAME}" | cut -f1)
    echo -e "${GREEN}✓ Archivo creado (${size})${NC}"
    echo ""
}

print_summary() {
    echo -e "${YELLOW}[5/5] Paquete listo${NC}"
    echo ""
    echo "Archivo: ${ARCHIVE_NAME}"
    echo "Plataforma: ${TARGET_PLATFORM}"
    echo ""
    echo "Pasos en el servidor:"
    echo "  1. Copia el archivo: scp ${ARCHIVE_NAME} usuario@servidor:/ruta/"
    echo "  2. Importa la imagen: sudo gunzip -c ${ARCHIVE_NAME} | sudo docker load"
    echo "  3. Verifica: sudo docker image inspect ${APP_IMAGE} --format '{{.Architecture}}'"
    echo "  4. Levanta servicios: sudo docker compose up -d --pull never"
    echo ""
    echo -e "${GREEN}Proceso completado correctamente.${NC}"
}

main() {
    print_header
    ensure_buildx
    build_app_image
    ensure_dependencies
    verify_images
    create_archive
    print_summary
}

main "$@"
