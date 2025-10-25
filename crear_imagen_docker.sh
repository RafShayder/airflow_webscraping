#!/bin/bash

# Script para crear, exportar e importar imagen Docker sin necesidad de internet
set -e

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Variables
IMAGE_NAME="scraper-integratel"
VERSION="latest"
ARCHIVE_NAME="scraper-integratel-docker-image.tar"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Docker Image Builder Script${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Función para construir la imagen
build_image() {
    echo -e "${YELLOW}[1/4] Construyendo imagen Docker...${NC}"
    docker build -t ${IMAGE_NAME}:${VERSION} -f Dockerfile .
    echo -e "${GREEN}✓ Imagen construida exitosamente: ${IMAGE_NAME}:${VERSION}${NC}"
    echo ""
}

# Función para exportar la imagen a un archivo tar
export_image() {
    echo -e "${YELLOW}[2/4] Exportando imagen a archivo tar...${NC}"
    docker save ${IMAGE_NAME}:${VERSION} | gzip > ${ARCHIVE_NAME}.gz
    echo -e "${GREEN}✓ Imagen exportada exitosamente a: ${ARCHIVE_NAME}.gz${NC}"
    
    FILE_SIZE=$(du -h ${ARCHIVE_NAME}.gz | cut -f1)
    echo -e "${GREEN}  Tamaño del archivo: ${FILE_SIZE}${NC}"
    echo ""
}

# Función para mostrar instrucciones de importación
show_import_instructions() {
    echo -e "${GREEN}Instrucciones para importar en servidor de producción:${NC}"
    echo ""
    echo "1. Copiar el archivo al servidor:"
    echo "   ${YELLOW}scp ${ARCHIVE_NAME}.gz usuario@servidor:/ruta/destino/${NC}"
    echo ""
    echo "2. En el servidor, importar la imagen:"
    echo "   ${YELLOW}docker load < ${ARCHIVE_NAME}.gz${NC}"
    echo ""
    echo "3. Verificar que la imagen se importó:"
    echo "   ${YELLOW}docker images | grep ${IMAGE_NAME}${NC}"
}

# Ejecutar todo
build_image
export_image
show_import_instructions
