# Instalación de Docker CE (Offline)

Guía para instalar Docker CE en servidores RHEL 9.x sin conexión a Internet.

---

## Requisitos previos

- Servidor con **RHEL 9.x (x86_64)** sin conexión a Internet.
- Una máquina con Internet para descargar los paquetes.
- Acceso al servidor mediante SFTP.

---

## 1. Descargar los paquetes RPM en una máquina con Internet

```bash
mkdir -p ~/docker_rpms_x86_64
cd ~/docker_rpms_x86_64

curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/containerd.io-1.7.28-2.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-ce-28.0.0-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-ce-cli-28.0.0-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-buildx-plugin-0.29.1-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-compose-plugin-2.29.7-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/docker-ce-rootless-extras-28.0.0-1.el9.x86_64.rpm
curl -LO https://download.docker.com/linux/rhel/9/x86_64/stable/Packages/container-selinux-2.237.0-2.el9_6.noarch.rpm
```

---

## 2. Crear un paquete comprimido

```bash
cd ..
tar czf docker_rpms_x86_64.tar.gz docker_rpms_x86_64
```

Transferir el archivo al servidor:

```bash
scp docker_rpms_x86_64.tar.gz usuario@ip_servidor:/home/usuario/
```

---

## 3. Extraer los RPM en el servidor

```bash
cd ~
tar xzf docker_rpms_x86_64.tar.gz
cd docker_rpms_x86_64
ls -1
```

---

## 4. Instalar Docker CE desde los paquetes locales

```bash
sudo dnf install -y --disablerepo='*' --skip-broken ./*.rpm
```

---

## 5. Habilitar y arrancar Docker

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now docker
sudo systemctl status docker
```

---

## 6. Verificar la instalación

```bash
docker --version
sudo docker info
```

---

## 7. Limpieza opcional

```bash
sudo dnf clean all
rm -rf ~/docker_rpms_x86_64
```

---

## Cambiar la carpeta de datos de Docker

Si necesitas mover el almacenamiento de imágenes/volúmenes a otra ruta (por ejemplo, `/daas1/docker`), sigue estos pasos:

### 1. Detener Docker

```bash
sudo systemctl stop docker
sudo systemctl stop docker.socket
```

### 2. Verificar que `dockerd` no esté en ejecución

```bash
ps aux | grep dockerd
```

### 3. Crear el nuevo directorio

```bash
sudo mkdir -p /daas1/docker
sudo chown root:root /daas1/docker
sudo chmod 711 /daas1/docker
```

### 4. Configurar `data-root`

```bash
sudo vi /etc/docker/daemon.json
```

Contenido:

```json
{
  "data-root": "/daas1/docker"
}
```

### 5. Confirmar la configuración

```bash
sudo cat /etc/docker/daemon.json
```

### 6. Iniciar Docker nuevamente

```bash
sudo systemctl start docker
```
