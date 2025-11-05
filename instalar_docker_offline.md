# Instalación Offline de Docker CE en Servidor RHEL (sin conexión a red)

Este documento describe el procedimiento para **instalar Docker CE en un
servidor Red Hat Enterprise Linux 9 (x86_64)** que **no tiene conexión a
Internet**, utilizando paquetes `.rpm` descargados previamente en otra
máquina con acceso a red (por ejemplo, una Mac).

## 📋 Requisitos previos

-   Servidor con **RHEL 9.x (x86_64)** sin conexión a Internet.
-   Una máquina con Internet para descargar los
    paquetes.
-   Acceso al servidor mediante SFTP

## 1️⃣ Descargar los paquetes RPM en una máquina con Internet

``` bash
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

## 2️⃣ Crear un paquete comprimido

``` bash
cd ..
tar czf docker_rpms_x86_64.tar.gz docker_rpms_x86_64
```

Transferir el archivo al servidor:

``` bash
scp docker_rpms_x86_64.tar.gz usuario@ip_servidor:/home/usuario/
```

## 3️⃣ Extraer los RPM en el servidor

``` bash
cd ~
tar xzf docker_rpms_x86_64.tar.gz
cd docker_rpms_x86_64
ls -1
```

## 4️⃣ Instalar Docker CE desde los paquetes locales

``` bash
sudo dnf install -y --disablerepo='*' --skip-broken ./*.rpm
```

## 5️⃣ Habilitar y arrancar Docker

``` bash
sudo systemctl daemon-reload
sudo systemctl enable --now docker
sudo systemctl status docker
```

## 6️⃣ Verificar la instalación

``` bash
docker --version
sudo docker info
```

## 7️⃣ Limpieza opcional

``` bash
sudo dnf clean all
rm -rf ~/docker_rpms_x86_64
```
