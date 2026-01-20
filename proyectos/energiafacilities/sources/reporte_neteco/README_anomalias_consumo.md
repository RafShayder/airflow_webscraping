# Reporte de Anomalías de Consumo NetEco

## ¿Qué hace este reporte?

Detecta sitios que tienen un **consumo de energía inusualmente alto** comparado con su comportamiento histórico normal.

---

## ¿Cómo funciona?

### Paso 1: Calcular el consumo "normal" de cada sitio

Para cada sitio, tomamos los últimos **30 días** de datos y calculamos:

- **Mediana**: El valor "del medio" cuando ordenamos todos los consumos. Es más confiable que el promedio porque no se afecta por valores extremos.

**Ejemplo:**
```
Consumos de 7 días: 50, 52, 48, 51, 49, 200, 50 kWh
Promedio: 71.4 kWh  (el 200 lo distorsiona)
Mediana: 50 kWh     (el valor real típico)
```

### Paso 2: Verificar si el baseline es confiable

No todos los sitios tienen un consumo estable. Algunos están apagados varios días, otros tienen consumo muy variable. Para estos sitios, la mediana no es confiable.

Usamos el **Coeficiente de Variación (CV)** para medir la estabilidad:

```
CV = Desviación Estándar / Mediana
```

| CV | Significado | ¿Confiable? |
|----|-------------|-------------|
| < 0.3 | Muy estable | ✅ Sí |
| 0.3 - 0.5 | Estable | ✅ Sí |
| 0.5 - 1.0 | Variable | ⚠️ Dudoso |
| > 1.0 | Muy inestable | ❌ No |

**Ejemplo:**
```
Sitio A (estable):     10, 11, 9, 10, 12, 10 kWh → CV = 0.09 ✅
Sitio B (inestable):   0, 0, 50, 0, 80, 0 kWh   → CV = 1.5  ❌
```

Solo alertamos en sitios con **CV ≤ 0.5** (baseline confiable).

### Paso 3: Detectar anomalías

Comparamos el consumo actual contra la mediana:

```
Variación % = ((Consumo Actual - Mediana) / Mediana) × 100
```

**Ejemplo:**
```
Mediana: 50 kWh
Consumo hoy: 90 kWh
Variación: ((90 - 50) / 50) × 100 = 80%
```

Solo alertamos si la **variación ≥ 80%**.

---

## Filtros aplicados (resumen)

Para que un sitio aparezca en el reporte debe cumplir **TODAS** estas condiciones:

| Filtro | Valor por defecto | Descripción |
|--------|-------------------|-------------|
| Variación % | ≥ 80% | El consumo debe ser al menos 80% mayor que la mediana |
| CV máximo | ≤ 0.5 | El sitio debe tener un historial de consumo estable |
| Días de ventana | 30 | Se usan los últimos 30 días para calcular la mediana |
| Días recientes | 7 | Solo se reportan anomalías de los últimos 7 días |

---

## ¿Cómo cambiar los parámetros?

### Opción 1: Por línea de comandos

```bash
python reporte_anomalias_consumo.py \
    --env staging \
    --pct-threshold 0.50 \
    --cv-max 0.7 \
    --window-days 60 \
    --recent-days 14
```

### Opción 2: Desde código Python

```python
from sources.reporte_neteco.reporte_anomalias_consumo import run_reporte_anomalias_consumo

output = run_reporte_anomalias_consumo(
    env='staging',
    pct_threshold=0.50,   # Alertar si variación >= 50%
    cv_max=0.7,           # Aceptar sitios con CV hasta 0.7
    window_days=60,       # Usar 60 días de historia
    recent_days=14,       # Reportar últimos 14 días
)
```

### Parámetros disponibles

| Parámetro | Tipo | Default | Descripción |
|-----------|------|---------|-------------|
| `env` | string | dev | Entorno: dev, staging, prod |
| `pct_threshold` | decimal | 0.80 | Umbral de variación (0.80 = 80%) |
| `cv_max` | decimal | 0.50 | CV máximo para considerar baseline confiable |
| `window_days` | entero | 30 | Días de historia para calcular mediana |
| `recent_days` | entero | 7 | Días recientes a incluir en el reporte |
| `normal_streak` | entero | 3 | Días normales consecutivos para cerrar alerta |

---

## Ejemplos de ajustes

### Quiero detectar anomalías más pequeñas
```python
pct_threshold=0.50  # Alertar con 50% de variación (en vez de 80%)
```

### Quiero incluir sitios con consumo menos estable
```python
cv_max=0.8  # Aceptar sitios con CV hasta 0.8 (en vez de 0.5)
```

### Quiero usar más historia para el baseline
```python
window_days=60  # Usar 60 días (en vez de 30)
```

### Quiero ver anomalías de más días
```python
recent_days=30  # Últimos 30 días (en vez de 7)
```

---

## Columnas del reporte generado

| Columna | Descripción |
|---------|-------------|
| Site Name | Nombre del sitio |
| Fecha | Fecha del consumo anómalo |
| Consumo Diario kWh | Consumo registrado ese día |
| Mediana 30d | Consumo típico del sitio (últimos 30 días) |
| Std Dev 30d | Desviación estándar (qué tan variable es el consumo) |
| CV | Coeficiente de variación (estabilidad del baseline) |
| Variación % | Porcentaje de aumento respecto a la mediana |
| Ventana Inicio/Fin | Período usado para calcular la mediana |
| Muestras Ventana | Cantidad de días con datos en la ventana |
| consumo_dia_1..7 | Consumo de los últimos 7 días (para contexto) |

---

## ¿Por qué un sitio NO aparece en el reporte?

1. **CV muy alto** (> 0.5): El sitio tiene consumo muy variable o estuvo apagado varios días. No podemos determinar qué es "normal" para ese sitio.

2. **Variación menor a 80%**: El aumento de consumo no es suficientemente significativo.

3. **Sitio nuevo o con pocos datos**: No hay suficiente historia para calcular un baseline confiable.

4. **Ya volvió a la normalidad**: Si el sitio tuvo 3+ días consecutivos de consumo normal después de la anomalía, se considera "cerrada".
