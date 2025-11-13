# Plan de Migración: GDE a Enfoque OOP

## Objetivo
Migrar `autin_gde/stractor.py` de enfoque funcional a orientado a objetos, siguiendo el patrón establecido en `autin_checklist/stractor.py` para mantener consistencia y mejorar mantenibilidad.

---

## Estructura Actual vs Propuesta

### Estructura Actual (Funcional)
```python
# Funciones libres
def run_gde(config, ...) -> Path:
    # Setup browser
    # Login
    # Aplicar filtros
    # Exportar
    # Descargar

def extraer_gde(config=None, ...) -> str:
    # Wrapper que llama a run_gde()
```

### Estructura Propuesta (OOP)
```python
class GDEWorkflow:
    def __init__(self, config, ...):
        # Inicializar managers y estado
        
    def run(self) -> Path:
        # Flujo principal
        
    def _authenticate_and_navigate(self) -> None:
        # Login y navegación
        
    def _apply_filters(self) -> None:
        # Aplicar filtros
        
    def _export_and_download(self) -> Path:
        # Exportar y descargar
        
    def close(self) -> None:
        # Cleanup
```

---

## Plan de Migración Paso a Paso

### Paso 1: Crear la clase `GDEWorkflow`

**Ubicación:** Después de `GDEConfig`, antes de las funciones helper

```python
class GDEWorkflow:
    """
    Implementa el flujo completo de GDE.
    
    Componentes clave utilizados:
        - ``BrowserManager``: instancia del driver de Selenium con rutas de descarga.
        - ``AuthManager``: login al portal Integratel.
        - ``IframeManager``: cambio entre iframes para acceder a filtros y Export Status.
        - ``FilterManager``: apertura/esperas del panel de filtros.
        - ``DateFilterManager``: aplicación de filtros de fecha.
        - Helpers locales: encapsulan interacciones específicas de la UI.
    """
    
    def __init__(
        self,
        config: GDEConfig,
        *,
        headless: Optional[bool] = None,
        chrome_extra_args: Optional[Iterable[str]] = None,
        status_timeout: Optional[int] = None,
        status_poll_interval: Optional[int] = None,
        output_filename: Optional[str] = None,
    ) -> None:
        self.config = config
        self.status_timeout = status_timeout or config.max_status_attempts * 30
        self.status_poll_interval = status_poll_interval or 8
        self.desired_filename = (output_filename or config.gde_output_filename or "").strip() or None
        self.download_dir = Path(config.download_path).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self._overwrite_files = config.export_overwrite_files
        self.export_started = None  # Se inicializa en _export_and_download()
        
        # Inicializar managers
        self.browser_manager = self._setup_browser(headless, chrome_extra_args)
        self.driver, self.wait = self.browser_manager.create_driver()
        self.iframe_manager = IframeManager(self.driver)
        self.filter_manager = FilterManager(self.driver, self.wait)
        self.date_filter_manager = DateFilterManager(self.driver, self.wait)
    
    def _setup_browser(
        self, headless: Optional[bool], chrome_extra_args: Optional[Iterable[str]]
    ) -> BrowserManager:
        """Configura y crea la instancia de BrowserManager con manejo de proxy."""
        return setup_browser_with_proxy(
            download_path=str(self.download_dir),
            proxy=self.config.proxy,
            headless=self.config.headless if headless is None else headless,
            chrome_extra_args=chrome_extra_args,
        )
    
    def run(self) -> Path:
        """Ejecuta el flujo completo de GDE."""
        self._authenticate_and_navigate()
        self._apply_filters()
        return self._export_and_download()
    
    def _authenticate_and_navigate(self) -> None:
        """Autentica y navega al módulo GDE."""
        auth_manager = AuthManager(self.driver)
        require(
            auth_manager.login(self.config.username, self.config.password),
            "No se pudo realizar el login",
        )
        require(
            self.iframe_manager.find_main_iframe(max_attempts=self.config.max_iframe_attempts),
            "No se encontró el iframe principal",
        )
        
        self.filter_manager.wait_for_filters_ready()
        self.filter_manager.open_filter_panel(method="complex")
    
    def _apply_filters(self) -> None:
        """Aplica los filtros necesarios para la consulta."""
        _click_clear_filters(self.driver, self.wait)
        _apply_task_type_filters(self.driver, self.wait, self.config.options_to_select)
        
        # Aplicar filtros de fecha según date_mode
        if self.config.date_mode == 1:
            _apply_gde_manual_dates(self.driver, self.wait, self.config)
        else:
            self.date_filter_manager.apply_date_filters(self.config)
        
        _apply_filters(self.driver)
    
    def _export_and_download(self) -> Path:
        """Ejecuta la exportación y maneja la descarga del archivo."""
        self.export_started = _trigger_export(self.driver)
        
        _navigate_to_export_status(self.iframe_manager)
        _monitor_status(
            self.driver,
            self.status_timeout,
            self.status_poll_interval,
        )
        
        downloaded = _download_export(
            self.driver,
            self.download_dir,
            self.export_started,
            overwrite_files=self._overwrite_files,
            output_filename=self.desired_filename,
        )
        
        logger.info("Flujo GDE completado")
        return downloaded
    
    def close(self) -> None:
        """Cierra el navegador."""
        logger.debug("Cerrando navegador...")
        self.browser_manager.close_driver()
```

### Paso 2: Refactorizar `run_gde()` para usar la clase

**Antes:**
```python
def run_gde(config, ...) -> Path:
    # 50+ líneas de código
    download_dir = Path(config.download_path).resolve()
    browser_manager = setup_browser_with_proxy(...)
    driver, wait = browser_manager.create_driver()
    try:
        # Todo el flujo aquí
    finally:
        browser_manager.close_driver()
```

**Después:**
```python
def run_gde(
    config: GDEConfig,
    *,
    headless: Optional[bool] = None,
    chrome_extra_args: Optional[Iterable[str]] = None,
    status_timeout: Optional[int] = None,
    status_poll_interval: Optional[int] = None,
    output_filename: Optional[str] = None,
) -> Path:
    """
    Ejecuta el flujo completo de exportación para GDE y devuelve la ruta del archivo descargado.
    
    (Mantener docstring original)
    """
    workflow = GDEWorkflow(
        config=config,
        headless=headless,
        chrome_extra_args=chrome_extra_args,
        status_timeout=status_timeout,
        status_poll_interval=status_poll_interval,
        output_filename=output_filename,
    )
    try:
        return workflow.run()
    finally:
        workflow.close()
```

### Paso 3: Mantener `extraer_gde()` sin cambios

La función `extraer_gde()` sigue siendo el punto de entrada público y no necesita cambios, ya que solo llama a `run_gde()`.

---

## Beneficios de la Migración

### 1. **Consistencia con Checklist**
- Mismo patrón arquitectónico en ambos módulos
- Más fácil de mantener y entender

### 2. **Mejor Encapsulación**
- Estado encapsulado en la clase
- Managers como atributos de instancia (más fácil de acceder)

### 3. **Mejor Testabilidad**
- Métodos aislados más fáciles de testear
- Mock de managers más sencillo

### 4. **Mejor Organización**
- Métodos agrupados por responsabilidad
- Flujo principal claro en `run()`

### 5. **Reutilización**
- Instancia de workflow puede reutilizarse
- Más fácil extender funcionalidad

---

## Cambios en Funciones Helper

### Funciones que NO cambian (siguen siendo libres)
- `_click_clear_filters()`
- `_apply_task_type_filters()`
- `_apply_filters()`
- `_click_filter_button()`
- `_wait_for_filters_to_apply()`
- `_robust_click()`
- `_switch_to_frame_with()`
- `_click_y_setear_fecha()`
- `_confirmar_selector_fecha()`
- `_apply_gde_manual_dates()`
- `_resolve_manual_date_range()`
- `_handle_filter_warning()`
- `_select_first_grid_row()`
- `_click_export_button()`
- `_trigger_export()`
- `_navigate_to_export_status()`
- `_monitor_status()`
- `_download_export()`

**Razón:** Son funciones puras o helpers que no necesitan estado de instancia.

### Funciones que SÍ cambian (métodos de clase)
- `run_gde()` → `GDEWorkflow.run()`
- Setup browser → `GDEWorkflow._setup_browser()`
- Login/navegación → `GDEWorkflow._authenticate_and_navigate()`
- Aplicar filtros → `GDEWorkflow._apply_filters()`
- Exportar/descargar → `GDEWorkflow._export_and_download()`

---

## Orden de Implementación Recomendado

1. ✅ **Crear clase `GDEWorkflow`** con `__init__()` y `_setup_browser()`
2. ✅ **Mover lógica de `run_gde()`** a métodos de clase
3. ✅ **Refactorizar `run_gde()`** para usar la clase
4. ✅ **Verificar que `extraer_gde()` sigue funcionando**
5. ✅ **Probar ejecución local**
6. ✅ **Verificar compatibilidad con DAGs de Airflow**

---

## Ejemplo de Uso Final

### Uso desde código externo (sin cambios)
```python
# Sigue funcionando igual
from energiafacilities import extraer_gde

file_path = extraer_gde(
    env="dev",
    overrides={"proxy": None},
    headless=False
)
```

### Uso directo de la clase (nuevo)
```python
from energiafacilities.sources.autin_gde.stractor import GDEWorkflow, GDEConfig

config = GDEConfig.from_yaml_config(env="dev")
workflow = GDEWorkflow(config, headless=False)
try:
    file_path = workflow.run()
finally:
    workflow.close()
```

---

## Consideraciones Importantes

### 1. **Compatibilidad hacia atrás**
- `extraer_gde()` mantiene su firma actual
- Los DAGs de Airflow no necesitan cambios
- Ejecución local sigue funcionando igual

### 2. **Funciones helper**
- Se mantienen como funciones libres (no métodos)
- Pueden acceder a `self.driver`, `self.wait` pasándolos como parámetros
- Alternativa: Convertirlas a métodos privados si se usan mucho

### 3. **Manejo de errores**
- Mantener `require()` y `logger.debug` como está
- Las excepciones siguen propagándose correctamente

### 4. **Testing**
- Más fácil crear mocks de managers
- Métodos más pequeños y enfocados

---

## Resumen

La migración es **segura y no rompe compatibilidad** porque:
- ✅ La función pública `extraer_gde()` mantiene su interfaz
- ✅ Los DAGs siguen funcionando sin cambios
- ✅ Solo refactorizamos código interno
- ✅ Mejoramos organización y mantenibilidad

¿Proceder con la implementación?

