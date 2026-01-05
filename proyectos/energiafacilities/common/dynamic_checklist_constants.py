"""
Constantes específicas para el workflow Dynamic Checklist.

Estas constantes definen índices, timeouts, textos, selectores y estados
utilizados en el proceso de scraping de Dynamic Checklist.
"""

# ========================================================================
# Índices y posiciones
# ========================================================================
MENU_INDEX_DYNAMIC_CHECKLIST = 5
MENU_INDEX_LOG_MANAGEMENT = 5
RADIO_BUTTON_LAST_MONTH_INDEX = 7
MIN_RADIO_BUTTONS_REQUIRED = 8

# ========================================================================
# Timeouts (segundos)
# ========================================================================
SPLITBUTTON_TIMEOUT = 30
LOADER_TIMEOUT = 60
DOWNLOAD_TIMEOUT = 300
DEFAULT_STATUS_POLL_INTERVAL = 30

# ========================================================================
# Sleeps (segundos)
# ========================================================================
SLEEP_AFTER_RADIO_CLICK = 2
SLEEP_AFTER_PROMPT_CLOSE = 2
SLEEP_AFTER_REFRESH = 2
SLEEP_AFTER_DOWNLOAD_CLICK = 3

# ========================================================================
# Textos y etiquetas
# ========================================================================
BUTTON_FILTER = "Filtrar"
BUTTON_FILTER_ALT = "Filter"
BUTTON_EXPORT = "Export sub WO detail"
BUTTON_REFRESH = "Refresh"
MENU_DYNAMIC_CHECKLIST = "Dynamic checklist"
MENU_LOG_MANAGEMENT = "Log Management"
SUBMENU_SUB_PM_QUERY = "Sub PM Query"
SUBMENU_DATA_EXPORT_LOGS = "Data Export Logs"
LABEL_SUB_PM_QUERY = "Sub PM Query"
LABEL_DATA_EXPORT_LOGS = "Data Export Logs"

# ========================================================================
# Estados de exportación
# ========================================================================
EXPORT_END_STATES = {"Succeed", "Failed", "Aborted", "Waiting", "Concurrent Waiting"}
EXPORT_SUCCESS_STATE = "Succeed"
EXPORT_RUNNING_STATE = "Running"

# ========================================================================
# Selectores CSS/XPath
# ========================================================================
CSS_RADIO_BUTTON = ".el-radio-button__inner"
CSS_SPLITBUTTON_TEXT = ".sdm_splitbutton_text"
XPATH_SPLITBUTTON_BY_TEXT = "//span[@class='sdm_splitbutton_text' and contains(text(),'{text}')]"
XPATH_PAGINATION_TOTAL = "//span[@class='el-pagination__total' and contains(text(),'Total')]"
XPATH_LOADING_MASK = "//div[contains(@class,'el-loading-mask')]"
XPATH_SUBMENU_SUB_PM_QUERY = "//span[@class='level-1 link-nav' and @title='Sub PM Query']"
XPATH_SUBMENU_DATA_EXPORT_LOGS = "//span[contains(text(),'Data Export Logs')]"
XPATH_EXPORT_ROW = "//tr[contains(.,'[check_list_mobile/check_list_mobile/custom_excel]')]"
XPATH_EXPORT_STATUS_CELL = ".//td[3]//span"
XPATH_DOWNLOAD_BUTTON = ".//td[11]//div[contains(@class,'export-operation-text') and contains(text(),'Download')]"
XPATH_CLOSE_PROMPT_BUTTON = "//button[@class='prompt-header-tool-btn keyboard-focus']//i[@class='prompt-header-close el-icon-close']"

