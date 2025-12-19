from core.utils import load_config
from clients.auth_new import AuthManager, LoginSelectors
from selenium.webdriver.common.by import By
from selenium import webdriver

from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--allow-insecure-localhost")
chrome_options.add_argument("--ignore-ssl-errors=yes")

driver = webdriver.Chrome(options=chrome_options)



neteco_link="https://10.125.129.82:31943"
config = load_config()
config_neteco = config.get("neteco", {})

usuario=config_neteco["username"]
password=config_neteco["password"]


selectors = LoginSelectors(
    username=(By.ID, "username"),
    password=(By.ID, "value"),
    submit=(By.ID, "submitDataverify"),
)

auth = AuthManager(
    driver=driver,
    login_url="https://10.125.129.82:31943",
    success_url_contains="https://10.125.129.82:31943/netecodashboard/assets/build/index.html#/dashboard/overview",
    selectors=selectors,
)

auth.login(usuario, password)

''' Navegamos a los filtros'''

classname = 'nco-search-container-buttons'

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
wait = WebDriverWait(driver, 30)

filtro = wait.until(
    EC.presence_of_element_located((By.CLASS_NAME, classname))
)

filtro.click()
input("Presiona Enter para continuar...")