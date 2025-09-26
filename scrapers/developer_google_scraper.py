# -*- coding: utf-8 -*-

"""
==================================================================================================
Web Scraper para el Blog de Google Developers (Search Central)
==================================================================================================
Autor: Gemini (actuando como Senior Python Developer)
Fecha: 25-09-2025
Versión: 1.0.0

Descripción:
Este script realiza un web scraping completo del blog de Google Search Central.
- Scraper Inicial: Extrae todos los artículos existentes, manejando la carga dinámica de contenido.
- Actualización Automática: Lógica para ejecutarse diariamente y añadir solo los nuevos posts.
- Manejo de Errores: Implementación robusta de reintentos, timeouts y logging detallado.
- Salida Estructurada: Guarda los datos en un archivo JSON bien formado.
- Anti-Baneo: Utiliza técnicas como rotación de User-Agents y delays aleatorios.
- Dinamismo: Maneja la paginación de tipo "click en botón 'Más'" utilizando Selenium.

Dependencias:
- beautifulsoup4
- selenium
- requests
- fake-useragent
- webdriver-manager

Instalación de dependencias:
pip install beautifulsoup4 selenium requests fake-useragent webdriver-manager
==================================================================================================
"""

import os
import json
import time
import random
import logging
import locale
from datetime import datetime
from shutil import copyfile
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURACIÓN GLOBAL Y CONSTANTES ---

BASE_URL = "https://developers.google.com/"

# URLs semilla con sus categorías normalizadas
SEED_URLS = {
    "blog": "https://developers.google.com/search/blog?hl=es",
    "news": "https://developers.google.com/search/news?hl=es",
}

# Selectores CSS robustos
SELECTORS = {
    'article_card': 'div.devsite-card-wrapper',
    'link': 'a.devsite-card-image-container, a[aria-label*="-"]', # Prioriza el link del título
    'title': 'h3.no-link',
    'thumbnail': 'img.devsite-card-image',
    'excerpt': 'p.devsite-card-summary',
    'date_from_card': 'p.devsite-card-attribution-date',
    'load_more_button': 'button.devsite-pagination-more-button',
    'content_container': 'div.devsite-article-body',
}

# Configuración de archivos y directorios
LOG_DIR = "logs"
LOG_FILENAME_FORMAT = "developer_google_scraper_{}.log"
JSON_FILE_PATH = "data/developer_google_database.json"
JSON_BACKUP_PATH = JSON_FILE_PATH + ".bak"

# Configuración de reintentos y delays
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2
RANDOM_DELAY_RANGE = (1, 3)

# --- CLASE PRINCIPAL DEL SCRAPER ---

class GoogleDeveloperScraper:
    def __init__(self):
        self._setup_logging()
        self.user_agent = UserAgent()
        self.session = self._get_requests_session()
        self.driver = self._setup_selenium_driver()
        try:
            locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
        except locale.Error:
            self.logger.warning("Locale 'es_ES.UTF-8' no encontrado. El parseo de fechas puede fallar.")
            locale.setlocale(locale.LC_TIME, '')

    def _setup_logging(self):
        if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
        log_file = os.path.join(LOG_DIR, LOG_FILENAME_FORMAT.format(datetime.now().strftime("%Y-%m-%d")))
        self.logger = logging.getLogger("GoogleDeveloperScraper")
        self.logger.setLevel(logging.INFO)
        if self.logger.hasHandlers(): self.logger.handlers.clear()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def _get_requests_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({'User-Agent': self.user_agent.random})
        return session

    def _setup_selenium_driver(self) -> webdriver.Chrome:
        self.logger.info("Configurando WebDriver de Selenium...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_argument(f"user-agent={self.user_agent.random}")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(45)
        return driver

    def _load_existing_data(self) -> dict:
        if os.path.exists(JSON_FILE_PATH):
            self.logger.info(f"Cargando datos existentes desde {JSON_FILE_PATH}")
            with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"titulo_sitio_web": "Developer Google", "articles": []}

    def _save_data(self, data: dict):
        self.logger.info(f"Guardando {len(data['articles'])} artículos en {JSON_FILE_PATH}")
        with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def _backup_database(self):
        if os.path.exists(JSON_FILE_PATH):
            try:
                copyfile(JSON_FILE_PATH, JSON_BACKUP_PATH)
                self.logger.info(f"Backup creado exitosamente en {JSON_BACKUP_PATH}")
            except Exception as e:
                self.logger.error(f"No se pudo crear el backup del archivo JSON: {e}")

    def _parse_date(self, date_str: str) -> str:
        try:
            # Formato: "18 de septiembre de 2025"
            date_obj = datetime.strptime(date_str.strip(), "%d de %B de %Y")
            return date_obj.strftime("%d-%m-%Y")
        except ValueError:
            self.logger.warning(f"Formato de fecha inesperado: '{date_str}'. Se guardará tal cual.")
            return date_str

    def _clean_html_content(self, content_soup: BeautifulSoup) -> str:
        allowed_tags = ['h2', 'h3', 'h4', 'h5', 'h6', 'p', 'figure', 'img', 'table', 'code', 'blockquote', 'a', 'ul', 'ol', 'li']
        content_parts = []
        for element in content_soup.find_all(True, recursive=True):
            if element.name in allowed_tags:
                if hasattr(element, 'attrs'):
                    allowed_attrs = ['href', 'src', 'alt', 'title']
                    attrs = dict(element.attrs)
                    for attr in attrs:
                        if attr not in allowed_attrs:
                            del element.attrs[attr]
                content_parts.append(str(element))
        return "\n".join(content_parts)

    def _fetch_article_content(self, article_url: str) -> str:
        self.logger.info(f"Scrapeando contenido del artículo: {article_url}")
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(random.uniform(*RANDOM_DELAY_RANGE))
                response = self.session.get(article_url, timeout=15)
                self.logger.info(f"GET {article_url} - Status: {response.status_code}")
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                content_container = soup.select_one(SELECTORS['content_container'])
                return self._clean_html_content(content_container) if content_container else ""
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Intento {attempt + 1}/{MAX_RETRIES} fallido para {article_url}: {e}")
                if attempt + 1 == MAX_RETRIES:
                    self.logger.error(f"No se pudo obtener la URL {article_url} después de {MAX_RETRIES} intentos.")
                    return ""
                time.sleep(RETRY_BACKOFF_FACTOR ** attempt)
        return ""

    def _scrape_category_page(self, category_url: str) -> str:
        self.logger.info(f"Abriendo página de categoría con Selenium: {category_url}")
        self.driver.get(category_url)
        
        while True:
            try:
                time.sleep(3) # Esperar a que la UI se estabilice
                # Puede haber varios botones "Más", los presionamos todos
                load_more_buttons = self.driver.find_elements(By.CSS_SELECTOR, SELECTORS['load_more_button'])
                
                clicked_any = False
                for button in load_more_buttons:
                    if button.is_displayed() and button.is_enabled():
                        self.driver.execute_script("arguments[0].click();", button)
                        self.logger.info("Botón 'Más' presionado.")
                        clicked_any = True
                        time.sleep(2) # Pequeña pausa entre clicks
                
                if not clicked_any:
                    self.logger.info("No se encontraron más botones 'Más' activos. Contenido cargado.")
                    break
            except (NoSuchElementException, TimeoutException, ElementNotInteractableException):
                self.logger.info("No se encontraron más botones 'Más'. Se asume que todo el contenido está cargado.")
                break
            except Exception as e:
                self.logger.error(f"Error inesperado al hacer clic en 'Más': {e}")
                break
        
        return self.driver.page_source

    def run_initial_scrape(self):
        self.logger.info("========== INICIANDO SCRAPING INICIAL ==========")
        all_articles = []
        processed_links = set()

        for category, url in SEED_URLS.items():
            self.logger.info(f"--- Procesando categoría: {category} ---")
            page_source = self._scrape_category_page(url)
            soup = BeautifulSoup(page_source, 'html.parser')
            article_cards = soup.select(SELECTORS['article_card'])
            self.logger.info(f"Se encontraron {len(article_cards)} tarjetas de artículo en la categoría '{category}'.")

            for card in article_cards:
                try:
                    link_tag = card.select_one(SELECTORS['link'])
                    if not link_tag or not link_tag.get('href'): continue

                    article_link = urljoin(BASE_URL, link_tag['href'])
                    if article_link in processed_links: continue
                    processed_links.add(article_link)

                    title_tag = card.select_one(SELECTORS['title'])
                    article_title = title_tag.get_text(strip=True) if title_tag else "Sin Título"
                    
                    thumb_tag = card.select_one(SELECTORS['thumbnail'])
                    excerpt_tag = card.select_one(SELECTORS['excerpt'])
                    date_tag = card.select_one(SELECTORS['date_from_card'])

                    full_content = self._fetch_article_content(article_link)
                    if not full_content:
                        self.logger.warning(f"No se pudo obtener contenido para '{article_title}'. Saltando.")
                        continue

                    article_data = {
                        "category": category,
                        "link": article_link,
                        "titulo_entrada": article_title,
                        "url_thumbnail": thumb_tag.get('src', '') if thumb_tag else '',
                        "full_content": full_content,
                        "excerpt_content": excerpt_tag.get_text(strip=True) if excerpt_tag else self._create_excerpt(full_content),
                        "date": self._parse_date(date_tag.get_text(strip=True)) if date_tag else '',
                        "favicon_url": "https://www.gstatic.com/devrel-devsite/prod/vc12d84b6edb3e25e1619b575cf813e1849a1c95098b711a6c56ab3968c9a4fa9/developers/images/favicon-new.png"
                    }
                    all_articles.append(article_data)
                    self.logger.info(f"Artículo procesado: '{article_title}'")

                except Exception as e:
                    self.logger.error(f"Error procesando una tarjeta de artículo: {e}", exc_info=True)
        
        final_data = {"titulo_sitio_web": "Developer Google", "articles": all_articles}
        self._save_data(final_data)
        self.logger.info("========== SCRAPING INICIAL COMPLETADO ==========")

    def run_update_scrape(self):
        self.logger.info("========== INICIANDO SCRAPING DE ACTUALIZACIÓN ==========")
        self._backup_database()
        existing_data = self._load_existing_data()
        existing_urls = {article['link'] for article in existing_data['articles']}
        self.logger.info(f"Base de datos actual contiene {len(existing_urls)} artículos.")
        new_articles_found = []
        
        for category, url in SEED_URLS.items():
            self.logger.info(f"--- Verificando nuevos artículos en categoría: {category} ---")
            try:
                self.driver.get(url)
                time.sleep(5) # Esperar carga inicial
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            except Exception as e:
                self.logger.error(f"No se pudo cargar la página de categoría {url}: {e}")
                continue

            article_cards = soup.select(SELECTORS['article_card'])
            if not article_cards: continue

            for card in article_cards[:5]:
                link_tag = card.select_one(SELECTORS['link'])
                if not link_tag or not link_tag.get('href'): continue
                
                article_link = urljoin(BASE_URL, link_tag['href'])
                
                if article_link not in existing_urls:
                    self.logger.info(f"¡Nuevo artículo encontrado!: {article_link}")
                    try:
                        title_tag = card.select_one(SELECTORS['title'])
                        article_title = title_tag.get_text(strip=True) if title_tag else "Sin Título"
                        thumb_tag = card.select_one(SELECTORS['thumbnail'])
                        excerpt_tag = card.select_one(SELECTORS['excerpt'])
                        date_tag = card.select_one(SELECTORS['date_from_card'])
                        
                        full_content = self._fetch_article_content(article_link)
                        if not full_content: continue

                        new_article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": thumb_tag.get('src', '') if thumb_tag else '',
                            "full_content": full_content,
                            "excerpt_content": excerpt_tag.get_text(strip=True) if excerpt_tag else self._create_excerpt(full_content),
                            "date": self._parse_date(date_tag.get_text(strip=True)) if date_tag else '',
                            "favicon_url": "https://www.gstatic.com/devrel-devsite/prod/vc12d84b6edb3e25e1619b575cf813e1849a1c95098b711a6c56ab3968c9a4fa9/developers/images/favicon-new.png"
                        }
                        new_articles_found.append(new_article_data)
                        existing_urls.add(article_link)
                    except Exception as e:
                        self.logger.error(f"Error procesando el nuevo artículo {article_link}: {e}", exc_info=True)
                else:
                    self.logger.info(f"Artículo ya existente encontrado en '{category}'. Pasando a la siguiente categoría.")
                    break
        
        if new_articles_found:
            self.logger.info(f"Se agregarán {len(new_articles_found)} nuevos artículos a la base de datos.")
            existing_data['articles'] = new_articles_found + existing_data['articles']
            self._save_data(existing_data)
        else:
            self.logger.info("No se encontraron nuevos artículos en esta ejecución.")
        self.logger.info("========== SCRAPING DE ACTUALIZACIÓN COMPLETADO ==========")

    def close(self):
        if self.driver:
            self.driver.quit()
            self.logger.info("WebDriver de Selenium cerrado.")

def main():
    scraper = GoogleDeveloperScraper()
    try:
        if not os.path.exists(JSON_FILE_PATH):
            scraper.run_initial_scrape()
        else:
            scraper.run_update_scrape()
    except Exception as e:
        scraper.logger.critical(f"Ha ocurrido un error fatal en el scraper: {e}", exc_info=True)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()