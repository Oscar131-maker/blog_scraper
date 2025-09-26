# -*- coding: utf-8 -*-

"""
==================================================================================================
Web Scraper para el Blog de Carlos Sánchez
==================================================================================================
Autor: Gemini (actuando como Senior Python Developer)
Fecha: 25-09-2025
Versión: 1.0.0

Descripción:
Este script realiza un web scraping completo del blog de Carlos Sánchez Donate.
Ha sido diseñado con las siguientes características clave:
- Scraper Inicial: Extrae todos los artículos existentes desde URLs semilla de categorías.
- Actualización Automática: Lógica para ejecutarse diariamente y añadir solo los nuevos posts.
- Manejo de Errores: Implementación robusta de reintentos, timeouts y logging detallado.
- Salida Estructurada: Guarda los datos en un archivo JSON bien formado.
- Anti-Baneo: Utiliza técnicas como rotación de User-Agents y delays aleatorios.
- Dinamismo: Maneja la paginación de tipo "click en botón" utilizando Selenium.

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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURACIÓN GLOBAL Y CONSTANTES ---

BASE_URL = "https://carlos.sanchezdonate.com/"

# URLs semilla con sus categorías normalizadas
SEED_URLS = {
    "rastreo": "https://carlos.sanchezdonate.com/seo-avanzado/rastreo/",
    "tecnologias": "https://carlos.sanchezdonate.com/seo-avanzado/tecnologias/",
    "servidores": "https://carlos.sanchezdonate.com/seo-avanzado/servidores/",
    "rendimiento-velocidad": "https://carlos.sanchezdonate.com/seo-avanzado/rendimiento-velocidad/",
    "metaetiquetas": "https://carlos.sanchezdonate.com/seo-avanzado/metaetiquetas/",
    "enlazado": "https://carlos.sanchezdonate.com/seo-avanzado/enlazado/",
    "multimedia": "https://carlos.sanchezdonate.com/seo-avanzado/multimedia/",
    "seo-internacional": "https://carlos.sanchezdonate.com/seo-avanzado/seo-internacional/",
}

# Selectores CSS basados en el HTML proporcionado
SELECTORS = {
    'articles_container': '#blog-display article.cubrelinks',
    'link': 'a.posts-h2',
    'thumbnail': 'div.posts-picture img',
    'excerpt': 'div.entry',
    'load_more_button': 'button#load-more',
    'content_container': 'section#post-display',
}

# Configuración de archivos y directorios
LOG_DIR = "logs"
LOG_FILENAME_FORMAT = "carlos_sanchez_scraper_{}.log"
JSON_FILE_PATH = "data/carlos_sanchez_database.json"
JSON_BACKUP_PATH = JSON_FILE_PATH + ".bak"

# Configuración de reintentos y delays
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2
RANDOM_DELAY_RANGE = (1, 3)

# --- CLASE PRINCIPAL DEL SCRAPER ---

class CarlosSanchezScraper:
    """
    Clase que encapsula toda la lógica de scraping para el blog de Carlos Sánchez.
    """

    def __init__(self):
        self._setup_logging()
        self.user_agent = UserAgent()
        self.session = self._get_requests_session()
        self.driver = self._setup_selenium_driver()

    def _setup_logging(self):
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        log_file = os.path.join(LOG_DIR, LOG_FILENAME_FORMAT.format(datetime.now().strftime("%Y-%m-%d")))
        self.logger = logging.getLogger("CarlosSanchezScraper")
        self.logger.setLevel(logging.INFO)
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
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
        driver.set_page_load_timeout(30)
        return driver

    def _load_existing_data(self) -> dict:
        if os.path.exists(JSON_FILE_PATH):
            self.logger.info(f"Cargando datos existentes desde {JSON_FILE_PATH}")
            with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"titulo_sitio_web": "Carlos Sánchez", "articles": []}

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
        """Convierte una fecha en formato 'YYYY-MM-DD' a 'DD-MM-YYYY'."""
        try:
            date_obj = datetime.strptime(date_str.strip(), "%Y-%m-%d")
            return date_obj.strftime("%d-%m-%Y")
        except ValueError:
            self.logger.warning(f"Formato de fecha inesperado: '{date_str}'. Se guardará tal cual.")
            return date_str

    def _clean_html_content(self, content_soup: BeautifulSoup) -> str:
        """Extrae y limpia el contenido HTML, manteniendo solo las etiquetas permitidas."""
        # Eliminar elementos no deseados como el índice, banners, artículos recomendados, etc.
        for unwanted in content_soup.select('#indice-contenido, #masterme, #recomendados-display, #remark, .feedback, .leyenda.choosenotfound, .bloque-share-rrss'):
            unwanted.decompose()

        allowed_tags = ['h2', 'h3', 'h4', 'h5', 'h6', 'p', 'figure', 'img', 'table', 'code', 'blockquote', 'a', 'ul', 'ol', 'li']
        content_parts = []
        for element in content_soup.find_all(True):
            if element.name in allowed_tags:
                if hasattr(element, 'attrs'):
                    allowed_attrs = ['href', 'src', 'alt', 'title']
                    attrs = dict(element.attrs)
                    for attr in attrs:
                        if attr not in allowed_attrs:
                            del element.attrs[attr]
                content_parts.append(str(element))
        
        return "\n".join(content_parts)

    def _fetch_article_details(self, article_url: str) -> tuple:
        """Obtiene el contenido completo y la fecha de una página de artículo."""
        self.logger.info(f"Scrapeando detalles del artículo: {article_url}")
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(random.uniform(*RANDOM_DELAY_RANGE))
                response = self.session.get(article_url, timeout=15)
                self.logger.info(f"GET {article_url} - Status: {response.status_code}")
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extraer contenido
                content_container = soup.select_one(SELECTORS['content_container'])
                full_content = self._clean_html_content(content_container) if content_container else ""
                
                # Extraer fecha
                date_str = ""
                dt_elements = soup.select('section.ficha-post dt')
                for dt in dt_elements:
                    if 'Fecha de publicación' in dt.get_text():
                        dd = dt.find_next_sibling('dd')
                        if dd:
                            date_str = dd.get_text(strip=True)
                        break
                
                if not full_content: self.logger.error(f"Selector de contenido no encontrado en {article_url}")
                if not date_str: self.logger.warning(f"Fecha no encontrada en {article_url}")

                return full_content, self._parse_date(date_str)

            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Intento {attempt + 1}/{MAX_RETRIES} fallido para {article_url}: {e}")
                if attempt + 1 == MAX_RETRIES:
                    self.logger.error(f"No se pudo obtener la URL {article_url} después de {MAX_RETRIES} intentos.")
                    return None, None
                time.sleep(RETRY_BACKOFF_FACTOR ** attempt)
        return None, None

    def _scrape_category_page(self, category_url: str) -> str:
        """Usa Selenium para cargar una página de categoría, manejando el botón 'Más artículos'."""
        self.logger.info(f"Abriendo página de categoría con Selenium: {category_url}")
        self.driver.get(category_url)
        
        while True:
            try:
                time.sleep(2)
                load_more_button = self.driver.find_element(By.CSS_SELECTOR, SELECTORS['load_more_button'])
                self.driver.execute_script("arguments[0].scrollIntoView();", load_more_button)
                time.sleep(1)
                load_more_button.click()
                self.logger.info("Botón 'Más artículos' presionado. Esperando contenido...")
            except (NoSuchElementException, TimeoutException):
                self.logger.info("No se encontró más el botón 'Más artículos'. Se asume que todo el contenido está cargado.")
                break
            except ElementClickInterceptedException:
                self.logger.warning("El botón 'Más artículos' no es clickeable, intentando con JavaScript.")
                self.driver.execute_script("arguments[0].click();", load_more_button)
            except Exception as e:
                self.logger.error(f"Error inesperado al hacer clic en 'Más artículos': {e}")
                break
        
        return self.driver.page_source

    def run_initial_scrape(self):
        self.logger.info("========== INICIANDO SCRAPING INICIAL ==========")
        all_articles = []
        for category, url in SEED_URLS.items():
            self.logger.info(f"--- Procesando categoría: {category} ---")
            page_source = self._scrape_category_page(url)
            soup = BeautifulSoup(page_source, 'html.parser')
            article_cards = soup.select(SELECTORS['articles_container'])
            self.logger.info(f"Se encontraron {len(article_cards)} tarjetas de artículo en la categoría '{category}'.")

            for card in article_cards:
                try:
                    link_tag = card.select_one(SELECTORS['link'])
                    if not link_tag or not link_tag.get('href'):
                        self.logger.warning("Tarjeta encontrada sin link. Saltando.")
                        continue

                    relative_link = link_tag['href']
                    article_link = urljoin(BASE_URL, relative_link)
                    article_title = link_tag.get_text(strip=True)
                    
                    thumb_tag = card.select_one(SELECTORS['thumbnail'])
                    excerpt_tag = card.select_one(SELECTORS['excerpt'])
                    
                    full_content, date = self._fetch_article_details(article_link)
                    if not full_content:
                        self.logger.warning(f"No se pudo obtener contenido para '{article_title}'. Saltando artículo.")
                        continue

                    article_data = {
                        "category": category,
                        "link": article_link,
                        "titulo_entrada": article_title,
                        "url_thumbnail": thumb_tag.get('src', '') if thumb_tag else '',
                        "full_content": full_content,
                        "excerpt_content": excerpt_tag.get_text(strip=True) if excerpt_tag else '',
                        "date": date,
                        "favicon_url": "https://cdn-carlos.sanchezdonate.com/wp-content/themes/sanchezdonate/images/favicon/favicon.png"
                    }
                    all_articles.append(article_data)
                    self.logger.info(f"Artículo procesado: '{article_title}'")

                except Exception as e:
                    self.logger.error(f"Error procesando una tarjeta de artículo: {e}", exc_info=True)
        
        final_data = {"titulo_sitio_web": "Carlos Sánchez", "articles": all_articles}
        self._save_data(final_data)
        self.logger.info("========== SCRAPING INICIAL COMPLETADO ==========")

    def run_update_scrape(self):
        # La lógica de actualización es similar y se beneficia de los selectores ya definidos
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
                time.sleep(3)
                soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            except Exception as e:
                self.logger.error(f"No se pudo cargar la página de categoría {url}: {e}")
                continue

            article_cards = soup.select(SELECTORS['articles_container'])
            if not article_cards: continue

            for card in article_cards[:10]: # Revisar los 10 más recientes es suficiente
                link_tag = card.select_one(SELECTORS['link'])
                if not link_tag or not link_tag.get('href'): continue
                
                article_link = urljoin(BASE_URL, link_tag['href'])
                
                if article_link not in existing_urls:
                    self.logger.info(f"¡Nuevo artículo encontrado!: {article_link}")
                    try:
                        article_title = link_tag.get_text(strip=True)
                        thumb_tag = card.select_one(SELECTORS['thumbnail'])
                        excerpt_tag = card.select_one(SELECTORS['excerpt'])
                        full_content, date = self._fetch_article_details(article_link)
                        if not full_content: continue

                        new_article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": thumb_tag.get('src', '') if thumb_tag else '',
                            "full_content": full_content,
                            "excerpt_content": excerpt_tag.get_text(strip=True) if excerpt_tag else '',
                            "date": date,
                            "favicon_url": "https://cdn-carlos.sanchezdonate.com/wp-content/themes/sanchezdonate/images/favicon/favicon.png"
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
    scraper = CarlosSanchezScraper()
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