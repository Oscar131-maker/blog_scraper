# -*- coding: utf-8 -*-

"""
==================================================================================================
Web Scraper para 'El Placer del SEO'
==================================================================================================
Autor: Gemini (actuando como Senior Python Developer)
Fecha: 25-09-2025
Versión: 2.0.0 (Final)

Descripción:
Este script realiza un web scraping completo del blog 'El Placer del SEO'.
- Scraper Inicial: Extrae todos los artículos existentes desde URLs semilla de categorías.
- Actualización Automática: Lógica para ejecutarse diariamente y añadir solo los nuevos posts.
- Manejo de Errores: Implementación robusta de reintentos, timeouts y logging detallado.
- Salida Estructurada: Guarda los datos en un archivo JSON bien formado.
- Anti-Baneo: Utiliza técnicas como rotación de User-Agents y delays aleatorios.
- Dinamismo: Maneja la paginación de tipo "Infinite Scroll" utilizando Selenium.

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

import requests
from bs4 import BeautifulSoup, NavigableString
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURACIÓN GLOBAL Y CONSTANTES ---

SEED_URLS = {
    "seo-de-popularidad": "https://elplacerdelseo.com/seo/seo-de-popularidad/",
    "seo-editorial": "https://elplacerdelseo.com/seo/seo-editorial/",
    "estrategia-seo": "https://elplacerdelseo.com/seo/estrategia-seo/",
    "inteligencia-artificial": "https://elplacerdelseo.com/seo/inteligencia-artificial/",
    "brand-seo": "https://elplacerdelseo.com/seo/brand-seo/",
}

# [CORRECCIÓN FINAL] Selectores ajustados con precisión al HTML proporcionado.
SELECTORS = {
    'articles_container': 'article.elementor-post',
    'link_and_thumb_container': 'a.elementor-post__thumbnail__link',
    'thumbnail': 'img',
    'titulo_entrada': 'h2.elementor-post__title a', # Corregido de h3 a h2
    'date_span': 'span.elementor-post-date',
    'content_container': 'div.entry-content'
}

LOG_DIR = "logs"
LOG_FILENAME_FORMAT = "scraper_{}.log"
JSON_FILE_PATH = "data/el_placer_del_seo_database.json"
JSON_BACKUP_PATH = JSON_FILE_PATH + ".bak"

MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2
RANDOM_DELAY_RANGE = (1, 3)

# --- CLASE PRINCIPAL DEL SCRAPER ---

class ElPlacerDelSEOScraper:
    def __init__(self):
        self._setup_logging()
        self.user_agent = UserAgent()
        self.session = self._get_requests_session()
        self.driver = self._setup_selenium_driver()
        try:
            locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
        except locale.Error:
            self.logger.warning("Locale 'es_ES.UTF-8' no encontrado. Intentando con locale por defecto.")
            try:
                locale.setlocale(locale.LC_TIME, '')
            except locale.Error:
                self.logger.error("No se pudo configurar ningún locale en español. El parseo de fechas puede fallar.")

    def _setup_logging(self):
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        log_file = os.path.join(LOG_DIR, LOG_FILENAME_FORMAT.format(datetime.now().strftime("%Y-%m-%d")))
        self.logger = logging.getLogger("ElPlacerDelSEOScraper")
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
        chrome_options.add_argument("--log-level=3") # Reduce logs de consola de Selenium
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
        return {"titulo_sitio_web": "El Placer del SEO", "articles": []}

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
            # Manejar el formato "abril 28, 2025"
            date_obj = datetime.strptime(date_str.strip(), "%B %d, %Y")
            return date_obj.strftime("%d-%m-%Y")
        except ValueError:
            self.logger.warning(f"No se pudo parsear la fecha: '{date_str}'. Se guardará en formato original.")
            return date_str

    def _clean_html_content(self, content_soup: BeautifulSoup) -> str:
        """
        [MEJORA] Extrae contenido limpio, eliminando los bloques de "Share at:"
        y manteniendo solo las etiquetas permitidas.
        """
        # Eliminar los divs de compartir en redes sociales
        for share_div in content_soup.find_all("div", style=lambda value: value and 'margin: 20px 0;' in value):
            share_div.decompose()

        allowed_tags = ['h2', 'h3', 'h4', 'h5', 'h6', 'p', 'figure', 'img', 'table', 'code', 'blockquote', 'a', 'ul', 'ol', 'li']
        content_parts = []
        for element in content_soup.find_all(True):
            if element.name in allowed_tags:
                # Eliminar atributos no esenciales como style, class, etc.
                if hasattr(element, 'attrs'):
                    allowed_attrs = ['href', 'src', 'alt', 'title']
                    attrs = dict(element.attrs)
                    for attr in attrs:
                        if attr not in allowed_attrs:
                            del element.attrs[attr]
                content_parts.append(str(element))
        
        return "\n".join(content_parts)

    def _create_excerpt(self, full_content_html: str) -> str:
        soup = BeautifulSoup(full_content_html, 'html.parser')
        text = ' '.join(soup.get_text().split())
        if len(text) > 70:
            return text[:70].strip() + "..."
        return text

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
                if not content_container:
                    self.logger.error(f"Selector de contenido no encontrado en {article_url}")
                    return ""
                return self._clean_html_content(content_container)
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
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.logger.info("Haciendo scroll para cargar más artículos...")
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(4)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                self.logger.info("No se cargó más contenido. Se ha llegado al final de la página.")
                break
            last_height = new_height
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
                    link_tag = card.select_one(SELECTORS['titulo_entrada'])
                    if not link_tag or not link_tag.get('href'):
                        self.logger.warning("Tarjeta de artículo encontrada sin link o título. Saltando.")
                        continue

                    article_link = link_tag['href']
                    article_title = link_tag.text.strip()
                    
                    thumb_container = card.select_one(SELECTORS['link_and_thumb_container'])
                    thumb_tag = thumb_container.select_one(SELECTORS['thumbnail']) if thumb_container else None
                    date_tag = card.select_one(SELECTORS['date_span'])
                    
                    full_content = self._fetch_article_content(article_link)
                    if not full_content:
                        self.logger.warning(f"No se pudo obtener contenido para '{article_title}'. Saltando artículo.")
                        continue

                    article_data = {
                        "category": category,
                        "link": article_link,
                        "titulo_entrada": article_title,
                        "url_thumbnail": thumb_tag.get('src', '') if thumb_tag else '',
                        "full_content": full_content,
                        "excerpt_content": self._create_excerpt(full_content),
                        "date": self._parse_date(date_tag.text.strip()) if date_tag else '',
                        "favicon_url": "https://elplacerdelseo.com/wp-content/uploads/2022/10/cropped-captura-de-pantalla-2022-10-19-a-las-17.14.18-32x32.png"
                    }
                    all_articles.append(article_data)
                    self.logger.info(f"Artículo procesado: '{article_title}'")

                except Exception as e:
                    self.logger.error(f"Error procesando una tarjeta de artículo: {e}", exc_info=True)
        
        final_data = {"titulo_sitio_web": "El Placer del SEO", "articles": all_articles}
        self._save_data(final_data)
        self.logger.info("========== SCRAPING INICIAL COMPLETADO ==========")

    def run_update_scrape(self):
        # (La lógica de actualización no necesita cambios, se beneficia de los selectores corregidos)
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
            if not article_cards:
                self.logger.warning(f"No se encontraron artículos en la primera página de '{category}'.")
                continue
            for card in article_cards[:10]:
                link_tag = card.select_one(SELECTORS['titulo_entrada'])
                if not link_tag or not link_tag.get('href'):
                    continue
                article_link = link_tag['href']
                if article_link not in existing_urls:
                    self.logger.info(f"¡Nuevo artículo encontrado!: {article_link}")
                    try:
                        article_title = link_tag.text.strip()
                        thumb_container = card.select_one(SELECTORS['link_and_thumb_container'])
                        thumb_tag = thumb_container.select_one(SELECTORS['thumbnail']) if thumb_container else None
                        date_tag = card.select_one(SELECTORS['date_span'])
                        full_content = self._fetch_article_content(article_link)
                        if not full_content:
                            self.logger.warning(f"No se pudo obtener contenido para el nuevo artículo '{article_title}'. Saltando.")
                            continue
                        new_article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": thumb_tag.get('src', '') if thumb_tag else '',
                            "full_content": full_content,
                            "excerpt_content": self._create_excerpt(full_content),
                            "date": self._parse_date(date_tag.text.strip()) if date_tag else '',
                            "favicon_url": "https://elplacerdelseo.com/wp-content/uploads/2022/10/cropped-captura-de-pantalla-2022-10-19-a-las-17.14.18-32x32.png"
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
    scraper = ElPlacerDelSEOScraper()
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