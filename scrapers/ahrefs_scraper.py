# -*- coding: utf-8 -*-

"""
==================================================================================================
Web Scraper para el Blog de Ahrefs (Español)
==================================================================================================
Autor: Gemini (actuando como Senior Python Developer)
Fecha: 25-09-2025
Versión: 1.0.0

Descripción:
Este script realiza un web scraping completo del blog en español de Ahrefs.
- Scraper Inicial: Extrae todos los artículos existentes, manejando una paginación compleja.
- Actualización Automática: Lógica para ejecutarse diariamente y añadir solo los nuevos posts.
- Manejo de Errores: Implementación robusta de reintentos, timeouts y logging detallado.
- Salida Estructurada: Guarda los datos en un archivo JSON bien formado.
- Anti-Baneo: Utiliza técnicas como rotación de User-Agents y delays aleatorios.
- Selectores Robustos: Se basa en la estructura del DOM para evitar problemas con clases dinámicas.

Dependencias:
- beautifulsoup4
- requests
- fake-useragent

Instalación de dependencias:
pip install beautifulsoup4 requests fake-useragent
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
from bs4 import BeautifulSoup, Tag
from fake_useragent import UserAgent

# --- CONFIGURACIÓN GLOBAL Y CONSTANTES ---

BASE_URL = "https://ahrefs.com/blog/es/"

# URLs semilla con sus categorías normalizadas
SEED_URLS = {
    "busqueda-con-ia": "https://ahrefs.com/blog/es/category/busqueda-con-ia/",
    "blog-de-producto": "https://ahrefs.com/blog/es/category/product-blog/",
    "estudios-y-datos": "https://ahrefs.com/blog/es/category/data-studies/",
    "seo-general": "https://ahrefs.com/blog/es/category/general-seo/",
    "investigacion-de-palabras-clave": "https://ahrefs.com/blog/es/category/keyword-research/",
    "seo-on-page": "https://ahrefs.com/blog/es/category/on-page-seo/",
    "link-building": "https://ahrefs.com/blog/es/category/link-building/",
    "seo-tecnico": "https://ahrefs.com/blog/es/category/technical-seo/",
    "seo-local": "https://ahrefs.com/blog/es/category/local-seo/",
    "seo-enterprise": "https://ahrefs.com/blog/es/category/enterprise-seo/",
    "marketing-general": "https://ahrefs.com/blog/es/category/marketing/",
    "marketing-de-contenidos": "https://ahrefs.com/blog/es/category/content-marketing/",
    "marketing-de-afiliacion": "https://ahrefs.com/blog/es/category/affiliate-marketing/",
    "marketing-de-pago": "https://ahrefs.com/blog/es/category/paid-marketing/",
    "video-marketing": "https://ahrefs.com/blog/es/category/video-marketing/",
}

# Selectores CSS robustos
SELECTORS = {
    'article_headers': 'header.post-header', # Captura tanto los "mejores" como los "recientes"
    'link_and_title': 'h3 a',
    'excerpt_best_articles': 'div.post-meta span',
    'pagination_container': 'div.wp-pagenavi',
    'current_page_span': 'div.wp-pagenavi span.current',
    'content_container': 'div.post-content',
    'date_tag': 'span.post-date.published.updated',
}

# Configuración de archivos y directorios
LOG_DIR = "logs"
LOG_FILENAME_FORMAT = "ahrefs_scraper_{}.log"
JSON_FILE_PATH = "data/ahrefs_database.json"
JSON_BACKUP_PATH = JSON_FILE_PATH + ".bak"

# Configuración de reintentos y delays
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2
RANDOM_DELAY_RANGE = (1, 3)

# --- CLASE PRINCIPAL DEL SCRAPER ---

class AhrefsScraper:
    def __init__(self):
        self._setup_logging()
        self.user_agent = UserAgent()
        self.session = self._get_requests_session()
        try:
            locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
        except locale.Error:
            self.logger.warning("Locale 'es_ES.UTF-8' no encontrado. El parseo de fechas puede fallar.")
            locale.setlocale(locale.LC_TIME, '')

    def _setup_logging(self):
        if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
        log_file = os.path.join(LOG_DIR, LOG_FILENAME_FORMAT.format(datetime.now().strftime("%Y-%m-%d")))
        self.logger = logging.getLogger("AhrefsScraper")
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

    def _load_existing_data(self) -> dict:
        if os.path.exists(JSON_FILE_PATH):
            self.logger.info(f"Cargando datos existentes desde {JSON_FILE_PATH}")
            with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"titulo_sitio_web": "Ahrefs", "articles": []}

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
            date_obj = datetime.strptime(date_str.strip(), "%B %d, %Y")
            return date_obj.strftime("%d-%m-%Y")
        except ValueError:
            self.logger.warning(f"Formato de fecha inesperado: '{date_str}'. Se guardará tal cual.")
            return date_str

    def _clean_html_content(self, content_soup: BeautifulSoup) -> str:
        # Eliminar elementos no deseados como bio del autor, banners, etc.
        for unwanted in content_soup.select('div.author-desktop, div.post-navigation2, div.post-navigation-left'):
            unwanted.decompose()
        
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

    def _create_excerpt(self, full_content_html: str) -> str:
        soup = BeautifulSoup(full_content_html, 'html.parser')
        text = ' '.join(soup.get_text().split())
        if len(text) > 70:
            return text[:70].strip() + "..."
        return text

    def _fetch_with_retries(self, url: str) -> requests.Response | None:
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(random.uniform(*RANDOM_DELAY_RANGE))
                response = self.session.get(url, timeout=20)
                self.logger.info(f"GET {url} - Status: {response.status_code}")
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Intento {attempt + 1}/{MAX_RETRIES} fallido para {url}: {e}")
                if attempt + 1 == MAX_RETRIES:
                    self.logger.error(f"No se pudo obtener la URL {url} después de {MAX_RETRIES} intentos.")
                    return None
                time.sleep(RETRY_BACKOFF_FACTOR ** attempt)
        return None

    def run_initial_scrape(self):
        self.logger.info("========== INICIANDO SCRAPING INICIAL ==========")
        all_articles = []
        processed_links = set()

        for category, start_url in SEED_URLS.items():
            self.logger.info(f"--- Procesando categoría: {category} ---")
            current_url = start_url
            
            while current_url:
                response = self._fetch_with_retries(current_url)
                if not response: break

                soup = BeautifulSoup(response.text, 'html.parser')
                article_headers = soup.select(SELECTORS['article_headers'])
                self.logger.info(f"Se encontraron {len(article_headers)} cabeceras de artículo en: {current_url}")

                for header in article_headers:
                    try:
                        link_tag = header.select_one(SELECTORS['link_and_title'])
                        if not link_tag or not link_tag.get('href'): continue

                        article_link = link_tag['href']
                        if article_link in processed_links: continue
                        processed_links.add(article_link)

                        article_title = link_tag.get_text(strip=True)
                        
                        article_response = self._fetch_with_retries(article_link)
                        if not article_response: continue
                        
                        article_soup = BeautifulSoup(article_response.text, 'html.parser')
                        content_container = article_soup.select_one(SELECTORS['content_container'])
                        full_content = self._clean_html_content(content_container) if content_container else ""
                        
                        date_tag = article_soup.select_one(SELECTORS['date_tag'])
                        date_str = date_tag.get_text(strip=True) if date_tag else ""

                        article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": "", # No hay thumbnail en las cards
                            "full_content": full_content,
                            "excerpt_content": self._create_excerpt(full_content),
                            "date": self._parse_date(date_str),
                            "favicon_url": "https://ahrefs.com/blog/es/wp-content/themes/Ahrefs-4/images/favicons/favicon-48x48.png"
                        }
                        all_articles.append(article_data)
                        self.logger.info(f"Artículo procesado: '{article_title}'")

                    except Exception as e:
                        self.logger.error(f"Error procesando una cabecera de artículo: {e}", exc_info=True)
                
                # Lógica de paginación inteligente
                pagination_container = soup.select_one(SELECTORS['pagination_container'])
                next_page_url = None
                if pagination_container:
                    current_page_span = pagination_container.select_one('span.current')
                    if current_page_span:
                        next_sibling = current_page_span.find_next_sibling()
                        if next_sibling and isinstance(next_sibling, Tag) and next_sibling.name == 'a' and 'page' in next_sibling.get('class', []):
                            next_page_url = next_sibling['href']
                    else: # Fallback para el caso de la flecha ">"
                        last_link = pagination_container.select_one('a.last')
                        if last_link:
                            next_page_url = last_link['href']

                if next_page_url:
                    current_url = urljoin(BASE_URL, next_page_url)
                    self.logger.info(f"Página siguiente encontrada: {current_url}")
                else:
                    self.logger.info(f"Fin de la paginación para la categoría '{category}'.")
                    current_url = None

        final_data = {"titulo_sitio_web": "Ahrefs", "articles": all_articles}
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
            response = self._fetch_with_retries(url)
            if not response: continue

            soup = BeautifulSoup(response.text, 'html.parser')
            article_headers = soup.select(SELECTORS['article_headers'])
            if not article_headers: continue

            for header in article_headers[:5]:
                link_tag = header.select_one(SELECTORS['link_and_title'])
                if not link_tag or not link_tag.get('href'): continue
                
                article_link = link_tag['href']
                
                if article_link not in existing_urls:
                    self.logger.info(f"¡Nuevo artículo encontrado!: {article_link}")
                    try:
                        article_title = link_tag.get_text(strip=True)
                        
                        article_response = self._fetch_with_retries(article_link)
                        if not article_response: continue
                        
                        article_soup = BeautifulSoup(article_response.text, 'html.parser')
                        content_container = article_soup.select_one(SELECTORS['content_container'])
                        full_content = self._clean_html_content(content_container) if content_container else ""
                        date_tag = article_soup.select_one(SELECTORS['date_tag'])
                        date_str = date_tag.get_text(strip=True) if date_tag else ""

                        new_article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": "",
                            "full_content": full_content,
                            "excerpt_content": self._create_excerpt(full_content),
                            "date": self._parse_date(date_str),
                            "favicon_url": "https://ahrefs.com/blog/es/wp-content/themes/Ahrefs-4/images/favicons/favicon-48x48.png"
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

def main():
    scraper = AhrefsScraper()
    try:
        if not os.path.exists(JSON_FILE_PATH):
            scraper.run_initial_scrape()
        else:
            scraper.run_update_scrape()
    except Exception as e:
        scraper.logger.critical(f"Ha ocurrido un error fatal en el scraper: {e}", exc_info=True)

if __name__ == "__main__":
    main()