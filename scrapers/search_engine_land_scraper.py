# -*- coding: utf-8 -*-

"""
==================================================================================================
Web Scraper para el Blog de Search Engine Land
==================================================================================================
Autor: Gemini (actuando como Senior Python Developer)
Fecha: 25-09-2025
Versión: 2.2.0 (Con Extracción de Contenido Corregida)

Descripción:
Este script realiza un web scraping completo del blog de Search Engine Land.
- Scraper Inicial: Extrae artículos hasta un máximo de 5 páginas por categoría para ser eficiente.
- Actualización Automática: Lógica para ejecutarse diariamente y añadir solo los nuevos posts.
- Manejo de Errores: Implementación robusta de reintentos, timeouts y logging detallado.
- Salida Estructurada: Guarda los datos en un archivo JSON bien formado.
- Anti-Baneo: Utiliza técnicas como rotación de User-Agents y delays aleatorios.
- Paginación Eficiente: Navega a través de las páginas de categorías sin necesidad de Selenium.

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
import re
from datetime import datetime
from shutil import copyfile
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# --- CONFIGURACIÓN GLOBAL Y CONSTANTES ---

BASE_URL = "https://searchengineland.com/"

# URLs semilla con sus categorías normalizadas
SEED_URLS = {
    "content": "https://searchengineland.com/library/seo/content",
    "ecommerce": "https://searchengineland.com/library/seo/ecommerce",
    "enterprise-seo": "https://searchengineland.com/library/seo/enterprise-seo",
    "international": "https://searchengineland.com/library/seo/international",
    "local": "https://searchengineland.com/library/seo/local",
    "technical": "https://searchengineland.com/library/seo/technical",
}

# Selectores CSS robustos
SELECTORS = {
    'article_card': 'article.stream-article',
    'link': 'h2.headline a',
    'thumbnail': 'div.article-image img',
    'excerpt': 'p.dek',
    'date_and_author': 'span.author-time',
    'content_container': 'div#articleContent',
}

# Límite de páginas a scrapear por categoría
MAX_PAGES_TO_SCRAPE = 5

# Configuración de archivos y directorios
LOG_DIR = "logs"
LOG_FILENAME_FORMAT = "search_engine_land_scraper_{}.log"
JSON_FILE_PATH = "data/search_engine_land_database.json"
JSON_BACKUP_PATH = JSON_FILE_PATH + ".bak"

# Configuración de reintentos y delays
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2
RANDOM_DELAY_RANGE = (1, 3)

# --- CLASE PRINCIPAL DEL SCRAPER ---

class SearchEngineLandScraper:
    def __init__(self):
        self._setup_logging()
        self.user_agent = UserAgent()
        self.session = self._get_requests_session()

    def _setup_logging(self):
        if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
        log_file = os.path.join(LOG_DIR, LOG_FILENAME_FORMAT.format(datetime.now().strftime("%Y-%m-%d")))
        self.logger = logging.getLogger("SearchEngineLandScraper")
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
        return {"titulo_sitio_web": "Search Engine Land", "articles": []}

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
        match = re.search(r'(\w{3}\s\d{1,2},\s\d{4})', date_str)
        if match:
            try:
                date_obj = datetime.strptime(match.group(1), "%b %d, %Y")
                return date_obj.strftime("%d-%m-%Y")
            except ValueError:
                self.logger.warning(f"No se pudo parsear la fecha: '{date_str}'.")
                return ""
        return ""

    # =================================================================================
    # --- [FUNCIÓN MODIFICADA] ---
    # Se ha corregido la lógica de limpieza para extraer correctamente el contenido.
    # =================================================================================
    def _clean_html_content(self, content_soup: BeautifulSoup) -> str:
        # 1. [CORRECCIÓN] Se eliminan selectores que borraban contenido útil como '.bialty-container'.
        # 2. [MEJORA] Se añaden nuevos selectores para limpiar elementos no deseados como formularios y anuncios.
        unwanted_selectors = '.article-disclosure, .author-about, .related-articles, .dmd-content, .eoa-ad, .nl-inline-form, form, .ad-space'
        for unwanted in content_soup.select(unwanted_selectors):
            if unwanted:
                unwanted.decompose()

        allowed_tags = ['h2', 'h3', 'h4', 'h5', 'h6', 'p', 'figure', 'img', 'table', 'code', 'blockquote', 'a', 'ul', 'ol', 'li']
        content_parts = []
        
        # Se itera sobre los elementos para construir el contenido limpio
        for element in content_soup.find_all(True, recursive=True):
            if element.name in allowed_tags:
                if hasattr(element, 'attrs'):
                    # 3. [MEJORA] Se añaden más atributos para capturar imágenes modernas correctamente.
                    allowed_attrs = ['href', 'src', 'alt', 'title', 'srcset', 'data-lazy-src', 'data-lazy-srcset', 'sizes']
                    attrs = dict(element.attrs)
                    for attr in attrs:
                        if attr not in allowed_attrs:
                            del element.attrs[attr]
                content_parts.append(str(element))
        return "\n".join(content_parts)

    def _create_excerpt(self, text: str) -> str:
        clean_text = ' '.join(text.split())
        if len(clean_text) > 70:
            return clean_text[:70].strip() + "..."
        return clean_text

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
            page_count = 1

            while current_url and page_count <= MAX_PAGES_TO_SCRAPE:
                self.logger.info(f"Procesando página {page_count}/{MAX_PAGES_TO_SCRAPE} de la categoría '{category}'...")
                response = self._fetch_with_retries(current_url)
                if not response: break

                soup = BeautifulSoup(response.text, 'html.parser')
                article_cards = soup.select(SELECTORS['article_card'])
                self.logger.info(f"Se encontraron {len(article_cards)} tarjetas de artículo en: {current_url}")

                for card in article_cards:
                    try:
                        link_tag = card.select_one(SELECTORS['link'])
                        if not link_tag or not link_tag.get('href'): continue

                        article_link = link_tag['href']
                        if article_link in processed_links: continue
                        processed_links.add(article_link)

                        article_title = link_tag.get_text(strip=True)
                        thumb_tag = card.select_one(SELECTORS['thumbnail'])
                        excerpt_tag = card.select_one(SELECTORS['excerpt'])
                        date_author_tag = card.select_one(SELECTORS['date_and_author'])
                        
                        article_response = self._fetch_with_retries(article_link)
                        if not article_response: continue
                        
                        article_soup = BeautifulSoup(article_response.text, 'html.parser')
                        content_container = article_soup.select_one(SELECTORS['content_container'])
                        
                        # Con la función corregida, 'full_content' ahora se poblará correctamente
                        full_content = self._clean_html_content(content_container) if content_container else ""
                        
                        excerpt_text = excerpt_tag.get_text(strip=True) if excerpt_tag else ""

                        article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": thumb_tag.get('data-lazy-src', thumb_tag.get('src', '')) if thumb_tag else '',
                            "full_content": full_content,
                            "excerpt_content": self._create_excerpt(excerpt_text),
                            "date": self._parse_date(date_author_tag.get_text(strip=True)) if date_author_tag else '',
                            "favicon_url": "https://searchengineland.com/favicon.ico"
                        }
                        all_articles.append(article_data)
                        self.logger.info(f"Artículo procesado: '{article_title}'")

                    except Exception as e:
                        self.logger.error(f"Error procesando una tarjeta de artículo: {e}", exc_info=True)
                
                next_page_tag = soup.find('a', text='»')
                if next_page_tag and next_page_tag.get('href'):
                    current_url = urljoin(BASE_URL, next_page_tag['href'])
                    self.logger.info(f"Página siguiente encontrada: {current_url}")
                else:
                    self.logger.info(f"Fin de la paginación para la categoría '{category}'.")
                    current_url = None
                
                page_count += 1
            
            if page_count > MAX_PAGES_TO_SCRAPE:
                self.logger.info(f"Límite de {MAX_PAGES_TO_SCRAPE} páginas alcanzado para la categoría '{category}'.")


        final_data = {"titulo_sitio_web": "Search Engine Land", "articles": all_articles}
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
            article_cards = soup.select(SELECTORS['article_card'])
            if not article_cards: continue

            for card in article_cards[:5]:
                link_tag = card.select_one(SELECTORS['link'])
                if not link_tag or not link_tag.get('href'): continue
                
                article_link = link_tag['href']
                
                if article_link not in existing_urls:
                    self.logger.info(f"¡Nuevo artículo encontrado!: {article_link}")
                    try:
                        article_title = link_tag.get_text(strip=True)
                        thumb_tag = card.select_one(SELECTORS['thumbnail'])
                        excerpt_tag = card.select_one(SELECTORS['excerpt'])
                        date_author_tag = card.select_one(SELECTORS['date_and_author'])
                        
                        article_response = self._fetch_with_retries(article_link)
                        if not article_response: continue
                        
                        article_soup = BeautifulSoup(article_response.text, 'html.parser')
                        content_container = article_soup.select_one(SELECTORS['content_container'])
                        
                        # Aplicando la corrección también en la actualización
                        full_content = self._clean_html_content(content_container) if content_container else ""
                        excerpt_text = excerpt_tag.get_text(strip=True) if excerpt_tag else ""

                        new_article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": thumb_tag.get('data-lazy-src', thumb_tag.get('src', '')) if thumb_tag else '',
                            "full_content": full_content,
                            "excerpt_content": self._create_excerpt(excerpt_text),
                            "date": self._parse_date(date_author_tag.get_text(strip=True)) if date_author_tag else '',
                            "favicon_url": "https://searchengineland.com/favicon.ico"
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
    scraper = SearchEngineLandScraper()
    try:
        if not os.path.exists(JSON_FILE_PATH):
            scraper.run_initial_scrape()
        else:
            scraper.run_update_scrape()
    except Exception as e:
        scraper.logger.critical(f"Ha ocurrido un error fatal en el scraper: {e}", exc_info=True)

if __name__ == "__main__":
    main()