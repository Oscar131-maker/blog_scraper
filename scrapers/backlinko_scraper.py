# -*- coding: utf-8 -*-

"""
==================================================================================================
Web Scraper para el Blog de Backlinko
==================================================================================================
Autor: Gemini (actuando como Senior Python Developer)
Fecha: 25-09-2025
Versión: 2.0.0 (Corregida con Paginación)

Descripción:
Este script realiza un web scraping completo del blog de Backlinko.
- Scraper Inicial: Extrae todos los artículos existentes desde URLs semilla, manejando paginación.
- Actualización Automática: Lógica para ejecutarse diariamente y añadir solo los nuevos posts.
- Manejo de Errores: Implementación robusta de reintentos, timeouts y logging detallado.
- Salida Estructurada: Guarda los datos en un archivo JSON bien formado.
- Anti-Baneo: Utiliza técnicas como rotación de User-Agents y delays aleatorios.
- Selectores Robustos: Evita el uso de clases CSS dinámicas, basándose en la estructura del DOM.

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
from datetime import datetime
from shutil import copyfile
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# --- CONFIGURACIÓN GLOBAL Y CONSTANTES ---

BASE_URL = "https://backlinko.com/"

# URLs semilla con sus categorías normalizadas
SEED_URLS = {
    "affiliate-marketing": "https://backlinko.com/blog/categories/affiliate-marketing",
    "content": "https://backlinko.com/blog/categories/content",
    "ecommerce": "https://backlinko.com/blog/categories/ecommerce",
    "seo": "https://backlinko.com/blog/categories/seo",
    "keyword-research": "https://backlinko.com/blog/categories/keyword-research",
    "link-building": "https://backlinko.com/blog/categories/link-building",
    "local-seo": "https://backlinko.com/blog/categories/local-seo",
    "marketing": "https://backlinko.com/blog/categories/marketing",
    "research": "https://backlinko.com/blog/categories/research",
    "technical-seo": "https://backlinko.com/blog/categories/technical-seo",
}

# Selectores CSS robustos basados en la estructura del DOM
SELECTORS = {
    'articles_container': 'main article',
    'link': 'a',
    'titulo_entrada': 'h2',
    'thumbnail': 'figure picture img',
    'next_page_link': 'nav ul[aria-label="Pagination"] li.next a',
    'content_container': 'div#content',
    'date_tag': 'header time.updated',
}

# Configuración de archivos y directorios
LOG_DIR = "logs"
LOG_FILENAME_FORMAT = "backlinko_scraper_{}.log"
JSON_FILE_PATH = "data/backlinko_database.json"
JSON_BACKUP_PATH = JSON_FILE_PATH + ".bak"

# Configuración de reintentos y delays
MAX_RETRIES = 3
RETRY_BACKOFF_FACTOR = 2
RANDOM_DELAY_RANGE = (1, 3)

# --- CLASE PRINCIPAL DEL SCRAPER ---

class BacklinkoScraper:
    def __init__(self):
        self._setup_logging()
        self.user_agent = UserAgent()
        self.session = self._get_requests_session()

    def _setup_logging(self):
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        log_file = os.path.join(LOG_DIR, LOG_FILENAME_FORMAT.format(datetime.now().strftime("%Y-%m-%d")))
        self.logger = logging.getLogger("BacklinkoScraper")
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

    def _load_existing_data(self) -> dict:
        if os.path.exists(JSON_FILE_PATH):
            self.logger.info(f"Cargando datos existentes desde {JSON_FILE_PATH}")
            with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"titulo_sitio_web": "Backlinko", "articles": []}

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
            date_part = date_str.split('T')[0]
            date_obj = datetime.strptime(date_part, "%Y-%m-%d")
            return date_obj.strftime("%d-%m-%Y")
        except (ValueError, IndexError):
            self.logger.warning(f"Formato de fecha inesperado: '{date_str}'. Se guardará tal cual.")
            return date_str

    def _clean_html_content(self, content_soup: BeautifulSoup) -> str:
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
        
        for category, start_url in SEED_URLS.items():
            self.logger.info(f"--- Procesando categoría: {category} ---")
            
            current_url = start_url
            # [CORRECCIÓN] Bucle para manejar la paginación
            while current_url:
                response = self._fetch_with_retries(current_url)
                if not response:
                    break

                soup = BeautifulSoup(response.text, 'html.parser')
                article_cards = soup.select(SELECTORS['articles_container'])
                self.logger.info(f"Se encontraron {len(article_cards)} tarjetas de artículo en: {current_url}")

                for card in article_cards:
                    try:
                        link_tag = card.select_one(SELECTORS['link'])
                        if not link_tag or not link_tag.get('href'):
                            self.logger.warning("Tarjeta encontrada sin link. Saltando.")
                            continue

                        relative_link = link_tag['href']
                        article_link = urljoin(BASE_URL, relative_link)
                        
                        if any(a['link'] == article_link for a in all_articles):
                            continue

                        titulo_tag = link_tag.select_one(SELECTORS['titulo_entrada'])
                        article_title = titulo_tag.get_text(strip=True) if titulo_tag else "Sin Título"
                        thumb_tag = link_tag.select_one(SELECTORS['thumbnail'])
                        
                        article_response = self._fetch_with_retries(article_link)
                        if not article_response:
                            self.logger.warning(f"No se pudo obtener contenido para '{article_title}'. Saltando.")
                            continue
                        
                        article_soup = BeautifulSoup(article_response.text, 'html.parser')
                        content_container = article_soup.select_one(SELECTORS['content_container'])
                        full_content = self._clean_html_content(content_container) if content_container else ""
                        date_tag = article_soup.select_one(SELECTORS['date_tag'])
                        date_str = date_tag['datetime'] if date_tag and date_tag.has_attr('datetime') else ""

                        article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": thumb_tag.get('src', '') if thumb_tag else '',
                            "full_content": full_content,
                            "excerpt_content": self._create_excerpt(full_content),
                            "date": self._parse_date(date_str),
                            "favicon_url": "https://backlinko.com/favicon-32x32.png?v=KmwbmdMLo6"
                        }
                        all_articles.append(article_data)
                        self.logger.info(f"Artículo procesado: '{article_title}'")

                    except Exception as e:
                        self.logger.error(f"Error procesando una tarjeta de artículo: {e}", exc_info=True)
                
                # =================================================================
                # LÓGICA DE PAGINACIÓN: Buscar el enlace a la siguiente página
                # =================================================================
                next_page_tag = soup.select_one(SELECTORS['next_page_link'])
                if next_page_tag and next_page_tag.get('href'):
                    next_page_url = urljoin(BASE_URL, next_page_tag['href'])
                    self.logger.info(f"Página siguiente encontrada: {next_page_url}")
                    current_url = next_page_url
                else:
                    self.logger.info(f"Fin de la paginación para la categoría '{category}'.")
                    current_url = None # Termina el bucle 'while' para esta categoría

        final_data = {"titulo_sitio_web": "Backlinko", "articles": all_articles}
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
            article_cards = soup.select(SELECTORS['articles_container'])
            if not article_cards: continue

            for card in article_cards[:5]:
                link_tag = card.select_one(SELECTORS['link'])
                if not link_tag or not link_tag.get('href'): continue
                
                article_link = urljoin(BASE_URL, link_tag['href'])
                
                if article_link not in existing_urls:
                    self.logger.info(f"¡Nuevo artículo encontrado!: {article_link}")
                    try:
                        titulo_tag = link_tag.select_one(SELECTORS['titulo_entrada'])
                        article_title = titulo_tag.get_text(strip=True) if titulo_tag else "Sin Título"
                        thumb_tag = link_tag.select_one(SELECTORS['thumbnail'])
                        
                        article_response = self._fetch_with_retries(article_link)
                        if not article_response: continue
                        
                        article_soup = BeautifulSoup(article_response.text, 'html.parser')
                        content_container = article_soup.select_one(SELECTORS['content_container'])
                        full_content = self._clean_html_content(content_container) if content_container else ""
                        date_tag = article_soup.select_one(SELECTORS['date_tag'])
                        date_str = date_tag['datetime'] if date_tag and date_tag.has_attr('datetime') else ""

                        new_article_data = {
                            "category": category,
                            "link": article_link,
                            "titulo_entrada": article_title,
                            "url_thumbnail": thumb_tag.get('src', '') if thumb_tag else '',
                            "full_content": full_content,
                            "excerpt_content": self._create_excerpt(full_content),
                            "date": self._parse_date(date_str),
                            "favicon_url": "https://backlinko.com/favicon-32x32.png?v=KmwbmdMLo6"
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
    scraper = BacklinkoScraper()
    try:
        if not os.path.exists(JSON_FILE_PATH):
            scraper.run_initial_scrape()
        else:
            scraper.run_update_scrape()
    except Exception as e:
        scraper.logger.critical(f"Ha ocurrido un error fatal en el scraper: {e}", exc_info=True)

if __name__ == "__main__":
    main()