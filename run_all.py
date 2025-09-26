# run_all.py
import subprocess
import sys

# Lista de los scripts de scrapers que quieres ejecutar
SCRAPERS = [
    "scrapers/ahrefs_scraper.py",
    "scrapers/backlinko_scraper.py",
    "scrapers/carlos_sanchez_scraper.py",
    "scrapers/developer_google_scraper.py",
    "scrapers/el_placer_del_seo_scraper.py",
    "scrapers/moz_scraper.py",
    "scrapers/search_engine_land_scraper.py"
]

def run_scrapers():
    print("--- INICIANDO EJECUCIÓN DE TODOS LOS SCRAPERS ---")
    for scraper_path in SCRAPERS:
        try:
            print(f"\n>>> Ejecutando: {scraper_path}")
            # Usamos subprocess para ejecutar cada script en su propio proceso
            # Esto asegura que si uno falla, no detiene a los demás.
            result = subprocess.run(
                [sys.executable, scraper_path],
                check=True,
                capture_output=True,
                text=True
            )
            print(f"--- Salida de {scraper_path}: ---")
            print(result.stdout)
            if result.stderr:
                print(f"--- Errores (stderr) de {scraper_path}: ---")
                print(result.stderr)
            print(f">>> {scraper_path} completado exitosamente.")
        except subprocess.CalledProcessError as e:
            print(f"!!! ERROR al ejecutar {scraper_path} !!!")
            print(e.stdout)
            print(e.stderr)
        except FileNotFoundError:
            print(f"!!! ERROR: No se encontró el script en la ruta: {scraper_path} !!!")

    print("\n--- TODOS LOS SCRAPERS HAN FINALIZADO SU EJECUCIÓN ---")

if __name__ == "__main__":
    run_scrapers()