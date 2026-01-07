import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

# --- CONFIGURACIÓN ---
BASE_URL = "https://www.hogarysalud.com.pe/c/nutricion/"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9'
}

DATOS_RECOPILADOS = []

def obtener_sopa(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        if response.status_code == 200:
            return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"❌ Error conectando a {url}: {e}")
    return None

def extraer_info_acordeon(url_producto):
    """
    Entra al producto y extrae TODAS las pestañas del acordeón dinámicamente.
    Devuelve un diccionario (ej: {'Composición': '...', 'Advertencias': '...'})
    """
    soup = obtener_sopa(url_producto)
    info_extra = {} # Diccionario vacío para guardar lo que encontremos
    
    if not soup: return info_extra
    
    # 1. Buscamos cada bloque del acordeón
    items_acordeon = soup.select('div.wd-accordion-item')
    
    for item in items_acordeon:
        try:
            # 2. Extraer el Título (Composición, Advertencias, etc.)
            titulo_tag = item.select_one('.wd-accordion-title-text')
            if not titulo_tag: continue
            
            titulo_texto = titulo_tag.get_text(strip=True)
            
            # 3. Extraer el Contenido (El texto oculto)
            contenido_tag = item.select_one('.woocommerce-Tabs-panel')
            
            if contenido_tag:
                # Usamos separator=' ' para que los párrafos no se peguen
                contenido_texto = contenido_tag.get_text(separator=' ', strip=True)
                
                # Guardamos en el diccionario: Clave = Título, Valor = Texto
                info_extra[titulo_texto] = contenido_texto
                
        except Exception as e:
            continue
            
    return info_extra

def escanear_catalogo():
    page = 1
    MAX_PAGES = 11 # Aumenta esto cuando quieras todo el catálogo
    
    while page <= MAX_PAGES:
        if page == 1: url = BASE_URL
        else: url = f"{BASE_URL}page/{page}/"
            
        print(f"\n--- 📄 PROCESANDO PÁGINA {page} ---")
        soup = obtener_sopa(url)
        if not soup: break
        
        # Selector de productos (Confirmado que funciona)
        productos = soup.select('div.wd-product')
        if not productos: break
            
        print(f"🔍 Encontrados {len(productos)} productos...")
        
        for prod in productos:
            try:
                # Datos básicos
                tag_titulo = prod.select_one('.wd-entities-title a')
                if not tag_titulo: continue
                
                nombre = tag_titulo.get_text(strip=True)
                link = tag_titulo['href']
                
                tag_precio = prod.select_one('.price')
                # Limpieza de precio para que Excel lo entienda mejor
                precio = tag_precio.get_text(separator=' ', strip=True) if tag_precio else "0"
                
                # --- AQUÍ LA MAGIA: Extraer datos dinámicos ---
                # Entramos al link y traemos el diccionario con todo lo que haya
                diccionario_info = extraer_info_acordeon(link)
                
                # Creamos el objeto final fusionando datos básicos + datos del acordeón
                item_final = {
                    'Nombre': nombre,
                    'Precio': precio,
                    'URL': link
                }
                # Fusionamos el diccionario de info extra (Advertencias, Composición, etc.)
                item_final.update(diccionario_info)
                
                DATOS_RECOPILADOS.append(item_final)
                
                print(f"✅ {nombre[:30]}... | Info extraída: {list(diccionario_info.keys())}")
                
                time.sleep(random.uniform(0.5, 1.2)) # Pausa de cortesía
                
            except Exception as e:
                print(f"⚠️ Error: {e}")
                continue
        
        page += 1

# --- EJECUCIÓN ---
print("🚀 Iniciando Scraper Inteligente...")
escanear_catalogo()

# --- GUARDADO EN EXCEL (.xlsx) ---
if DATOS_RECOPILADOS:
    df = pd.DataFrame(DATOS_RECOPILADOS)
    
    # Esto guardará directamente en formato Excel con celdas separadas
    nombre_archivo = 'catalogo_completo_dinamico.xlsx'
    df.to_excel(nombre_archivo, index=False)
    
    print(f"\n🎉 ¡ÉXITO! Se generó '{nombre_archivo}'.")
    print("Nota: Las columnas se crearon automáticamente según la info encontrada.")
else:
    print("\nNo se encontraron datos.")