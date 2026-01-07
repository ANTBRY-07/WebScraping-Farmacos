import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

# --- CONFIGURACIÓN ---
# URL (URL de las categorias de la pagina)
BASE_URL = "https://www.hogarysalud.com.pe/c/nutricion/"

# Cabeceras para simular un navegador real
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

def extraer_detalle_producto(url_producto):
    """ Entra a la ficha del producto para sacar composición y advertencias """
    soup = obtener_sopa(url_producto)
    if not soup: return "No accesible", "No accesible"
    
    composicion = "No especificado"
    advertencias = "No especificado"
    
    # Buscamos títulos dentro de la descripción (H2, H3, H4 o Strong)
    # Esta parte busca palabras clave en el contenido
    textos_clave = soup.find_all(['h2', 'h3', 'h4', 'strong', 'b'])
    
    for t in textos_clave:
        texto = t.get_text(strip=True).lower()
        
        # Lógica para encontrar el texto que sigue al título
        if 'composición' in texto:
            # Buscamos el siguiente elemento que contenga texto
            siguiente = t.find_next_sibling()
            if siguiente: composicion = siguiente.get_text(strip=True)
            
        elif 'advertencias' in texto or 'contraindicaciones' in texto:
            siguiente = t.find_next_sibling()
            if siguiente: advertencias = siguiente.get_text(strip=True)
            
    return composicion, advertencias

def escanear_catalogo():
    page = 1
    # ⚠️ IMPORTANTE: Pon aquí cuántas páginas quieres escanear (ej. 5). 
    # Si pones 100, tardará bastante.
    MAX_PAGES = 11
    
    while page <= MAX_PAGES:
        # Construcción de la URL de paginación
        if page == 1:
            url_actual = BASE_URL
        else:
            url_actual = f"{BASE_URL}page/{page}/"
            
        print(f"\n--- 📄 PROCESANDO PÁGINA {page} ---")
        soup = obtener_sopa(url_actual)
        
        if not soup: break
        
        # --- SELECTOR MAESTRO ---
        # Buscamos 'div' que tenga la clase 'wd-product'
        productos = soup.select('div.wd-product')
        
        if not productos:
            print("🛑 No se encontraron más productos. Fin del escaneo.")
            break
            
        print(f"🔍 Encontrados {len(productos)} productos. Extrayendo datos...")
        
        for prod in productos:
            try:
                # 1. TÍTULO Y LINK 
                # Buscamos la clase 'wd-entities-title'
                tag_titulo = prod.select_one('.wd-entities-title a')
                
                if not tag_titulo: continue # Si no tiene título, saltamos
                
                nombre = tag_titulo.get_text(strip=True)
                link = tag_titulo['href']
                
                # 2. PRECIO
                # Buscamos la clase 'price'
                tag_precio = prod.select_one('.price')
                precio = tag_precio.get_text(strip=True) if tag_precio else "Agotado/Sin precio"
                
                # 3. EXTRAER DETALLES (Entrando al link)
                # Hacemos una pausa pequeña para no saturar
                time.sleep(random.uniform(0.5, 1.0))
                comp, adv = extraer_detalle_producto(link)
                
                # Guardar en memoria
                DATOS_RECOPILADOS.append({
                    'Nombre': nombre,
                    'Precio': precio,
                    'Composición': comp,
                    'Info Importante': adv,
                    'URL': link
                })
                
                print(f"✅ {nombre[:30]}... | {precio}")
                
            except Exception as e:
                print(f"⚠️ Error en un producto: {e}")
                continue
        
        page += 1

# --- EJECUCIÓN ---
print("🚀 Iniciando Robot Farmacéutico...")
escanear_catalogo()

# --- GUARDADO ---
if DATOS_RECOPILADOS:
    df = pd.DataFrame(DATOS_RECOPILADOS)
    nombre_archivo = 'reporte_farmacia_final.csv'
    df.to_csv(nombre_archivo, index=False, encoding='utf-8-sig')
    print(f"\n🎉 ¡ÉXITO! Se generó el archivo '{nombre_archivo}' con {len(DATOS_RECOPILADOS)} productos.")
else:
    print("\nNo se pudieron extraer datos. Revisa tu conexión.")