import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import unicodedata

# --- CONFIGURACIÓN ---
NOMBRE_ARCHIVO_LISTA = "lista_minsa.txt"
URL_HOME = "https://www.hogarysalud.com.pe"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9'
}

DATOS_RECOPILADOS = []
LISTA_MINSA = set()

# ==========================================
# 1. HERRAMIENTAS DE TEXTO (Normalización)
# ==========================================
def normalizar(texto):
    """ Quita tildes, pasa a mayúsculas y limpia espacios """
    if not isinstance(texto, str): return ""
    texto = texto.upper()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

# ==========================================
# 2. CARGADOR DE LISTA (Filtro TXT)
# ==========================================
def cargar_filtro_txt():
    print(f"📖 Leyendo lista de medicamentos esenciales desde: {NOMBRE_ARCHIVO_LISTA}...")
    global LISTA_MINSA
    try:
        with open(NOMBRE_ARCHIVO_LISTA, 'r', encoding='utf-8') as f:
            for linea in f:
                med = linea.strip()
                if len(med) > 3:
                    LISTA_MINSA.add(normalizar(med))
        print(f"✅ Filtro cargado: {len(LISTA_MINSA)} medicamentos seguros.")
    except FileNotFoundError:
        print(f"❌ ERROR: No existe '{NOMBRE_ARCHIVO_LISTA}'. Crea el archivo primero.")

def cumple_filtro_minsa(nombre_producto_web):
    """ Compara el producto web contra tu lista TXT normalizada """
    nombre_norm = normalizar(nombre_producto_web)
    for med_clave in LISTA_MINSA:
        # Lógica de coincidencia robusta (evita falsos positivos parciales)
        if f" {med_clave} " in f" {nombre_norm} " or \
           nombre_norm.startswith(f"{med_clave} ") or \
           med_clave == nombre_norm:
            return True
    return False

# ==========================================
# 3. EL NAVEGADOR (Crawler Mejorado)
# ==========================================
def obtener_sopa(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=25)
        if response.status_code == 200:
            return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print(f"⚠️ Error conectando a {url}: {e}")
    return None

def descubrir_categorias_menu():
    """ Usa la lógica del primer código que funcionaba mejor para detectar el menú principal """
    print(f"🌎 Conectando a {URL_HOME} para leer el menú...")
    soup = obtener_sopa(URL_HOME)
    
    if not soup: return []

    lista_urls_final = []
    
    # Buscamos el contenedor del menú por su ID específico
    menu_container = soup.find('ul', id='menu-mega-menu-categorias')
    
    if menu_container:
        print("✅ Menú encontrado. Filtrando solo categorías principales...")
        # recursive=False es clave para no tomar subcategorías
        items_principales = menu_container.find_all('li', recursive=False)
        
        for item in items_principales:
            enlace = item.find('a', href=True)
            if enlace:
                url = enlace['href']
                texto = enlace.get_text(strip=True)
                
                # Validación de seguridad
                if '/c/' in url and 'hogarysalud.com.pe' in url:
                    lista_urls_final.append(url)
                    print(f"   🔹 Categoría detectada: {texto}")
    else:
        print("⚠️ No se pudo leer el menú principal (ID no encontrado).")

    print(f"📋 Total: {len(lista_urls_final)} categorías listas para procesar.")
    return lista_urls_final

# ==========================================
# 4. EXTRACTOR DE DETALLES (Deep Scraping)
# ==========================================
def extraer_detalles_profundos(soup_producto):
    """ Extrae Registro Sanitario, Composición, etc. del detalle del producto """
    info = {
        'Descripción': 'No especificado',
        'Advertencias': 'No especificado',
        'Contraindicaciones': 'No especificado',
        'Composición': 'No especificado',
        'Registro Sanitario': 'No especificado'
    }
    
    if not soup_producto: return info

    # A) BUSCAR EN ACORDEONES (Pestañas desplegables)
    items_acordeon = soup_producto.select('div.wd-accordion-item')
    for item in items_acordeon:
        try:
            titulo_tag = item.select_one('.wd-accordion-title-text')
            contenido_tag = item.select_one('.woocommerce-Tabs-panel')
            
            if titulo_tag and contenido_tag:
                titulo = titulo_tag.get_text(strip=True).lower()
                contenido = contenido_tag.get_text(separator=' ', strip=True)
                
                if 'descripci' in titulo: info['Descripción'] = contenido
                elif 'advertencia' in titulo: info['Advertencias'] = contenido
                elif 'contraindicaci' in titulo: info['Contraindicaciones'] = contenido
                elif 'composici' in titulo: info['Composición'] = contenido
        except: continue

    # B) BUSCAR EN TABLA DE ATRIBUTOS (Registro Sanitario)
    tabla_atributos = soup_producto.select('tr.woocommerce-product-attributes-item')
    for fila in tabla_atributos:
        try:
            texto_label = fila.select_one('th').get_text(strip=True).lower()
            texto_valor = fila.select_one('td').get_text(strip=True)
            
            if 'registro' in texto_label or 'sanitario' in texto_label:
                info['Registro Sanitario'] = texto_valor
            elif 'composici' in texto_label and info['Composición'] == 'No especificado':
                info['Composición'] = texto_valor
        except: continue

    return info

# ==========================================
# 5. PROCESADOR PRINCIPAL (Mejorado)
# ==========================================
def procesar_categoria(url_categoria):
    page = 1
    MAX_PAGES = 100 # Ajusta según necesites
    
    # Obtener nombre limpio de la categoría
    nombre_cat = url_categoria.strip('/').split('/')[-1].replace('-', ' ').title()
    print(f"\n📂 PROCESANDO: {nombre_cat}")
    
    while page <= MAX_PAGES:
        url_actual = url_categoria if page == 1 else f"{url_categoria}page/{page}/"
        soup = obtener_sopa(url_actual)
        
        if not soup: break
        
        # Selector de productos
        productos = soup.select('div.wd-product')
        if not productos: break
            
        print(f"  --> Pág {page}: {len(productos)} productos detectados.")
        
        contador_guardados = 0
        for prod in productos:
            try:
                # 1. Extracción Básica
                tag_titulo = prod.select_one('.wd-entities-title a')
                if not tag_titulo: continue
                
                nombre = tag_titulo.get_text(strip=True)
                
                # 2. FILTRO MINSA (Solo procesamos si es esencial)
                if not cumple_filtro_minsa(nombre):
                    continue 
                
                link = tag_titulo['href']
                tag_precio = prod.select_one('.price')
                precio = tag_precio.get_text(separator=' ', strip=True) if tag_precio else "0"
                
                # 3. EXTRACCIÓN PROFUNDA (Entrar al link)
                time.sleep(random.uniform(0.5, 1.0)) # Pausa antibloqueo
                soup_detalle = obtener_sopa(link)
                detalles = extraer_detalles_profundos(soup_detalle)
                
                # 4. Consolidación de datos
                item_final = {
                    'Categoría': nombre_cat,
                    'Nombre': nombre,
                    'Precio': precio,
                    'Registro Sanitario': detalles['Registro Sanitario'],
                    'Composición': detalles['Composición'],
                    'Descripción': detalles['Descripción'],
                    'Advertencias': detalles['Advertencias'],
                    'Contraindicaciones': detalles['Contraindicaciones'],
                    'URL': link
                }
                
                DATOS_RECOPILADOS.append(item_final)
                contador_guardados += 1
                
            except Exception: continue
        
        print(f"      ✅ Guardados: {contador_guardados} productos esenciales.")
        
        # Paginación
        if not soup.select_one('.next'): break
        page += 1

# ==========================================
# EJECUCIÓN MAESTRA
# ==========================================
# 1. Cargamos el filtro TXT
cargar_filtro_txt()

if LISTA_MINSA:
    # 2. Obtenemos las categorías principales
    cats = descubrir_categorias_menu()
    
    if cats:
        print(f"🚀 Iniciando extracción filtrada en {len(cats)} categorías...")
        for cat in cats:
            procesar_categoria(cat)
            time.sleep(2) # Respiro entre categorías
        
        # 3. Guardado final
        if DATOS_RECOPILADOS:
            df = pd.DataFrame(DATOS_RECOPILADOS)
            archivo_final = 'catalogo_minsa_completo.xlsx'
            df.to_excel(archivo_final, index=False)
            print(f"\n🎉 ¡ÉXITO! Se ha generado: {archivo_final}")
            print(f"Total de productos procesados: {len(DATOS_RECOPILADOS)}")
        else:
            print("\n⚠️ El script terminó, pero no encontró coincidencias con tu lista del MINSA.")
    else:
        print("❌ No se encontraron categorías en la web.")
else:
    print("❌ Lista MINSA vacía o archivo no encontrado.")