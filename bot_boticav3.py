import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import unicodedata
import re
import concurrent.futures # <--- EL MOTOR TURBO

# --- CONFIGURACIÓN ---
NOMBRE_ARCHIVO_LISTA = "lista_minsa.txt"
URL_HOME = "https://www.hogarysalud.com.pe"
MAX_WORKERS = 5  # <--- NÚMERO DE "REPARTIDORES" SIMULTÁNEOS (No subas de 10)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9'
}

DATOS_RECOPILADOS = []
LISTA_MINSA = set()
session = requests.Session() # <--- CONEXIÓN PERSISTENTE
session.headers.update(HEADERS)

# ==========================================
# 1. HERRAMIENTAS Y FILTROS
# ==========================================
def normalizar(texto):
    if not isinstance(texto, str): return ""
    texto = texto.upper()
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def cargar_filtro_txt():
    print(f"📖 Leyendo lista segura: {NOMBRE_ARCHIVO_LISTA}...")
    global LISTA_MINSA
    try:
        with open(NOMBRE_ARCHIVO_LISTA, 'r', encoding='utf-8') as f:
            for linea in f:
                med = linea.strip()
                if len(med) > 3: LISTA_MINSA.add(normalizar(med))
        print(f"✅ Filtro cargado: {len(LISTA_MINSA)} medicamentos.")
    except FileNotFoundError:
        print(f"❌ ERROR: Crea '{NOMBRE_ARCHIVO_LISTA}' primero.")

def cumple_filtro_minsa(nombre_producto_web):
    nombre_norm = normalizar(nombre_producto_web)
    for med in LISTA_MINSA:
        if f" {med} " in f" {nombre_norm} " or nombre_norm.startswith(f"{med} ") or med == nombre_norm:
            return True
    return False

def analizar_precios(texto_precio):
    if not texto_precio: return 0.0, 0.0
    numeros = re.findall(r'(\d+\.\d{2})', texto_precio)
    if not numeros: return 0.0, 0.0
    valores = [float(n) for n in numeros]
    return min(valores), max(valores)

# ==========================================
# 2. NAVEGADOR MEJORADO
# ==========================================
def obtener_sopa(url):
    try:
        # Usamos 'session' en lugar de 'requests' directo
        response = session.get(url, timeout=20)
        if response.status_code == 200:
            return BeautifulSoup(response.content, 'html.parser')
    except: pass
    return None

def descubrir_categorias_menu():
    print(f"🌎 Conectando a {URL_HOME}...")
    soup = obtener_sopa(URL_HOME)
    if not soup: return []
    lista = []
    menu = soup.find('ul', id='menu-mega-menu-categorias')
    if menu:
        for item in menu.find_all('li', recursive=False):
            link = item.find('a', href=True)
            if link and '/c/' in link['href']:
                lista.append(link['href'])
    return lista

# ==========================================
# 3. PROCESADOR DE UN SOLO PRODUCTO (Worker)
# ==========================================
def procesar_producto_individual(datos_base):
    """ Esta función será ejecutada por múltiples hilos a la vez """
    try:
        nombre = datos_base['Nombre']
        link = datos_base['URL']
        
        # 1. Filtro (Rápido, en memoria)
        if not cumple_filtro_minsa(nombre):
            return None # Ignoramos si no es esencial

        # 2. Extracción Profunda (Lento, requiere red)
        # Pequeña pausa aleatoria para que los hilos no golpeen al unísono exacto
        time.sleep(random.uniform(0.1, 0.5)) 
        soup = obtener_sopa(link)
        
        info = {
            'Registro Sanitario': 'No especificado', 'Composición': 'No especificado',
            'Descripción': 'No especificado', 'Advertencias': 'No especificado',
            'Contraindicaciones': 'No especificado'
        }
        
        if soup:
            # Acordeones
            for item in soup.select('div.wd-accordion-item'):
                t = item.select_one('.wd-accordion-title-text')
                c = item.select_one('.woocommerce-Tabs-panel')
                if t and c:
                    tit = t.get_text(strip=True).lower()
                    cont = c.get_text(separator=' ', strip=True)
                    if 'descripci' in tit: info['Descripción'] = cont
                    elif 'advertencia' in tit: info['Advertencias'] = cont
                    elif 'contraindicaci' in tit: info['Contraindicaciones'] = cont
                    elif 'composici' in tit: info['Composición'] = cont
            
            # Tabla atributos
            for row in soup.select('tr.woocommerce-product-attributes-item'):
                th = row.select_one('th')
                td = row.select_one('td')
                if th and td:
                    label = th.get_text(strip=True).lower()
                    val = td.get_text(strip=True)
                    if 'registro' in label: info['Registro Sanitario'] = val
                    elif 'composici' in label and info['Composición'] == 'No especificado': info['Composición'] = val

        # Retornamos el diccionario completo
        datos_base.update(info)
        return datos_base

    except Exception as e:
        return None

# ==========================================
# 4. GESTOR DE CATEGORÍA (El Jefe de los Hilos)
# ==========================================
def procesar_categoria(url_cat):
    page = 1
    MAX_PAGES = 100
    nombre_cat = url_cat.strip('/').split('/')[-1].replace('-', ' ').title()
    print(f"\n📂 CATEGORÍA: {nombre_cat}")

    while page <= MAX_PAGES:
        url = url_cat if page == 1 else f"{url_cat}page/{page}/"
        soup = obtener_sopa(url)
        if not soup: break
        
        productos_html = soup.select('div.wd-product')
        if not productos_html: break
        
        print(f"  --> Pág {page}: {len(productos_html)} productos detectados. Procesando en paralelo...")
        
        # A) Preparamos la lista de tareas
        tareas = []
        for prod in productos_html:
            tag_a = prod.select_one('.wd-entities-title a')
            if not tag_a: continue
            
            tag_p = prod.select_one('.price')
            txt_p = tag_p.get_text(separator=' ', strip=True) if tag_p else ""
            p_min, p_max = analizar_precios(txt_p)
            
            datos_iniciales = {
                'Categoría': nombre_cat,
                'Nombre': tag_a.get_text(strip=True),
                'Precio Mínimo (S/)': p_min,
                'Precio Máximo (S/)': p_max,
                'URL': tag_a['href']
            }
            tareas.append(datos_iniciales)

        # B) Lanzamos los hilos (ThreadPoolExecutor)
        # Esto hace que se procesen MAX_WORKERS productos al mismo tiempo
        guardados_pagina = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            resultados = list(executor.map(procesar_producto_individual, tareas))
        
        # C) Recolectamos resultados válidos
        for res in resultados:
            if res: # Si no es None (o sea, pasó el filtro y se extrajo)
                DATOS_RECOPILADOS.append(res)
                guardados_pagina += 1
        
        print(f"      ✅ Se guardaron {guardados_pagina} esenciales de esta página.")
        
        if not soup.select_one('.next'): break
        page += 1

# ==========================================
# EJECUCIÓN
# ==========================================
cargar_filtro_txt()
if LISTA_MINSA:
    cats = descubrir_categorias_menu()
    if cats:
        start_time = time.time() # Cronómetro
        for cat in cats:
            procesar_categoria(cat)
        
        if DATOS_RECOPILADOS:
            df = pd.DataFrame(DATOS_RECOPILADOS)
            df.to_excel('catalogo_turbo_minsa.xlsx', index=False)
            mins = (time.time() - start_time) / 60
            print(f"\n🏁 ¡TERMINADO EN {mins:.2f} MINUTOS!")
            print(f"Archivo guardado: catalogo_turbo_minsa.xlsx")
        else:
            print("⚠️ No se encontraron datos.")
else:
    print("❌ Lista MINSA vacía.")