import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import unicodedata
import re
import concurrent.futures  # <--- Librería para ejecución paralela (Multithreading)

# ==============================================================================
# CONFIGURACIÓN GENERAL DEL SCRIPT
# ==============================================================================

# Nombre del archivo de texto que contiene la lista de medicamentos del MINSA
NOMBRE_ARCHIVO_LISTA = "lista_minsa.txt"

# URL base de la farmacia a scrapear
URL_HOME = "https://www.hogarysalud.com.pe"

# Configuración del motor de paralelismo
# NOTA: Mantener entre 5 y 10 workers. Un número mayor podría causar
# que el servidor bloquee tu dirección IP por "Denegación de Servicio" (DoS).
MAX_WORKERS = 5

# Cabeceras HTTP para simular un navegador real y evitar bloqueos básicos
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9'
}

# Variables globales para almacenamiento
DATOS_RECOPILADOS = []
LISTA_MINSA = set()

# Configuración de la sesión HTTP
# Usar 'Session' permite reutilizar la conexión TCP (Keep-Alive),
# lo que acelera significativamente las peticiones múltiples.
session = requests.Session()
session.headers.update(HEADERS)


# ==============================================================================
# 1. BLOQUE DE HERRAMIENTAS Y FILTROS DE TEXTO
# ==============================================================================

def normalizar(texto):
    """
    Elimina tildes y convierte el texto a mayúsculas para facilitar comparaciones.
    Ejemplo: 'Ácido' -> 'ACIDO'
    
    Args:
        texto (str): El texto original.
        
    Returns:
        str: Texto limpio y normalizado.
    """
    if not isinstance(texto, str):
        return ""
    
    texto = texto.upper()
    
    # Normalización NFD para separar caracteres base de sus acentos
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')


def cargar_filtro_txt():
    """
    Lee el archivo de texto línea por línea y carga los medicamentos en memoria (Set).
    Se usa un 'Set' (conjunto) porque la búsqueda es mucho más rápida que en una lista.
    """
    print(f"📖 Leyendo lista segura: {NOMBRE_ARCHIVO_LISTA}...")
    global LISTA_MINSA
    
    try:
        with open(NOMBRE_ARCHIVO_LISTA, 'r', encoding='utf-8') as f:
            for linea in f:
                med = linea.strip()
                # Solo guardamos si tiene una longitud mínima para evitar ruido
                if len(med) > 3:
                    LISTA_MINSA.add(normalizar(med))
                    
        print(f"✅ Filtro cargado: {len(LISTA_MINSA)} medicamentos listos para filtrar.")
        
    except FileNotFoundError:
        print(f"❌ ERROR CRÍTICO: No se encontró el archivo '{NOMBRE_ARCHIVO_LISTA}'. Crea el archivo antes de continuar.")


def cumple_filtro_minsa(nombre_producto_web):
    """
    Verifica si el nombre del producto encontrado en la web contiene
    alguna de las palabras clave de la lista del MINSA.
    
    Args:
        nombre_producto_web (str): Nombre extraído de la web.
        
    Returns:
        bool: True si es un medicamento esencial, False si no lo es.
    """
    nombre_norm = normalizar(nombre_producto_web)
    
    for med in LISTA_MINSA:
        # Verificamos coincidencia exacta de palabra o inicio de frase
        # para evitar falsos positivos (ej: que "AJO" active "BAJO").
        if f" {med} " in f" {nombre_norm} " or nombre_norm.startswith(f"{med} ") or med == nombre_norm:
            return True
            
    return False


def analizar_precios(texto_precio):
    """
    Extrae los valores numéricos de una cadena de texto de precio.
    Maneja rangos de precios (ej: "S/ 10.00 - S/ 20.00").
    
    Args:
        texto_precio (str): Texto bruto del precio (ej: "S/ 12.50").
        
    Returns:
        tuple: (precio_minimo, precio_maximo) como flotantes.
    """
    if not texto_precio:
        return 0.0, 0.0
    
    # Regex para encontrar números con formato decimal (ej: 10.50)
    numeros = re.findall(r'(\d+\.\d{2})', texto_precio)
    
    if not numeros:
        return 0.0, 0.0
    
    # Convertimos strings a floats
    valores = [float(n) for n in numeros]
    
    # Devolvemos el mínimo y máximo encontrado
    return min(valores), max(valores)


# ==============================================================================
# 2. BLOQUE DE NAVEGACIÓN WEB (CRAWLER)
# ==============================================================================

def obtener_sopa(url):
    """
    Realiza una petición GET a la URL y devuelve el objeto BeautifulSoup.
    Maneja excepciones silenciosamente para no detener el flujo masivo.
    
    Args:
        url (str): Dirección web a consultar.
        
    Returns:
        BeautifulSoup object | None: El HTML parseado o None si falló.
    """
    try:
        # Usamos 'session' para mantener cookies y conexión viva
        response = session.get(url, timeout=20)
        
        if response.status_code == 200:
            return BeautifulSoup(response.content, 'html.parser')
            
    except Exception:
        # En scraping masivo, a veces es mejor ignorar errores puntuales
        pass
        
    return None


def descubrir_categorias_menu():
    """
    Escanea la página principal para encontrar los enlaces de las categorías.
    Se enfoca en el menú específico 'menu-mega-menu-categorias' para evitar enlaces basura.
    
    Returns:
        list: Lista de URLs de categorías.
    """
    print(f"🌎 Conectando a {URL_HOME} para descubrir catálogo...")
    
    soup = obtener_sopa(URL_HOME)
    if not soup:
        return []
    
    lista_links = []
    
    # Buscamos el contenedor específico del menú
    menu = soup.find('ul', id='menu-mega-menu-categorias')
    
    if menu:
        # 'recursive=False' asegura que solo tomamos las categorías principales
        # y no sub-niveles que podrían duplicar la búsqueda.
        for item in menu.find_all('li', recursive=False):
            link = item.find('a', href=True)
            
            # Filtro adicional para asegurar que es un link de categoría válida
            if link and '/c/' in link['href']:
                lista_links.append(link['href'])
                
    return lista_links


# ==============================================================================
# 3. LÓGICA DE PROCESAMIENTO PARALELO (WORKER)
# ==============================================================================

def procesar_producto_individual(datos_base):
    """
    FUNCIÓN PRINCIPAL DEL HILO (WORKER).
    Esta función se ejecuta de forma paralela para múltiples productos a la vez.
    Realiza el filtrado y, si pasa, entra al detalle del producto (Deep Scraping).
    
    Args:
        datos_base (dict): Diccionario con Nombre, URL y Precio inicial.
        
    Returns:
        dict | None: Datos completos del producto o None si fue filtrado/falló.
    """
    try:
        nombre = datos_base['Nombre']
        link = datos_base['URL']
        
        # --- PASO 1: FILTRADO RÁPIDO ---
        # Verificamos si el producto está en la lista del MINSA antes de hacer
        # la petición web, para ahorrar tiempo y recursos.
        if not cumple_filtro_minsa(nombre):
            return None # Se descarta el producto
            
        # --- PASO 2: EXTRACCIÓN PROFUNDA (DEEP SCRAPING) ---
        # Pausa aleatoria para simular comportamiento humano y evitar bloqueos
        time.sleep(random.uniform(0.1, 0.5)) 
        
        soup = obtener_sopa(link)
        
        # Diccionario por defecto para campos opcionales
        info_adicional = {
            'Registro Sanitario': 'No especificado',
            'Composición': 'No especificado',
            'Descripción': 'No especificado',
            'Advertencias': 'No especificado',
            'Contraindicaciones': 'No especificado'
        }
        
        if soup:
            # A. Buscar información en los acordeones (pestañas desplegables)
            # ------------------------------------------------------------
            for item in soup.select('div.wd-accordion-item'):
                t_tag = item.select_one('.wd-accordion-title-text')
                c_tag = item.select_one('.woocommerce-Tabs-panel')
                
                if t_tag and c_tag:
                    titulo = t_tag.get_text(strip=True).lower()
                    contenido = c_tag.get_text(separator=' ', strip=True)
                    
                    # Asignación dinámica según palabras clave en el título
                    if 'descripci' in titulo:
                        info_adicional['Descripción'] = contenido
                    elif 'advertencia' in titulo:
                        info_adicional['Advertencias'] = contenido
                    elif 'contraindicaci' in titulo:
                        info_adicional['Contraindicaciones'] = contenido
                    elif 'composici' in titulo:
                        info_adicional['Composición'] = contenido
            
            # B. Buscar información en la tabla de atributos técnicos
            # ------------------------------------------------------------
            for row in soup.select('tr.woocommerce-product-attributes-item'):
                th = row.select_one('th')
                td = row.select_one('td')
                
                if th and td:
                    label = th.get_text(strip=True).lower()
                    valor = td.get_text(strip=True)
                    
                    if 'registro' in label:
                        info_adicional['Registro Sanitario'] = valor
                    elif 'composici' in label and info_adicional['Composición'] == 'No especificado':
                        info_adicional['Composición'] = valor

        # Fusionamos los datos base con la información extraída
        datos_base.update(info_adicional)
        
        return datos_base

    except Exception as e:
        # Si algo falla dentro del hilo, retornamos None para no romper el proceso
        return None


# ==============================================================================
# 4. GESTOR DE CATEGORÍA Y PAGINACIÓN (MANAGER)
# ==============================================================================

def procesar_categoria(url_cat):
    """
    Controla la paginación de una categoría y distribuye los productos
    encontrados a los 'Workers' para su procesamiento en paralelo.
    
    Args:
        url_cat (str): URL de la categoría a procesar.
    """
    page = 1
    MAX_PAGES = 100 # Límite de seguridad
    
    # Extraemos el nombre limpio de la categoría desde la URL
    nombre_cat = url_cat.strip('/').split('/')[-1].replace('-', ' ').title()
    print(f"\n📂 PROCESANDO CATEGORÍA: {nombre_cat}")

    while page <= MAX_PAGES:
        # Construcción de la URL paginada
        url_actual = url_cat if page == 1 else f"{url_cat}page/{page}/"
        
        soup = obtener_sopa(url_actual)
        if not soup:
            break
        
        # Selector para encontrar las "cajas" de los productos
        productos_html = soup.select('div.wd-product')
        
        if not productos_html:
            print("   -> No se encontraron más productos. Fin de categoría.")
            break
        
        print(f"  --> Pág {page}: {len(productos_html)} productos detectados. Iniciando análisis paralelo...")
        
        # --- FASE A: PREPARACIÓN DE TAREAS ---
        # Recopilamos la info básica de cada producto en la rejilla
        tareas_para_workers = []
        
        for prod in productos_html:
            tag_a = prod.select_one('.wd-entities-title a')
            if not tag_a: continue
            
            # Extracción y limpieza de precios
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
            tareas_para_workers.append(datos_iniciales)

        # --- FASE B: EJECUCIÓN PARALELA (ThreadPoolExecutor) ---
        # Aquí es donde ocurre la magia de la velocidad. Se lanzan múltiples hilos.
        guardados_pagina = 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Mapeamos la función 'procesar_producto_individual' a cada tarea
            resultados = list(executor.map(procesar_producto_individual, tareas_para_workers))
        
        # --- FASE C: RECOLECCIÓN DE RESULTADOS ---
        # Filtramos los 'None' (productos que no pasaron el filtro o fallaron)
        for res in resultados:
            if res: 
                DATOS_RECOPILADOS.append(res)
                guardados_pagina += 1
        
        print(f"      ✅ Se guardaron {guardados_pagina} productos esenciales de esta página.")
        
        # Verificar si existe botón de 'Siguiente página'
        if not soup.select_one('.next'):
            break
            
        page += 1


# ==============================================================================
# 5. BLOQUE DE EJECUCIÓN PRINCIPAL
# ==============================================================================

if __name__ == "__main__":
    # 1. Cargar base de datos del MINSA
    cargar_filtro_txt()
    
    if LISTA_MINSA:
        # 2. Obtener categorías de la web
        cats = descubrir_categorias_menu()
        
        if cats:
            print(f"\n🚀 INICIANDO SCRAPING MASIVO CON {MAX_WORKERS} HILOS...")
            start_time = time.time() # Iniciar cronómetro
            
            # 3. Procesar cada categoría encontrada
            for cat in cats:
                procesar_categoria(cat)
            
            # 4. Guardar resultados
            if DATOS_RECOPILADOS:
                print("\n💾 Guardando datos en Excel...")
                df = pd.DataFrame(DATOS_RECOPILADOS)
                
                nombre_archivo = 'catalogo_turbo_minsa.xlsx'
                df.to_excel(nombre_archivo, index=False)
                
                # Cálculo de tiempo total
                mins = (time.time() - start_time) / 60
                print(f"\n🏁 ¡PROCESO TERMINADO EN {mins:.2f} MINUTOS!")
                print(f"📄 Archivo generado: {nombre_archivo}")
                print(f"📦 Total productos: {len(DATOS_RECOPILADOS)}")
                
            else:
                print("\n⚠️ El script finalizó pero no encontró coincidencias con la lista del MINSA.")
        else:
            print("\n❌ No se pudieron detectar categorías en la página web.")
    else:
        print("\n❌ La lista del MINSA está vacía o no se pudo cargar.")