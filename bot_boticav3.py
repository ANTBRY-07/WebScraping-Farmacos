import requests
import pandas as pd
import time
import random
import unicodedata
import re
import concurrent.futures
from bs4 import BeautifulSoup


# ==============================================================================
# SECCIÓN 1: CONFIGURACIÓN Y CONSTANTES GLOBALES
# ==============================================================================

# Nombre del archivo de texto que contiene la base de datos de medicamentos MINSA
FILE_MINSA = "lista_minsa.txt"

# URL base del sitio web objetivo para el scraping
URL_HOME = "https://www.hogarysalud.com.pe"

# Cabeceras HTTP para simular una navegación humana legítima
# Esto ayuda a evitar bloqueos por parte del servidor (Error 403 Forbidden)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'es-ES,es;q=0.9'
}

# Número máximo de hilos simultáneos (Workers)
# Se recomienda mantener entre 5 y 10 para no saturar el servidor destino
MAX_WORKERS = 5

# Estructuras de datos globales para almacenamiento en memoria
lista_minsa = set()          # Conjunto para búsqueda rápida O(1) de medicamentos
datos_recopilados = []       # Lista para almacenar los diccionarios de productos encontrados
URLS_VISTAS = set()          # Set para registrar URLs y evitar procesar duplicados en tiempo real

# Configuración de la sesión HTTP persistente
# Permite reutilizar la conexión TCP (Keep-Alive) para mayor velocidad
session = requests.Session()
session.headers.update(HEADERS)


# ==============================================================================
# SECCIÓN 2: FUNCIONES DE UTILIDAD Y NORMALIZACIÓN
# ==============================================================================

def normalizar(txt):
    """
    Normaliza una cadena de texto para facilitar comparaciones insensibles a formato.
    
    Proceso:
    1. Convierte todo el texto a mayúsculas.
    2. Elimina acentos y diacríticos (ej: Á -> A, ñ -> n).
    
    Args:
        txt (str): Texto original.
        
    Returns:
        str: Texto limpio y normalizado.
    """
    # Validación preventiva: si no es texto, retornar cadena vacía
    if not isinstance(txt, str): 
        return ""
    
    # Normalización Unicode NFD para separar caracteres base de sus acentos
    texto_normalizado = unicodedata.normalize('NFD', txt.upper())
    
    # Filtrado de caracteres: nos quedamos solo con los que NO son marcas diacríticas ('Mn')
    return ''.join(c for c in texto_normalizado if unicodedata.category(c) != 'Mn')


def cargar_filtro():
    """
    Lee el archivo de texto local y carga los medicamentos en memoria.
    Utiliza un 'Set' (conjunto) para optimizar la velocidad de búsqueda.
    """
    try:
        print(f"📖 Leyendo archivo de filtro: {FILE_MINSA}...")
        
        with open(FILE_MINSA, 'r', encoding='utf-8') as f:
            global lista_minsa
            
            # Comprensión de conjuntos para cargar y limpiar en una sola pasada
            # Filtramos líneas menores a 3 caracteres para evitar ruido
            lista_minsa = {normalizar(line.strip()) for line in f if len(line.strip()) > 3}
            
        print(f"✅ Filtro cargado exitosamente: {len(lista_minsa)} medicamentos listos.")
        
    except FileNotFoundError:
        print(f"❌ ERROR CRÍTICO: No se encontró el archivo '{FILE_MINSA}'.")
        print("   Por favor, asegúrate de que el archivo existe en la carpeta del script.")


def cumple_filtro(nombre):
    """
    Determina si un producto de la web coincide con la lista del MINSA.
    
    Args:
        nombre (str): Nombre del producto extraído de la web.
        
    Returns:
        bool: True si hay coincidencia, False en caso contrario.
    """
    # Normalizamos el nombre que viene de la web
    n = normalizar(nombre)
    
    # Iteramos sobre la lista segura para buscar coincidencias
    # Se verifica: coincidencia exacta O palabra contenida con espacios (para evitar falsos positivos)
    for m in lista_minsa:
        
        condicion_exacta = (m == n)
        condicion_contenido = (f" {m} " in f" {n} ")
        condicion_inicio = n.startswith(f"{m} ")
        
        if condicion_exacta or condicion_contenido or condicion_inicio:
            return True
            
    return False


def get_precios(txt):
    """
    Analiza una cadena de texto de precio y extrae los valores numéricos.
    Maneja rangos de precios (ej: "S/ 10.00 - S/ 20.00").
    
    Args:
        txt (str): Texto crudo del precio.
        
    Returns:
        tuple: (precio_minimo, precio_maximo) como floats.
    """
    if not txt:
        return 0.0, 0.0
        
    # Regex para capturar números con formato decimal (ej: 14.50)
    vals = [float(x) for x in re.findall(r'(\d+\.\d{2})', txt)]
    
    if vals:
        return min(vals), max(vals)
    else:
        return 0.0, 0.0


def get_soup(url):
    """
    Realiza la petición HTTP GET de forma segura.
    
    Args:
        url (str): URL a consultar.
        
    Returns:
        BeautifulSoup object | None: Objeto parseado o None si falló.
    """
    try:
        # Timeout de 20 segundos para evitar bloqueos infinitos
        r = session.get(url, timeout=20)
        
        if r.status_code == 200:
            return BeautifulSoup(r.content, 'html.parser')
        else:
            return None
            
    except Exception: 
        # En caso de error de red, retornamos None silenciosamente
        return None


# ==============================================================================
# SECCIÓN 3: LÓGICA DE PROCESAMIENTO PARALELO (WORKER)
# ==============================================================================

def procesar_producto(data):
    """
    Función Worker: Procesa un único producto.
    1. Verifica si cumple el filtro MINSA.
    2. Si cumple, entra a la URL (Deep Scraping).
    3. Extrae detalles técnicos (Registro, Composición, etc.).
    
    Args:
        data (dict): Datos básicos del producto (Nombre, URL, Precio).
        
    Returns:
        dict | None: Datos enriquecidos o None si fue descartado.
    """
    
    # --- PASO 1: FILTRADO PREVIO ---
    # Si el nombre no está en la lista MINSA, abortamos para ahorrar recursos
    if not cumple_filtro(data['Nombre']): 
        return None
    
    # Pequeña pausa aleatoria para comportamiento humano
    time.sleep(random.uniform(0.1, 0.5))
    
    # --- PASO 2: EXTRACCIÓN PROFUNDA ---
    soup = get_soup(data['URL'])
    
    # Diccionario por defecto para campos que podrían no existir
    info = {
        'Registro Sanitario': 'No especificado', 
        'Composición': 'No especificado', 
        'Descripción': 'No especificado', 
        'Advertencias': 'No especificado', 
        'Contraindicaciones': 'No especificado'
    }
    
    if soup:
        
        # A. Búsqueda en Acordeones (Pestañas desplegables)
        # -------------------------------------------------
        map_titles = {
            'descripci': 'Descripción', 
            'advertencia': 'Advertencias', 
            'contraindicaci': 'Contraindicaciones', 
            'composici': 'Composición'
        }
        
        for item in soup.select('div.wd-accordion-item'):
            t = item.select_one('.wd-accordion-title-text')
            c = item.select_one('.woocommerce-Tabs-panel')
            
            if t and c:
                txt_t = t.get_text(strip=True).lower()
                
                # Asignación dinámica según el título del acordeón
                for key, val in map_titles.items():
                    if key in txt_t: 
                        info[val] = c.get_text(separator=' ', strip=True)

        # B. Búsqueda en Tabla de Atributos (Info Técnica)
        # -------------------------------------------------
        for row in soup.select('tr.woocommerce-product-attributes-item'):
            th = row.select_one('th')
            td = row.select_one('td')
            
            if th and td:
                lbl = th.get_text(strip=True).lower()
                val = td.get_text(strip=True)
                
                # Extracción específica de Registro Sanitario
                if 'registro' in lbl: 
                    info['Registro Sanitario'] = val
                    
                # Respaldo para Composición si no se encontró en acordeones
                elif 'composici' in lbl and info['Composición'] == 'No especificado': 
                    info['Composición'] = val

    # Fusionamos los datos extraídos con los datos base
    data.update(info)
    
    return data


# ==============================================================================
# SECCIÓN 4: GESTOR DE CATEGORÍAS Y PAGINACIÓN (MANAGER)
# ==============================================================================

def procesar_categoria(url):
    """
    Itera sobre todas las páginas de una categoría específica.
    Recolecta productos y delega el procesamiento detallado a los Workers.
    
    Args:
        url (str): URL de la categoría a procesar.
    """
    
    # Extraemos un nombre legible de la categoría desde la URL
    nombre_cat = url.strip('/').split('/')[-1].replace('-', ' ').title()
    print(f"\n📂 CATEGORÍA DETECTADA: {nombre_cat}")
    print("   Iniciando secuencia de paginación...")
    
    page = 1
    
    while page <= 100: # Límite de seguridad
        
        # Construcción de URL paginada
        current_url = f"{url}page/{page}/" if page > 1 else url
        soup = get_soup(current_url)
        
        # Si no carga la página, asumimos fin de categoría
        if not soup: 
            break
        
        # Selector de productos en la rejilla
        prods = soup.select('div.wd-product')
        
        if not prods: 
            break
        
        # --- PREPARACIÓN DE LOTE DE TAREAS ---
        tareas = []
        
        for p in prods:
            tag_a = p.select_one('.wd-entities-title a')
            
            # Validación básica de integridad HTML
            if not tag_a: 
                continue
            
            url_prod = tag_a['href']
            
            # -------------------------------------------------------
            # [OPTIMIZACIÓN] FILTRO DE DUPLICADOS EN TIEMPO REAL
            # -------------------------------------------------------
            # Si la URL ya está en nuestro set 'URLS_VISTAS', significa
            # que este producto ya apareció en otra categoría. Lo saltamos.
            if url_prod in URLS_VISTAS:
                continue
            
            # Si es nuevo, lo registramos para futuras comparaciones
            URLS_VISTAS.add(url_prod)
            
            # Extracción de precios
            tag_price = p.select_one('.price')
            txt_price = tag_price.get_text(separator=' ', strip=True) if tag_price else ""
            p_min, p_max = get_precios(txt_price)
            
            # Empaquetado de datos iniciales
            datos_producto = {
                'Categoría': nombre_cat, 
                'Nombre': tag_a.get_text(strip=True),
                'Precio Mínimo (S/)': p_min, 
                'Precio Máximo (S/)': p_max, 
                'URL': url_prod
            }
            
            tareas.append(datos_producto)

        # --- EJECUCIÓN PARALELA (MULTITHREADING) ---
        if tareas:
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                
                # Mapas de tareas a workers y filtrado de resultados nulos
                resultados = list(filter(None, ex.map(procesar_producto, tareas)))
                
                # Almacenamiento global
                datos_recopilados.extend(resultados)
                
                print(f"   -> Página {page}: {len(resultados)} productos validados y guardados.")
        else:
             print(f"   -> Página {page}: Todos los productos ya fueron procesados previamente.")

        # Verificar si existe botón de "Siguiente" para continuar el bucle
        if not soup.select_one('.next'): 
            break
            
        page += 1


# ==============================================================================
# SECCIÓN 5: BLOQUE PRINCIPAL DE EJECUCIÓN (MAIN)
# ==============================================================================

if __name__ == "__main__":
    
    # Inicio del cronómetro
    start = time.time()
    
    # 1. Cargar la base de datos de medicamentos
    cargar_filtro()
    
    if lista_minsa:
        
        # 2. Descubrir categorías en el Home
        print("🌍 Conectando a la página principal para mapear categorías...")
        soup_home = get_soup(URL_HOME)
        
        # Selector CSS específico para el menú de categorías
        if soup_home:
            items_menu = soup_home.select('#menu-mega-menu-categorias li a[href*="/c/"]')
            cats = [l['href'] for l in items_menu]
        else:
            cats = []
        
        if cats:
            print(f"🚀 SE HAN DETECTADO {len(cats)} CATEGORÍAS. INICIANDO SCRAPING MASIVO...")
            
            # 3. Procesar cada categoría secuencialmente
            for c in cats: 
                procesar_categoria(c)
            
            # 4. Exportación de Resultados
            if datos_recopilados:
                print("\n💾 Procesando archivo de salida...")
                
                df = pd.DataFrame(datos_recopilados)
                
                # --- LIMPIEZA FINAL DE SEGURIDAD ---
                # Aunque filtramos en tiempo real, hacemos una pasada final
                # para eliminar duplicados basados en URL.
                cantidad_antes = len(df)
                df.drop_duplicates(subset=['URL'], keep='first', inplace=True)
                cantidad_despues = len(df)
                
                if cantidad_antes != cantidad_despues:
                    print(f"   🧹 Se eliminaron {cantidad_antes - cantidad_despues} duplicados residuales.")
                
                # Guardado en Excel
                nombre_excel = 'catalogo_turbo_minsa.xlsx'
                df.to_excel(nombre_excel, index=False)
                
                # Reporte final
                mins_totales = (time.time() - start) / 60
                print(f"\n🏁 ¡PROCESO COMPLETADO EN {mins_totales:.2f} MINUTOS!")
                print(f"📊 Total productos únicos recolectados: {len(df)}")
                print(f"📄 Archivo generado: {nombre_excel}")
                
            else: 
                print("\n⚠️ El proceso finalizó pero no se encontraron coincidencias con la lista MINSA.")
        else: 
            print("❌ ERROR: No se pudieron extraer las categorías del menú principal.")
    else:
        print("❌ DETENIDO: No se puede continuar sin la lista de medicamentos cargada.")