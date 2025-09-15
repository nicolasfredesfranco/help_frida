#!/usr/bin/env python3
"""
Script para descargar información de películas desde Cinépolis Chile.
Descarga la página principal, extrae las películas y guarda su información.
"""

import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import unicodedata

def remove_accents(text):
    """Elimina acentos y caracteres especiales para nombres de archivo."""
    nfd_form = unicodedata.normalize('NFD', text)
    return ''.join(char for char in nfd_form if unicodedata.category(char) != 'Mn')

def clean_filename(filename):
    """Limpia el nombre del archivo para que sea válido."""
    # Eliminar acentos
    filename = remove_accents(filename)
    # Reemplazar caracteres no válidos
    filename = re.sub(r'[^\w\s-]', '', filename)
    # Reemplazar espacios con guiones
    filename = re.sub(r'[-\s]+', '-', filename)
    return filename.strip('-').upper()

def get_page_content(url):
    """Descarga el contenido de una página web."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error descargando {url}: {e}")
        return None

def extract_text_from_html(html_content):
    """Extrae todo el texto visible de una página HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Eliminar scripts y estilos
    for script in soup(["script", "style"]):
        script.decompose()
    
    # Obtener texto
    text = soup.get_text()
    
    # Limpiar espacios en blanco
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    
    return text

def extract_structured_info(html_content):
    """Extrae información estructurada de la página de la película."""
    soup = BeautifulSoup(html_content, 'html.parser')
    info = {
        'nombre_pelicula': '',
        'restriccion_edad': '',
        'duracion': '',
        'categoria': '',
        'sinopsis': '',
        'actores': '',
        'directores': ''
    }
    
    try:
        # Obtener todo el texto de la página
        page_text = soup.get_text()
        
        # Título de la película - buscar en el title tag primero
        title_tag = soup.find('title')
        if title_tag:
            title_text = title_tag.text.strip()
            # El formato es "NOMBRE PELÍCULA | Cinépolis"
            if '|' in title_text:
                info['nombre_pelicula'] = title_text.split('|')[0].strip()
            else:
                info['nombre_pelicula'] = title_text.strip()
        
        # Buscar una sección compacta con toda la información
        # En Cinépolis, la info está junta: "PELÍCULA[PELÍCULA]TE+123 minCategoríaCalifica"
        
        # Primero intentar encontrar el bloque de información principal
        # Buscar el patrón donde aparece el título seguido de la clasificación y duración
        compact_info_pattern = r'([A-Z][A-Z0-9\s\-:]+?)(?:TE\+|\d{1,2}\+?)'
        
        # Restricción de edad - buscar TE+, TE+7, 14, etc.
        # Buscar específicamente el patrón antes de la duración
        edad_patterns = [
            r'(TE\+)(?=\d{2,4}\s*min)',  # TE+ seguido de duración
            r'(TE\+\d+)(?=\s*\d{2,4}\s*min)',  # TE+7, etc.
            r'(\d{1,2}\+?)(?=\s*\d{2,4}\s*min)',  # 14, 14+, etc.
            r'([A-Z]{1,3}\+?\d*)(?=\d{2,4}\s*min)'  # Otros formatos
        ]
        
        for pattern in edad_patterns:
            edad_match = re.search(pattern, page_text)
            if edad_match:
                edad = edad_match.group(1)
                # Limpiar casos como "TE+76" donde 76 es parte de "763 min"
                if 'TE+' in edad and len(edad) > 4:
                    info['restriccion_edad'] = 'TE+'
                else:
                    info['restriccion_edad'] = edad
                break
        
        # Duración - buscar específicamente después de la clasificación
        # Buscar el primer número de 2-4 dígitos seguido de "min"
        duracion_patterns = [
            r'(?:TE\+|TE\+\d+|\d{1,2}\+?)[^\d]*(\d{2,4})\s*min',  # Después de clasificación
            r'(\d{2,3})\s*min(?:utos)?',  # Patrón general de 2-3 dígitos
        ]
        
        for pattern in duracion_patterns:
            duracion_match = re.search(pattern, page_text)
            if duracion_match:
                duracion_num = int(duracion_match.group(1))
                # Validar que la duración sea razonable (entre 30 y 999 minutos)
                if 30 <= duracion_num <= 999:
                    info['duracion'] = f"{duracion_num} min"
                    break
        
        # Categoría - buscar después de la duración y antes de "Califica"
        # El patrón es: "XXX minCategoríaCalifica"
        if info['duracion']:
            categoria_pattern = rf"{duracion_match.group(1)}\s*min([A-Za-záéíóúñÁÉÍÓÚÑ\s,]+?)(?:Califica|Sinopsis|$)"
            cat_match = re.search(categoria_pattern, page_text)
            if cat_match:
                categoria = cat_match.group(1).strip()
                # Limpiar categoría
                categoria = re.sub(r'[^A-Za-záéíóúñÁÉÍÓÚÑ\s,]', '', categoria).strip()
                if categoria and len(categoria) < 50:  # Evitar capturar texto muy largo
                    info['categoria'] = categoria
        
        # Sinopsis - buscar después de la palabra "Sinopsis"
        sinopsis_match = re.search(r'Sinopsis([^.]+\.)', page_text)
        if sinopsis_match:
            sinopsis = sinopsis_match.group(1).strip()
            # Limpiar la sinopsis de texto adicional
            sinopsis = sinopsis.split('Créditos')[0].split('Actores')[0].split('Director')[0]
            info['sinopsis'] = sinopsis.strip()
        
        # Actores - buscar después de "Actores"
        actores_match = re.search(r'Actores([A-Za-záéíóúñÁÉÍÓÚÑ\s,]+?)(?:Director|Consulta|$)', page_text)
        if actores_match:
            actores = actores_match.group(1).strip()
            # Limpiar lista de actores
            actores = re.sub(r'[^A-Za-záéíóúñÁÉÍÓÚÑ\s,]', '', actores).strip()
            if actores and len(actores) < 200:
                info['actores'] = actores
        
        # Directores - buscar después de "Directores" o "Director"
        director_match = re.search(r'Director(?:es)?([A-Za-záéíóúñÁÉÍÓÚÑ\s,\.]+?)(?:Consulta|Horarios|$|\{)', page_text)
        if director_match:
            directores = director_match.group(1).strip()
            # Limpiar lista de directores
            directores = re.sub(r'[^A-Za-záéíóúñÁÉÍÓÚÑ\s,\.]', '', directores).strip()
            if directores and len(directores) < 200:
                info['directores'] = directores
        
        # Si no se encontró el nombre de la película, intentar extraerlo del contenido
        if not info['nombre_pelicula'] or info['nombre_pelicula'] == 'Cinépolis®':
            # Buscar el primer texto en mayúsculas que aparece
            nombre_match = re.search(r'^([A-Z][A-Z0-9\s\-:]+?)\s*\|', page_text, re.MULTILINE)
            if nombre_match:
                info['nombre_pelicula'] = nombre_match.group(1).strip()
    
    except Exception as e:
        print(f"   Error extrayendo información estructurada: {e}")
    
    return info

def get_movie_urls(main_page_content):
    """Extrae las URLs de las películas desde la página principal."""
    soup = BeautifulSoup(main_page_content, 'html.parser')
    movie_urls = []
    
    # Buscar enlaces a películas - pueden estar en diferentes formatos
    # Patrón 1: Enlaces directos a /pelicula/
    movie_links = soup.find_all('a', href=re.compile(r'/pelicula/[^/]+/?$'))
    
    for link in movie_links:
        href = link.get('href')
        if href:
            # Construir URL completa
            full_url = urljoin('https://cinepolischile.cl', href)
            if full_url not in movie_urls:
                movie_urls.append(full_url)
    
    # Si no encontramos películas con el patrón anterior, buscar otros patrones
    if not movie_urls:
        # Buscar todos los enlaces que contengan "pelicula"
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link.get('href')
            if 'pelicula' in href.lower() and not href.endswith('.jpg') and not href.endswith('.png'):
                full_url = urljoin('https://cinepolischile.cl', href)
                if full_url not in movie_urls and '/pelicula/' in full_url:
                    movie_urls.append(full_url)
    
    return movie_urls

def save_movie_info(movie_url, movies_dir='movies'):
    """Descarga y guarda la información de una película."""
    # Obtener el contenido de la página
    content = get_page_content(movie_url)
    if not content:
        return False
    
    # Extraer el nombre de la película desde la URL
    movie_slug = movie_url.rstrip('/').split('/')[-1]
    movie_name = clean_filename(movie_slug.replace('-', ' '))
    
    # Crear directorio para la película (sobrescribir si existe)
    movie_dir = os.path.join(movies_dir, movie_name)
    os.makedirs(movie_dir, exist_ok=True)
    
    # Extraer texto completo de la página
    text_content = extract_text_from_html(content)
    
    # Guardar el contenido en description.txt
    description_file = os.path.join(movie_dir, 'description.txt')
    with open(description_file, 'w', encoding='utf-8') as f:
        f.write(text_content)
    
    # Guardar la URL en webpage.txt
    webpage_file = os.path.join(movie_dir, 'webpage.txt')
    with open(webpage_file, 'w', encoding='utf-8') as f:
        f.write(movie_url)
    
    # Extraer información estructurada
    structured_info = extract_structured_info(content)
    
    # Si no se encontró el nombre de la película en el HTML, usar el de la URL
    if not structured_info['nombre_pelicula']:
        structured_info['nombre_pelicula'] = movie_name.replace('-', ' ')
    
    # Crear archivo texto_estructurado.txt
    structured_file = os.path.join(movie_dir, 'texto_estructurado.txt')
    with open(structured_file, 'w', encoding='utf-8') as f:
        f.write(f"1 nombre pelicula: {structured_info['nombre_pelicula']}\n")
        f.write(f"2 Restriccion Edad: {structured_info['restriccion_edad']}\n")
        f.write(f"3 duracion: {structured_info['duracion']}\n")
        f.write(f"4 categoria: {structured_info['categoria']}\n")
        f.write(f"5 Sinopsis: {structured_info['sinopsis']}\n")
        f.write(f"6 Actores: {structured_info['actores']}\n")
        f.write(f"7 Directores: {structured_info['directores']}\n")
    
    print(f"✓ Guardada información de: {movie_name}")
    print(f"  - Nombre: {structured_info['nombre_pelicula']}")
    print(f"  - Categoría: {structured_info['categoria'] or 'No encontrada'}")
    print(f"  - Duración: {structured_info['duracion'] or 'No encontrada'}")
    
    return True

def main():
    """Función principal del script."""
    base_url = 'https://cinepolischile.cl/'
    
    print("=" * 60)
    print("DESCARGADOR DE INFORMACIÓN DE PELÍCULAS - CINÉPOLIS CHILE")
    print("=" * 60)
    
    # Crear directorio movies si no existe
    if not os.path.exists('movies'):
        os.makedirs('movies')
    
    # Descargar página principal
    print(f"\n1. Descargando página principal: {base_url}")
    main_content = get_page_content(base_url)
    
    if not main_content:
        print("Error: No se pudo descargar la página principal")
        return
    
    # Guardar contenido de la página principal para debug
    with open('main_page.html', 'w', encoding='utf-8') as f:
        f.write(main_content)
    print("   Página principal guardada en main_page.html")
    
    # Extraer URLs de películas
    print("\n2. Buscando películas en la página principal...")
    movie_urls = get_movie_urls(main_content)
    
    if not movie_urls:
        print("   No se encontraron películas. Verificando estructura de la página...")
        # Intentar buscar películas en la página de cartelera
        cartelera_url = 'https://cinepolischile.cl/cartelera'
        print(f"   Intentando con: {cartelera_url}")
        cartelera_content = get_page_content(cartelera_url)
        if cartelera_content:
            movie_urls = get_movie_urls(cartelera_content)
    
    if movie_urls:
        print(f"   Encontradas {len(movie_urls)} películas")
        
        # Mostrar las películas encontradas
        print("\n   Películas encontradas:")
        for i, url in enumerate(movie_urls, 1):
            movie_name = url.rstrip('/').split('/')[-1]
            print(f"   {i}. {movie_name}")
        
        # Descargar información de cada película
        print(f"\n3. Descargando información de {len(movie_urls)} películas...")
        successful = 0
        failed = 0
        
        for i, movie_url in enumerate(movie_urls, 1):
            print(f"\n   [{i}/{len(movie_urls)}] Procesando: {movie_url}")
            if save_movie_info(movie_url):
                successful += 1
            else:
                failed += 1
            
            # Pequeña pausa para no sobrecargar el servidor
            if i < len(movie_urls):
                time.sleep(1)
        
        # Resumen
        print("\n" + "=" * 60)
        print("RESUMEN")
        print("=" * 60)
        print(f"Total de películas procesadas: {len(movie_urls)}")
        print(f"Exitosas: {successful}")
        print(f"Fallidas: {failed}")
        print(f"\nLos archivos se guardaron en la carpeta 'movies/'")
    else:
        print("   No se encontraron películas en el sitio web.")
        print("   Verifica que el sitio esté activo y la estructura no haya cambiado.")
    
    print("\n¡Proceso completado!")

if __name__ == "__main__":
    main()