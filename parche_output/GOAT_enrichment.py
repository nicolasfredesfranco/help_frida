#!/usr/bin/env python3
"""
GOAT Movie Data Enrichment System
Complete enrichment pipeline with multi-source web scraping and intelligent replication
"""

import pandas as pd
import numpy as np
import re
import os
import sys
import time
import json
import logging
import requests
import unicodedata
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import hashlib

warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('GOAT_enrichment.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class GOATMovieEnricher:
    """
    GOAT (Greatest Of All Time) Movie Enricher
    Comprehensive movie data enrichment using multiple web sources
    """
    
    def __init__(self, 
                 input_file: str = 'output_data.csv',
                 output_file: str = 'standardized.csv',
                 cache_file: str = 'movie_cache.json'):
        self.input_file = input_file
        self.output_file = output_file
        self.cache_file = cache_file
        self.cache = self.load_cache()
        self.metrics = {'initial_empty_cells': 0, 'final_empty_cells': 0}
        
        # Initialize session for web requests
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        logger.info("GOAT Movie Enricher initialized")
    
    def load_cache(self) -> Dict:
        """Load cached movie data"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        return {}
    
    def save_cache(self):
        """Save cache to file"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text"""
        if pd.isna(text) or text == '':
            return ''
        text = str(text).strip()
        text = re.sub(r'\s+', ' ', text)
        return text
    
    def remove_accents(self, text: str) -> str:
        """Remove accents for URL formatting"""
        nfd = unicodedata.normalize('NFD', text)
        return ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
    
    def clean_movie_name(self, name: str) -> str:
        """Clean movie name for searches"""
        if pd.isna(name):
            return ''
        # Remove format indicators
        name = re.sub(r'\b(ESP|SUB|2D|3D|4DX|IMAX|SCREENX|DIG)\b', '', name, flags=re.I)
        name = re.sub(r'[^\w\s\-:]', ' ', name)
        return re.sub(r'\s+', ' ', name).strip()
    
    def search_cinepolis(self, movie_name: str) -> Dict:
        """Search Cinépolis Chile website"""
        info = {'categoria': '', 'descripcion': '', 'actor_principal': '', 'director': '', 'duracion': ''}
        
        try:
            clean_name = self.remove_accents(movie_name.lower())
            clean_name = re.sub(r'[^\w\s-]', '', clean_name)
            clean_name = re.sub(r'[-\s]+', '-', clean_name)
            
            url = f"https://cinepolischile.cl/pelicula/{clean_name}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                text = soup.get_text()
                
                # Extract duration
                dur_match = re.search(r'(\d{2,3})\s*min', text, re.I)
                if dur_match:
                    info['duracion'] = f"{dur_match.group(1)} minutos"
                
                # Extract category
                cat_match = re.search(r'Categoría[:\s]*([^.\n]+?)(?:Califica|Sinopsis|$)', text, re.I)
                if cat_match:
                    info['categoria'] = self.clean_text(cat_match.group(1)).upper()
                
                # Extract description
                desc_match = re.search(r'Sinopsis[:\s]*([^.]+\.)', text, re.I)
                if desc_match:
                    info['descripcion'] = self.clean_text(desc_match.group(1))
                
                # Extract actors
                act_match = re.search(r'Actores?[:\s]*([^.\n]+?)(?:Director|$)', text, re.I)
                if act_match:
                    info['actor_principal'] = self.clean_text(act_match.group(1))
                
                # Extract director
                dir_match = re.search(r'Director(?:es)?[:\s]*([^.\n]+?)(?:Reparto|$)', text, re.I)
                if dir_match:
                    info['director'] = self.clean_text(dir_match.group(1))
        except:
            pass
        
        return info
    
    def search_wikipedia(self, movie_name: str) -> Dict:
        """Search Wikipedia for movie info"""
        info = {'categoria': '', 'descripcion': '', 'actor_principal': '', 'director': '', 'duracion': ''}
        
        try:
            # Search Wikipedia API
            search_url = "https://es.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'format': 'json',
                'list': 'search',
                'srsearch': f'{movie_name} película',
                'srlimit': 1
            }
            
            response = self.session.get(search_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('query', {}).get('search'):
                    title = data['query']['search'][0]['title']
                    
                    # Get page content
                    page_url = f"https://es.wikipedia.org/wiki/{quote(title)}"
                    page_resp = self.session.get(page_url, timeout=10)
                    
                    if page_resp.status_code == 200:
                        soup = BeautifulSoup(page_resp.text, 'html.parser')
                        
                        # Extract from infobox
                        infobox = soup.find('table', class_='infobox')
                        if infobox:
                            rows = infobox.find_all('tr')
                            for row in rows:
                                header = row.find('th')
                                if header:
                                    h_text = header.get_text().lower()
                                    cell = row.find('td')
                                    if cell:
                                        c_text = self.clean_text(cell.get_text())
                                        
                                        if 'director' in h_text:
                                            info['director'] = c_text
                                        elif 'género' in h_text:
                                            info['categoria'] = c_text.upper()
                                        elif 'duración' in h_text:
                                            m = re.search(r'(\d+)', c_text)
                                            if m:
                                                info['duracion'] = f"{m.group(1)} minutos"
                                        elif 'reparto' in h_text or 'protagonista' in h_text:
                                            info['actor_principal'] = c_text.replace('\n', '; ')
                        
                        # Get description from first paragraph
                        if not info['descripcion']:
                            for p in soup.find_all('p')[:3]:
                                text = self.clean_text(p.get_text())
                                if len(text) > 50 and 'película' in text.lower():
                                    info['descripcion'] = text[:500]
                                    break
        except:
            pass
        
        return info
    
    def search_imdb_via_google(self, movie_name: str) -> Dict:
        """Search IMDb using Google"""
        info = {'categoria': '', 'descripcion': '', 'actor_principal': '', 'director': '', 'duracion': ''}
        
        try:
            query = f"{movie_name} site:imdb.com película"
            google_url = f"https://www.google.com/search?q={quote(query)}"
            
            response = self.session.get(google_url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find IMDb link
                for link in soup.find_all('a', href=True):
                    href = link.get('href')
                    if 'imdb.com/title/' in href:
                        # Extract URL
                        if '/url?q=' in href:
                            import urllib.parse
                            parsed = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                            if 'q' in parsed:
                                imdb_url = parsed['q'][0]
                                
                                # Fetch IMDb page
                                imdb_resp = self.session.get(imdb_url, timeout=10)
                                if imdb_resp.status_code == 200:
                                    imdb_soup = BeautifulSoup(imdb_resp.text, 'html.parser')
                                    
                                    # Try to extract JSON-LD data
                                    json_ld = imdb_soup.find('script', type='application/ld+json')
                                    if json_ld:
                                        try:
                                            data = json.loads(json_ld.string)
                                            
                                            if 'genre' in data:
                                                genres = data['genre'] if isinstance(data['genre'], list) else [data['genre']]
                                                info['categoria'] = ', '.join(genres).upper()
                                            
                                            if 'description' in data:
                                                info['descripcion'] = data['description']
                                            
                                            if 'director' in data:
                                                if isinstance(data['director'], dict):
                                                    info['director'] = data['director'].get('name', '')
                                                elif isinstance(data['director'], list):
                                                    dirs = [d.get('name', '') for d in data['director'] if isinstance(d, dict)]
                                                    info['director'] = ', '.join(dirs)
                                            
                                            if 'actor' in data:
                                                if isinstance(data['actor'], list):
                                                    actors = [a.get('name', '') for a in data['actor'][:5] if isinstance(a, dict)]
                                                    info['actor_principal'] = '; '.join(actors)
                                        except:
                                            pass
                                break
        except:
            pass
        
        return info
    
    def comprehensive_search(self, movie_name: str) -> Dict:
        """Perform comprehensive web search using all sources"""
        cache_key = hashlib.md5(movie_name.encode()).hexdigest()
        
        # Check cache
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        clean_name = self.clean_movie_name(movie_name)
        if not clean_name:
            return {}
        
        combined_info = {'categoria': '', 'descripcion': '', 'actor_principal': '', 'director': '', 'duracion': ''}
        
        # Search all sources
        search_funcs = [
            self.search_cinepolis,
            self.search_wikipedia,
            self.search_imdb_via_google
        ]
        
        for func in search_funcs:
            try:
                info = func(clean_name)
                for key, value in info.items():
                    if value and not combined_info[key]:
                        combined_info[key] = value
                
                # Stop if we have all info
                if all(combined_info.values()):
                    break
            except:
                continue
        
        # Cache result
        self.cache[cache_key] = combined_info
        
        return combined_info
    
    def reassign_movie_ids(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reassign movie IDs to ensure unique IDs for unique names"""
        logger.info("Reassigning movie IDs...")
        
        unique_names = df['NOMBRE_UNICO'].dropna().unique()
        name_to_id = {name: idx + 1 for idx, name in enumerate(unique_names)}
        
        df['MOVIE_ID'] = df['NOMBRE_UNICO'].map(name_to_id)
        
        # Handle empty names
        max_id = len(unique_names) + 1
        empty_mask = df['MOVIE_ID'].isna()
        df.loc[empty_mask, 'MOVIE_ID'] = range(max_id, max_id + empty_mask.sum())
        
        df['MOVIE_ID'] = df['MOVIE_ID'].astype(int)
        
        logger.info(f"Reassigned IDs for {len(unique_names)} unique movies")
        return df
    
    def enrich_movies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich movies with web data"""
        logger.info("Starting web enrichment...")
        
        # Get unique movies needing enrichment
        unique_movies = df.groupby('NOMBRE_UNICO').first()
        needs_enrich = unique_movies[
            (unique_movies['CATEGORIA'].isna() | (unique_movies['CATEGORIA'] == '')) |
            (unique_movies['DESCRIPCION'].isna() | (unique_movies['DESCRIPCION'] == '')) |
            (unique_movies['DIRECTOR'].isna() | (unique_movies['DIRECTOR'] == '')) |
            (unique_movies['ACTOR_PRINCIPAL'].isna() | (unique_movies['ACTOR_PRINCIPAL'] == ''))
        ]
        
        logger.info(f"Found {len(needs_enrich)} movies needing enrichment")
        
        enriched_data = {}
        with tqdm(total=len(needs_enrich), desc="Enriching movies") as pbar:
            for nombre_unico in needs_enrich.index:
                if pd.isna(nombre_unico) or nombre_unico == '':
                    pbar.update(1)
                    continue
                
                movie_info = self.comprehensive_search(nombre_unico)
                if movie_info:
                    enriched_data[nombre_unico] = movie_info
                
                # Save cache periodically
                if len(enriched_data) % 25 == 0:
                    self.save_cache()
                
                time.sleep(0.5)  # Rate limiting
                pbar.update(1)
        
        # Apply enriched data
        for nombre_unico, info in enriched_data.items():
            mask = df['NOMBRE_UNICO'] == nombre_unico
            for field, value in info.items():
                if value:
                    field_upper = field.upper()
                    if field_upper in df.columns:
                        empty_mask = mask & ((df[field_upper].isna()) | (df[field_upper] == ''))
                        df.loc[empty_mask, field_upper] = value
        
        self.save_cache()
        logger.info(f"Enriched {len(enriched_data)} movies")
        return df
    
    def vertical_replication(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply vertical replication strategy"""
        logger.info("Applying vertical replication...")
        
        fields = ['CATEGORIA', 'DESCRIPCION', 'ACTOR_PRINCIPAL', 'DIRECTOR', 
                  'DURACION', 'FAMILIA', 'DESCRIPCION2', 'CATEGORIA_CINEPOLIS']
        
        for field in fields:
            if field not in df.columns:
                df[field] = ''
        
        for nombre_unico in tqdm(df['NOMBRE_UNICO'].unique(), desc="Vertical replication"):
            if pd.isna(nombre_unico) or nombre_unico == '':
                continue
            
            mask = df['NOMBRE_UNICO'] == nombre_unico
            group = df[mask]
            
            for field in fields:
                non_empty = group[field].dropna()
                non_empty = non_empty[non_empty != '']
                
                if len(non_empty) > 0:
                    best_value = non_empty.value_counts().index[0] if len(non_empty) > 1 else non_empty.iloc[0]
                    df.loc[mask, field] = best_value
        
        return df
    
    def horizontal_replication(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply horizontal replication strategy"""
        logger.info("Applying horizontal replication...")
        
        rules = [
            ('CATEGORIA', 'CATEGORIA_CINEPOLIS'),
            ('DESCRIPCION', 'DESCRIPCION2'),
            ('TITULO_LIMPIO', 'NOMBRE_ORIGINAL'),
            ('TITULO_LIMPIO_CLEAN', 'NOMBRE_ORIGINAL_CLEAN')
        ]
        
        for source, target in rules:
            if source in df.columns and target in df.columns:
                empty = (df[target].isna()) | (df[target] == '')
                df.loc[empty, target] = df.loc[empty, source]
        
        # Handle FAMILIA field
        if 'FAMILIA' in df.columns and 'TITULO_LIMPIO' in df.columns:
            empty = (df['FAMILIA'].isna()) | (df['FAMILIA'] == '')
            df.loc[empty, 'FAMILIA'] = df.loc[empty, 'TITULO_LIMPIO'].apply(
                lambda x: x.split()[0] if pd.notna(x) and x else ''
            )
        
        return df
    
    def calculate_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate empty cell metrics"""
        total_cells = df.size
        empty_cells = df.isna().sum().sum() + (df == '').sum().sum()
        empty_percentage = (empty_cells / total_cells) * 100
        
        return {
            'total_cells': total_cells,
            'empty_cells': empty_cells,
            'empty_percentage': round(empty_percentage, 2),
            'total_rows': len(df),
            'unique_movies': df['NOMBRE_UNICO'].nunique()
        }
    
    def process(self):
        """Main processing pipeline"""
        logger.info("="*60)
        logger.info("GOAT Movie Enrichment Starting")
        logger.info("="*60)
        
        # Load data
        logger.info(f"Loading {self.input_file}...")
        df = pd.read_csv(self.input_file, encoding='utf-8')
        
        # Initial metrics
        initial_metrics = self.calculate_metrics(df)
        logger.info(f"Initial empty cells: {initial_metrics['empty_percentage']}%")
        
        # Process pipeline
        df = self.reassign_movie_ids(df)
        df = self.enrich_movies(df)
        df = self.vertical_replication(df)
        df = self.horizontal_replication(df)
        
        # Save output
        logger.info(f"Saving to {self.output_file}...")
        df.to_csv(self.output_file, index=False, encoding='utf-8')
        
        # Final metrics
        final_metrics = self.calculate_metrics(df)
        logger.info(f"Final empty cells: {final_metrics['empty_percentage']}%")
        logger.info(f"Improvement: {initial_metrics['empty_percentage'] - final_metrics['empty_percentage']:.2f}%")
        
        # Save metrics
        metrics = {
            'initial': initial_metrics,
            'final': final_metrics,
            'improvement': initial_metrics['empty_percentage'] - final_metrics['empty_percentage']
        }
        
        with open('enrichment_metrics.json', 'w') as f:
            json.dump(metrics, f, indent=2)
        
        logger.info("="*60)
        logger.info("Processing complete!")
        logger.info("="*60)
        
        return metrics


if __name__ == "__main__":
    enricher = GOATMovieEnricher()
    metrics = enricher.process()
    print(f"\n✅ Enrichment complete! Empty cells reduced from {metrics['initial']['empty_percentage']}% to {metrics['final']['empty_percentage']}%")