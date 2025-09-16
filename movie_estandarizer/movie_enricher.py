#!/usr/bin/env python3
"""
Movie Enricher - Complete Movie Data Enrichment System
Enriches movie catalog by:
1. Propagating data between records with same NOMBRE_UNICO
2. Searching missing info from Cinépolis website
3. Extracting structured data from movie pages
"""

import pandas as pd
import numpy as np
import re
import os
import sys
import time
import requests
import unicodedata
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
from typing import Dict, List, Optional
import logging
from tqdm import tqdm
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('movie_enricher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MovieEnricher:
    """
    Enriches movie data by propagating internal data and fetching from Cinépolis
    """
    
    def __init__(self, input_file: str = 'output_data/output_data.csv',
                 output_file: str = 'output_data/output_data_enriched.csv'):
        self.input_file = input_file
        self.output_file = output_file
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def remove_accents(self, text: str) -> str:
        """Remove accents from text"""
        if not text:
            return text
        nfd_form = unicodedata.normalize('NFD', text)
        return ''.join(char for char in nfd_form if unicodedata.category(char) != 'Mn')
    
    def clean_for_url(self, text: str) -> str:
        """Clean text for URL construction"""
        if not text:
            return ''
        # Remove accents
        text = self.remove_accents(text)
        # Convert to lowercase and replace spaces with hyphens
        text = text.lower().strip()
        # Remove special characters
        text = re.sub(r'[^\w\s-]', '', text)
        # Replace spaces with hyphens
        text = re.sub(r'[-\s]+', '-', text)
        return text.strip('-')
    
    def propagate_data_within_groups(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Propagate data between records with same NOMBRE_UNICO
        Fill empty fields with data from other records in the same group
        """
        logger.info("Propagating data within NOMBRE_UNICO groups...")
        
        # Group by NOMBRE_UNICO
        grouped = df.groupby('NOMBRE_UNICO')
        
        # Fields to propagate (exclude MOVIE_ID and MOVIE_NAME as they can differ)
        fields_to_propagate = [
            'CATEGORIA', 'DESCRIPCION', 'FAMILIA', 
            'ACTOR_PRINCIPAL', 'DIRECTOR', 'DURACION',
            'CATEGORIA_CINEPOLIS', 'DESCRIPCION2'
        ]
        
        propagated_count = 0
        
        for nombre_unico, group in tqdm(grouped, desc="Propagating data"):
            if len(group) > 1:
                # For each field, find non-empty values and propagate
                for field in fields_to_propagate:
                    non_empty_values = group[field].dropna()
                    non_empty_values = non_empty_values[non_empty_values != '']
                    
                    if len(non_empty_values) > 0:
                        # Use the most common non-empty value
                        most_common = non_empty_values.mode()[0] if len(non_empty_values.mode()) > 0 else non_empty_values.iloc[0]
                        
                        # Fill empty values in this group
                        mask = (df['NOMBRE_UNICO'] == nombre_unico) & ((df[field].isna()) | (df[field] == ''))
                        if mask.any():
                            df.loc[mask, field] = most_common
                            propagated_count += mask.sum()
        
        logger.info(f"Propagated {propagated_count} field values within groups")
        return df
    
    def search_movie_on_cinepolis(self, movie_name: str) -> Optional[str]:
        """
        Search for movie on Cinépolis website and return the movie page URL
        """
        try:
            # Clean movie name for search
            search_query = self.clean_for_url(movie_name)
            
            # Try direct URL construction first
            direct_url = f"https://cinepolischile.cl/pelicula/{search_query}"
            
            # Check if direct URL works
            response = self.session.get(direct_url, timeout=10)
            if response.status_code == 200:
                # Verify it's actually a movie page
                if 'pelicula' in response.url and response.status_code == 200:
                    return response.url
            
            # If direct URL doesn't work, try Google search
            google_query = f"{movie_name} site:cinepolischile.cl/pelicula"
            google_url = f"https://www.google.com/search?q={quote(google_query)}"
            
            response = self.session.get(google_url, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find links to Cinépolis
                for link in soup.find_all('a', href=True):
                    href = link.get('href')
                    if 'cinepolischile.cl/pelicula/' in href:
                        # Extract actual URL from Google redirect
                        if '/url?q=' in href:
                            import urllib.parse
                            parsed = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                            if 'q' in parsed:
                                return parsed['q'][0]
                        elif href.startswith('http'):
                            return href
            
            return None
            
        except Exception as e:
            logger.error(f"Error searching for {movie_name}: {e}")
            return None
    
    def extract_movie_info_from_page(self, url: str) -> Dict:
        """
        Extract structured information from Cinépolis movie page
        """
        info = {
            'categoria': '',
            'descripcion': '',
            'actor_principal': '',
            'director': '',
            'duracion': ''
        }
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text()
            
            # Extract duration
            duracion_match = re.search(r'(\d{2,3})\s*min', page_text)
            if duracion_match:
                info['duracion'] = f"{duracion_match.group(1)} minutos"
            
            # Extract category
            categoria_patterns = [
                r'Categoría[:\s]*([A-Za-záéíóúñÁÉÍÓÚÑ\s,]+?)(?:Califica|Sinopsis|$)',
                r'Género[:\s]*([A-Za-záéíóúñÁÉÍÓÚÑ\s,]+?)(?:Califica|Sinopsis|$)',
                r'min([A-Za-záéíóúñÁÉÍÓÚÑ\s,]+?)(?:Califica|Sinopsis|$)'
            ]
            
            for pattern in categoria_patterns:
                cat_match = re.search(pattern, page_text, re.IGNORECASE)
                if cat_match:
                    categoria = cat_match.group(1).strip()
                    categoria = re.sub(r'[^A-Za-záéíóúñÁÉÍÓÚÑ\s,]', '', categoria).strip()
                    if categoria and len(categoria) < 50:
                        info['categoria'] = categoria.upper()
                        break
            
            # Extract synopsis
            sinopsis_patterns = [
                r'Sinopsis[:\s]*([^.]+\.)',
                r'Descripción[:\s]*([^.]+\.)',
                r'Historia[:\s]*([^.]+\.)'
            ]
            
            for pattern in sinopsis_patterns:
                sinopsis_match = re.search(pattern, page_text, re.IGNORECASE)
                if sinopsis_match:
                    sinopsis = sinopsis_match.group(1).strip()
                    sinopsis = sinopsis.split('Créditos')[0].split('Actores')[0].split('Director')[0]
                    info['descripcion'] = sinopsis.strip()
                    break
            
            # Extract actors
            actores_patterns = [
                r'Actores[:\s]*([A-Za-záéíóúñÁÉÍÓÚÑ\s,;]+?)(?:Director|Consulta|$)',
                r'Reparto[:\s]*([A-Za-záéíóúñÁÉÍÓÚÑ\s,;]+?)(?:Director|Consulta|$)',
                r'Cast[:\s]*([A-Za-záéíóúñÁÉÍÓÚÑ\s,;]+?)(?:Director|Consulta|$)'
            ]
            
            for pattern in actores_patterns:
                actores_match = re.search(pattern, page_text, re.IGNORECASE)
                if actores_match:
                    actores = actores_match.group(1).strip()
                    actores = re.sub(r'[^A-Za-záéíóúñÁÉÍÓÚÑ\s,;]', '', actores).strip()
                    if actores and len(actores) < 200:
                        # Clean up actors list
                        actores = '; '.join([a.strip() for a in actores.split(',') if a.strip()])
                        info['actor_principal'] = actores
                        break
            
            # Extract director
            director_patterns = [
                r'Director(?:es)?[:\s]*([A-Za-záéíóúñÁÉÍÓÚÑ\s,\.]+?)(?:Consulta|Horarios|$|\{)',
                r'Dirigid[oa] por[:\s]*([A-Za-záéíóúñÁÉÍÓÚÑ\s,\.]+?)(?:Consulta|Horarios|$|\{)'
            ]
            
            for pattern in director_patterns:
                director_match = re.search(pattern, page_text, re.IGNORECASE)
                if director_match:
                    directores = director_match.group(1).strip()
                    directores = re.sub(r'[^A-Za-záéíóúñÁÉÍÓÚÑ\s,\.]', '', directores).strip()
                    if directores and len(directores) < 200:
                        info['director'] = directores
                        break
            
        except Exception as e:
            logger.error(f"Error extracting info from {url}: {e}")
        
        return info
    
    def enrich_missing_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich missing data by searching and extracting from Cinépolis website
        """
        # Identify unique movies with missing data
        enrichment_fields = ['CATEGORIA', 'DESCRIPCION', 'ACTOR_PRINCIPAL', 'DIRECTOR']
        
        # Get unique movies
        unique_movies = df.drop_duplicates(subset=['NOMBRE_UNICO'])
        
        # Find movies with missing data
        missing_mask = unique_movies[enrichment_fields].isna().any(axis=1) | \
                      (unique_movies[enrichment_fields] == '').any(axis=1)
        
        movies_to_enrich = unique_movies[missing_mask]['NOMBRE_UNICO'].unique()
        
        logger.info(f"Found {len(movies_to_enrich)} unique movies with missing data")
        
        enriched_count = 0
        cache = {}  # Cache to avoid repeated searches
        
        for nombre_unico in tqdm(movies_to_enrich[:50], desc="Enriching from web"):  # Limit to 50 for testing
            if nombre_unico in cache:
                continue
            
            # Search for movie on Cinépolis
            url = self.search_movie_on_cinepolis(nombre_unico)
            
            if url:
                # Extract information
                info = self.extract_movie_info_from_page(url)
                cache[nombre_unico] = info
                
                # Update dataframe for all records with this NOMBRE_UNICO
                mask = df['NOMBRE_UNICO'] == nombre_unico
                
                if info['categoria'] and (df.loc[mask, 'CATEGORIA'].isna().any() or (df.loc[mask, 'CATEGORIA'] == '').any()):
                    df.loc[mask, 'CATEGORIA'] = info['categoria']
                    df.loc[mask, 'CATEGORIA_CINEPOLIS'] = info['categoria']
                    enriched_count += 1
                
                if info['descripcion'] and (df.loc[mask, 'DESCRIPCION'].isna().any() or (df.loc[mask, 'DESCRIPCION'] == '').any()):
                    df.loc[mask, 'DESCRIPCION'] = info['descripcion']
                    df.loc[mask, 'DESCRIPCION2'] = info['descripcion']
                    enriched_count += 1
                
                if info['actor_principal'] and (df.loc[mask, 'ACTOR_PRINCIPAL'].isna().any() or (df.loc[mask, 'ACTOR_PRINCIPAL'] == '').any()):
                    df.loc[mask, 'ACTOR_PRINCIPAL'] = info['actor_principal']
                    enriched_count += 1
                
                if info['director'] and (df.loc[mask, 'DIRECTOR'].isna().any() or (df.loc[mask, 'DIRECTOR'] == '').any()):
                    df.loc[mask, 'DIRECTOR'] = info['director']
                    enriched_count += 1
                
                # Small delay to avoid overloading server
                time.sleep(0.5)
        
        logger.info(f"Enriched {enriched_count} fields from web")
        return df
    
    def ensure_final_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final consistency check ensuring all records with same NOMBRE_UNICO
        have consistent MOVIE_ID and data
        """
        logger.info("Running final consistency validation...")
        
        inconsistencies = 0
        fixes = 0
        
        # Check each unique movie group
        for nombre_unico in df['NOMBRE_UNICO'].unique():
            if pd.isna(nombre_unico) or nombre_unico == '':
                continue
                
            mask = df['NOMBRE_UNICO'] == nombre_unico
            group = df[mask]
            
            # Verify MOVIE_ID consistency
            unique_ids = group['MOVIE_ID'].unique()
            if len(unique_ids) > 1:
                logger.warning(f"Found {len(unique_ids)} different MOVIE_IDs for '{nombre_unico}'")
                # Use the most common ID
                correct_id = group['MOVIE_ID'].mode()[0]
                df.loc[mask, 'MOVIE_ID'] = correct_id
                inconsistencies += 1
                fixes += len(unique_ids) - 1
            
            # Ensure all enriched fields are consistent
            enriched_fields = ['CATEGORIA', 'DESCRIPCION', 'ACTOR_PRINCIPAL', 'DIRECTOR', 'FAMILIA']
            for field in enriched_fields:
                non_empty = group[field].dropna()
                non_empty = non_empty[non_empty != '']
                if len(non_empty) > 0:
                    # Use most complete value
                    best_value = non_empty.mode()[0] if len(non_empty.mode()) > 0 else non_empty.iloc[0]
                    empty_mask = mask & ((df[field].isna()) | (df[field] == ''))
                    if empty_mask.any():
                        df.loc[empty_mask, field] = best_value
                        fixes += empty_mask.sum()
        
        logger.info(f"✓ Consistency check complete: {inconsistencies} issues found, {fixes} fixes applied")
        return df
    
    def process(self):
        """
        Main processing pipeline with final consistency check
        """
        logger.info("Starting movie enrichment process")
        
        # Check if input file exists
        if not os.path.exists(self.input_file):
            logger.error(f"Input file not found: {self.input_file}")
            logger.error("Please run the standardizer first to create the input file.")
            raise FileNotFoundError(f"Input file not found: {self.input_file}")
        
        # Load data
        logger.info(f"Loading data from {self.input_file}")
        df = pd.read_csv(self.input_file)
        initial_empty = df.isna().sum().sum() + (df == '').sum().sum()
        logger.info(f"Initial empty fields: {initial_empty}")
        
        # Step 1: Propagate data within groups
        df = self.propagate_data_within_groups(df)
        after_propagation_empty = df.isna().sum().sum() + (df == '').sum().sum()
        logger.info(f"Empty fields after propagation: {after_propagation_empty}")
        
        # Step 2: Enrich from web (limited for now)
        df = self.enrich_missing_data(df)
        after_enrichment_empty = df.isna().sum().sum() + (df == '').sum().sum()
        logger.info(f"Empty fields after enrichment: {after_enrichment_empty}")
        
        # Step 3: Final consistency check
        df = self.ensure_final_consistency(df)
        final_empty = df.isna().sum().sum() + (df == '').sum().sum()
        logger.info(f"Final empty fields: {final_empty}")
        
        # Save enriched data
        df.to_csv(self.output_file, index=False)
        logger.info(f"Saved enriched data to {self.output_file}")
        
        # Generate summary report
        logger.info("=" * 60)
        logger.info("ENRICHMENT SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total records: {len(df):,}")
        logger.info(f"Unique movies: {df['NOMBRE_UNICO'].nunique():,}")
        logger.info(f"Empty fields filled: {initial_empty - final_empty:,}")
        logger.info(f"Completion rate: {(1 - final_empty/(len(df) * len(df.columns))) * 100:.2f}%")
        logger.info("=" * 60)
        
        return df


def main():
    """Main execution"""
    enricher = MovieEnricher()
    enricher.process()


if __name__ == "__main__":
    main()
