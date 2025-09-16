#!/usr/bin/env python3
"""
Movie Standardizer - Cinema Transaction Deduplication Framework
Author: Data Engineering Team
Date: December 2024
Description: Transforms 17M+ transactional cinema records into deduplicated movie catalog
"""

import pandas as pd
import numpy as np
import re
import os
import sys
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('movie_standardizer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class MovieStandardizer:
    """
    Main class for movie standardization and deduplication
    """
    
    def __init__(self, 
                 input_file: str = 'input_data/Cinepolis.csv',
                 output_file: str = 'output_data/output_data.csv',
                 template_file: str = 'output_data/example_output_data.csv',
                 batch_size: int = 100000):
        """
        Initialize the Movie Standardizer
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            template_file: Path to template/example CSV file
            batch_size: Size of batches for processing large files
        """
        self.input_file = input_file
        self.output_file = output_file
        self.template_file = template_file
        self.batch_size = batch_size
        
        # Format mappings
        self.FORMAT_MAPPING = {
            'ESP': '2D',
            'SUB': '2D',
            'DUB': '2D',
            'DOB': '2D',
            'SP': '2D',
            '4DX': '4D',
            '4DX 3D': '4D',
            '4DX/3D': '4D',
            '4DX/2D': '4D',
            '4DX 3D DOB': '4D',
            '4DX 3D DUB': '4D',
            '4DX 3D SUB': '4D',
            'SCREENX': 'SCREENX',
            'XE SCREENX': 'SCREENX',
            'XE': 'SCREENX',
            'IMAX': 'IMAX',
            '3D': '3D',
            '3D ESP': '3D',
            '3D SUB': '3D',
            '3D DOB': '3D'
        }
        
        # Language patterns to remove
        self.LANGUAGE_PATTERNS = [
            r'\s+ESP$', r'\s+SUB$', r'\s+DUB$', r'\s+DOB$', r'\s+SP$',
            r'\s+Esp$', r'\s+Sub$', r'\s+Dub$', r'\s+Dob$', r'\s+Sp$'
        ]
        
        # Format patterns to remove
        self.FORMAT_PATTERNS = [
            r'\s+4DX.*$', r'\s+IMAX.*$', r'\s+SCREENX.*$', r'\s+XE.*$',
            r'\s+3D.*$', r'\s+2D.*$', r'\s+4D.*$'
        ]
        
        # All patterns to remove for NOMBRE_UNICO
        self.ALL_PATTERNS = self.LANGUAGE_PATTERNS + self.FORMAT_PATTERNS
        
        logger.info(f"MovieStandardizer initialized")
        logger.info(f"Input: {self.input_file}")
        logger.info(f"Output: {self.output_file}")
        logger.info(f"Template: {self.template_file}")
    
    def normalize_title_level1(self, movie_name: str) -> str:
        """
        Level 1 normalization: Basic cleaning and uppercase
        """
        if pd.isna(movie_name) or movie_name == '':
            return ''
        
        # Convert to uppercase and strip whitespace
        title = str(movie_name).upper().strip()
        
        # Normalize multiple spaces to single space
        title = re.sub(r'\s+', ' ', title)
        
        return title
    
    def extract_format(self, movie_name: str, movie_format: str = None) -> str:
        """
        Extract format from movie name or format field
        """
        if pd.isna(movie_name) and pd.isna(movie_format):
            return '2D'
        
        # First try to extract from MOVIE_FORMAT field if available
        if movie_format and not pd.isna(movie_format):
            format_upper = str(movie_format).upper().strip()
            if format_upper in self.FORMAT_MAPPING:
                return self.FORMAT_MAPPING[format_upper]
        
        # Then try to extract from movie name
        name_upper = str(movie_name).upper() if not pd.isna(movie_name) else ''
        
        # Check for specific format indicators in the name
        if '4DX' in name_upper:
            return '4D'
        elif 'SCREENX' in name_upper or 'XE' in name_upper:
            return 'SCREENX'
        elif 'IMAX' in name_upper:
            return 'IMAX'
        elif '3D' in name_upper:
            return '3D'
        
        # Default to 2D
        return '2D'
    
    def extract_language(self, movie_name: str, movie_language: str = None) -> str:
        """
        Extract language from movie name or language field
        """
        # First try the MOVIE_LENGUAJE field if available
        if movie_language and not pd.isna(movie_language):
            lang_upper = str(movie_language).upper().strip()
            if lang_upper in ['ESP', 'SUB', 'DUB', 'DOB']:
                return lang_upper if lang_upper != 'DOB' else 'DUB'
        
        # Then try to extract from movie name
        if not pd.isna(movie_name):
            name_upper = str(movie_name).upper()
            
            # Check for language indicators at the end of the name
            if re.search(r'\s+ESP\s*$', name_upper):
                return 'ESP'
            elif re.search(r'\s+SUB\s*$', name_upper):
                return 'SUB'
            elif re.search(r'\s+(DUB|DOB)\s*$', name_upper):
                return 'DUB'
            elif re.search(r'\s+SP\s*$', name_upper):
                return 'ESP'
        
        # Default to ESP if no language found
        return 'ESP'
    
    def normalize_title_level2(self, titulo_limpio: str) -> str:
        """
        Level 2 normalization: Remove format and language suffixes
        """
        if pd.isna(titulo_limpio) or titulo_limpio == '':
            return ''
        
        clean_title = str(titulo_limpio)
        
        # Remove language suffixes
        for pattern in self.LANGUAGE_PATTERNS:
            clean_title = re.sub(pattern, '', clean_title, flags=re.IGNORECASE)
        
        # Remove format suffixes
        for pattern in self.FORMAT_PATTERNS:
            clean_title = re.sub(pattern, '', clean_title, flags=re.IGNORECASE)
        
        # Clean up any remaining artifacts
        clean_title = clean_title.strip()
        clean_title = re.sub(r'\s+', ' ', clean_title)
        
        return clean_title
    
    def generate_unique_name(self, clean_title: str) -> str:
        """
        Level 3 normalization: Generate canonical unique identifier
        """
        if pd.isna(clean_title) or clean_title == '':
            return ''
        
        unique_name = str(clean_title).upper()
        
        # Remove all format and language patterns
        for pattern in self.ALL_PATTERNS:
            unique_name = re.sub(pattern, '', unique_name, flags=re.IGNORECASE)
        
        # Remove special characters but keep letters, numbers and spaces
        unique_name = re.sub(r'[^\w\s]', ' ', unique_name)
        
        # Normalize multiple spaces
        unique_name = re.sub(r'\s+', ' ', unique_name)
        
        # Remove common articles (if needed)
        unique_name = re.sub(r'^(THE|LA|EL|LOS|LAS|UN|UNA)\s+', '', unique_name)
        
        return unique_name.strip()
    
    def detect_family(self, titulo_limpio_clean: str) -> str:
        """
        Detect movie family/franchise
        """
        if pd.isna(titulo_limpio_clean) or titulo_limpio_clean == '':
            return titulo_limpio_clean
        
        family = str(titulo_limpio_clean)
        
        # Remove sequel numbers and subtitles to get base franchise name
        # Patterns for sequels: "Movie 2", "Movie III", "Movie: Subtitle"
        family = re.sub(r'\s+\d+\s*$', '', family)  # Remove trailing numbers
        family = re.sub(r'\s+[IVX]+\s*$', '', family)  # Remove Roman numerals
        family = re.sub(r':\s+.*$', '', family)  # Remove subtitle after colon
        family = re.sub(r'\s+-\s+.*$', '', family)  # Remove subtitle after dash
        
        return family.strip()
    
    def normalize_duration(self, duration_str: str) -> str:
        """
        Normalize duration format
        """
        if pd.isna(duration_str) or duration_str == '':
            return ''
        
        # Extract numbers from duration string
        numbers = re.findall(r'\d+', str(duration_str))
        
        if numbers:
            minutes = numbers[0]
            return f"{minutes} minutos"
        
        return ''
    
    def process_batch(self, df_batch: pd.DataFrame) -> pd.DataFrame:
        """
        Process a batch of records - ALL records, not just unique ones
        """
        # Create output dataframe from ALL records in batch
        output_df = pd.DataFrame()
        
        # Keep ALL movie names, not just unique ones
        output_df['MOVIE_NAME'] = df_batch['MOVIE_NAME']
        
        # Level 1: TITULO_LIMPIO
        output_df['TITULO_LIMPIO'] = df_batch['MOVIE_NAME'].apply(self.normalize_title_level1)
        
        # Extract format and language
        output_df['FORMATO'] = df_batch.apply(
            lambda x: self.extract_format(x.get('MOVIE_NAME', ''), x.get('MOVIE_FORMAT', '')), 
            axis=1
        )
        output_df['IDIOMA'] = df_batch.apply(
            lambda x: self.extract_language(x.get('MOVIE_NAME', ''), x.get('MOVIE_LENGUAJE', '')), 
            axis=1
        )
        
        # Level 2: Clean titles
        output_df['NOMBRE_ORIGINAL_CLEAN'] = output_df['TITULO_LIMPIO'].apply(self.normalize_title_level2)
        output_df['TITULO_LIMPIO_CLEAN'] = output_df['NOMBRE_ORIGINAL_CLEAN']
        
        # Level 3: Unique name
        output_df['NOMBRE_UNICO'] = output_df['NOMBRE_ORIGINAL_CLEAN'].apply(self.generate_unique_name)
        
        # Family detection
        output_df['FAMILIA'] = output_df['NOMBRE_ORIGINAL_CLEAN'].apply(self.detect_family)
        
        # Duration
        if 'MOVIE_DURATION' in df_batch.columns:
            output_df['DURACION'] = df_batch['MOVIE_DURATION'].apply(self.normalize_duration)
        else:
            output_df['DURACION'] = ''
        
        # Fields requiring enrichment (placeholders)
        output_df['CATEGORIA'] = ''
        output_df['DESCRIPCION'] = ''
        output_df['NOMBRE_ORIGINAL'] = output_df['MOVIE_NAME']
        output_df['DESCRIPCION2'] = output_df['DESCRIPCION']
        output_df['ACTOR_PRINCIPAL'] = ''
        output_df['DIRECTOR'] = ''
        output_df['CATEGORIA_CINEPOLIS'] = ''
        
        return output_df
    
    def validate_input_data(self) -> bool:
        """
        Validate that input data exists and is readable
        """
        if not os.path.exists(self.input_file):
            error_msg = f"""
            ❌ ERROR: Input file not found!
            
            The file '{self.input_file}' does not exist.
            
            Please follow these steps:
            1. Execute the SQL query from 'input_data/input_query.sql'
            2. Export the results to CSV format
            3. Save the file as 'input_data/Cinepolis.csv'
            
            Note: In production, this will be automated through direct database connection.
            """
            logger.error(error_msg)
            print(error_msg)
            return False
        
        # Check if file is readable and has data
        try:
            test_df = pd.read_csv(self.input_file, nrows=5)
            required_columns = ['MOVIE_NAME', 'MOVIE_LENGUAJE', 'MOVIE_FORMAT', 'MOVIE_DURATION']
            missing_columns = [col for col in required_columns if col not in test_df.columns]
            
            if missing_columns:
                logger.error(f"Missing required columns in input file: {missing_columns}")
                return False
                
            logger.info(f"✓ Input file validated: {self.input_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error reading input file: {e}")
            return False
    
    def create_empty_template(self) -> pd.DataFrame:
        """
        Create an empty template with the correct structure
        """
        template_columns = [
            'MOVIE_ID', 'MOVIE_NAME', 'TITULO_LIMPIO', 'FORMATO', 'IDIOMA',
            'CATEGORIA', 'DESCRIPCION', 'FAMILIA', 'NOMBRE_ORIGINAL',
            'DESCRIPCION2', 'ACTOR_PRINCIPAL', 'DIRECTOR', 'DURACION',
            'CATEGORIA_CINEPOLIS', 'NOMBRE_ORIGINAL_CLEAN', 'TITULO_LIMPIO_CLEAN',
            'NOMBRE_UNICO'
        ]
        
        empty_df = pd.DataFrame(columns=template_columns)
        
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(self.output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
        
        # Save empty template if template file doesn't exist
        if not os.path.exists(self.template_file):
            empty_df.to_csv(self.template_file, index=False)
            logger.info(f"Created empty template file: {self.template_file}")
        
        return empty_df
    
    def load_existing_catalog(self) -> pd.DataFrame:
        """
        Load existing catalog from template or output file, or create new one
        """
        # First try to load the existing output file
        if os.path.exists(self.output_file):
            logger.info(f"Loading existing catalog from {self.output_file}")
            return pd.read_csv(self.output_file)
        
        # Otherwise load the template
        if os.path.exists(self.template_file):
            logger.info(f"Loading template catalog from {self.template_file}")
            return pd.read_csv(self.template_file)
        
        # If neither exists, create empty template
        logger.warning("No existing catalog found, creating new empty template")
        return self.create_empty_template()
    
    def ensure_consistency(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final consistency check to ensure all records with same NOMBRE_UNICO
        have the same MOVIE_ID and consistent data
        """
        logger.info("Running final consistency check...")
        
        # Group by NOMBRE_UNICO and ensure consistent MOVIE_ID
        unique_mapping = {}
        inconsistencies_fixed = 0
        
        for nombre_unico in df['NOMBRE_UNICO'].unique():
            mask = df['NOMBRE_UNICO'] == nombre_unico
            group = df[mask]
            
            # Get the most common MOVIE_ID for this NOMBRE_UNICO
            movie_ids = group['MOVIE_ID'].value_counts()
            if len(movie_ids) > 1:
                # Inconsistency found - fix it
                correct_id = movie_ids.index[0]  # Most common ID
                df.loc[mask, 'MOVIE_ID'] = correct_id
                inconsistencies_fixed += len(group) - movie_ids.iloc[0]
            
            # Ensure consistent metadata within group
            for field in ['CATEGORIA', 'DESCRIPCION', 'ACTOR_PRINCIPAL', 'DIRECTOR', 'FAMILIA']:
                non_empty = group[field].dropna()
                non_empty = non_empty[non_empty != '']
                if len(non_empty) > 0:
                    most_common = non_empty.mode()[0] if len(non_empty.mode()) > 0 else non_empty.iloc[0]
                    empty_mask = mask & ((df[field].isna()) | (df[field] == ''))
                    if empty_mask.any():
                        df.loc[empty_mask, field] = most_common
        
        if inconsistencies_fixed > 0:
            logger.warning(f"Fixed {inconsistencies_fixed} MOVIE_ID inconsistencies")
        else:
            logger.info("✓ All MOVIE_IDs are consistent")
        
        return df
    
    def process_all_records(self) -> Dict:
        """
        Process ALL records from input, creating a complete mapping
        Optimized for maximum performance with 500K batch size
        """
        # Validate input data first
        if not self.validate_input_data():
            raise FileNotFoundError("Input data validation failed. Please check the logs.")
        
        start_time = datetime.now()
        logger.info("Starting COMPLETE movie standardization (ALL records)")
        
        # Optimized batch size for memory efficiency
        OPTIMAL_BATCH_SIZE = 500000  # Process 500K records at once
        
        # First pass: Build unique movie catalog
        logger.info("Pass 1: Building unique movie catalog...")
        unique_catalog = {}
        movie_id_counter = 1
        
        # Load existing template catalog
        if os.path.exists(self.template_file):
            template_df = pd.read_csv(self.template_file)
            for _, row in template_df.iterrows():
                nombre_unico = str(row['NOMBRE_UNICO']).upper() if 'NOMBRE_UNICO' in row else ''
                if nombre_unico and nombre_unico not in unique_catalog:
                    unique_catalog[nombre_unico] = {
                        'MOVIE_ID': row['MOVIE_ID'] if 'MOVIE_ID' in row else movie_id_counter,
                        **row.to_dict()
                    }
                    movie_id_counter = max(movie_id_counter, row['MOVIE_ID'] + 1 if 'MOVIE_ID' in row else movie_id_counter + 1)
        
        # Quick scan for unique movies with large batches
        total_lines = sum(1 for line in open(self.input_file, 'r', encoding='utf-8', errors='ignore')) - 1
        
        with tqdm(total=total_lines, desc="Scanning for unique movies") as pbar:
            for chunk in pd.read_csv(self.input_file, chunksize=OPTIMAL_BATCH_SIZE, 
                                    low_memory=False, on_bad_lines='skip'):
                chunk = chunk[chunk['MOVIE_NAME'].notna()]
                
                # Vectorized processing for speed
                chunk['MOVIE_NAME_UPPER'] = chunk['MOVIE_NAME'].str.upper()
                chunk['TITULO_LIMPIO'] = chunk['MOVIE_NAME_UPPER']
                
                # Batch extract format and language
                chunk['FORMATO'] = chunk.apply(
                    lambda x: self.extract_format(x.get('MOVIE_NAME', ''), x.get('MOVIE_FORMAT', '')), 
                    axis=1
                )
                chunk['IDIOMA'] = chunk.apply(
                    lambda x: self.extract_language(x.get('MOVIE_NAME', ''), x.get('MOVIE_LENGUAJE', '')), 
                    axis=1
                )
                
                # Vectorized normalization
                chunk['NOMBRE_ORIGINAL_CLEAN'] = chunk['TITULO_LIMPIO'].apply(self.normalize_title_level2)
                chunk['NOMBRE_UNICO'] = chunk['NOMBRE_ORIGINAL_CLEAN'].apply(self.generate_unique_name)
                
                # Get unique names in this chunk
                unique_in_chunk = chunk.drop_duplicates(subset=['NOMBRE_UNICO'])
                
                for _, row in unique_in_chunk.iterrows():
                    nombre_unico = str(row['NOMBRE_UNICO']).upper()
                    if nombre_unico and nombre_unico not in unique_catalog:
                        unique_catalog[nombre_unico] = {
                            'MOVIE_ID': movie_id_counter,
                            'NOMBRE_UNICO': nombre_unico,
                            'FAMILIA': self.detect_family(row['NOMBRE_ORIGINAL_CLEAN']),
                            'DURACION': self.normalize_duration(row.get('MOVIE_DURATION', ''))
                        }
                        movie_id_counter += 1
                
                pbar.update(len(chunk))
        
        logger.info(f"Found {len(unique_catalog)} unique movies")
        
        # Second pass: Process ALL records with mapping
        logger.info("Pass 2: Processing ALL records with movie ID mapping...")
        
        output_file = self.output_file
        total_records = 0
        
        # Process in large chunks and write directly
        first_chunk = True
        
        with tqdm(total=total_lines, desc="Processing ALL records") as pbar:
            for chunk_num, chunk in enumerate(pd.read_csv(self.input_file, 
                                                         chunksize=OPTIMAL_BATCH_SIZE,
                                                         low_memory=False, 
                                                         on_bad_lines='skip')):
                chunk = chunk[chunk['MOVIE_NAME'].notna()]
                total_records += len(chunk)
                
                # Batch process entire chunk
                processed = self.process_batch(chunk)
                
                # Vectorized ID mapping
                processed['NOMBRE_UNICO_UPPER'] = processed['NOMBRE_UNICO'].str.upper()
                processed['MOVIE_ID'] = processed['NOMBRE_UNICO_UPPER'].map(
                    lambda x: unique_catalog.get(x, {}).get('MOVIE_ID', 0)
                )
                
                # Fill missing FAMILIA and DURACION from catalog
                processed['FAMILIA'] = processed.apply(
                    lambda x: unique_catalog.get(x['NOMBRE_UNICO_UPPER'], {}).get('FAMILIA', x['FAMILIA']),
                    axis=1
                )
                
                # Drop temporary column
                processed = processed.drop('NOMBRE_UNICO_UPPER', axis=1)
                
                # Reorder columns
                column_order = [
                    'MOVIE_ID', 'MOVIE_NAME', 'TITULO_LIMPIO', 'FORMATO', 'IDIOMA',
                    'CATEGORIA', 'DESCRIPCION', 'FAMILIA', 'NOMBRE_ORIGINAL',
                    'DESCRIPCION2', 'ACTOR_PRINCIPAL', 'DIRECTOR', 'DURACION',
                    'CATEGORIA_CINEPOLIS', 'NOMBRE_ORIGINAL_CLEAN', 'TITULO_LIMPIO_CLEAN',
                    'NOMBRE_UNICO'
                ]
                
                for col in column_order:
                    if col not in processed.columns:
                        processed[col] = ''
                
                processed = processed[column_order]
                
                # Write to file
                if first_chunk:
                    processed.to_csv(output_file, index=False, mode='w')
                    first_chunk = False
                else:
                    processed.to_csv(output_file, index=False, mode='a', header=False)
                
                pbar.update(len(chunk))
                
                if (chunk_num + 1) % 5 == 0:
                    logger.info(f"Written {total_records:,} records to output")
        
        # Calculate final metrics
        processing_time = (datetime.now() - start_time).total_seconds()
        
        results = {
            'total_input_records': total_records,
            'total_output_records': total_records,
            'unique_movies': len(unique_catalog),
            'compression_ratio': f"{total_records/len(unique_catalog):.1f}:1" if len(unique_catalog) > 0 else "N/A",
            'processing_time': processing_time,
            'records_per_second': int(total_records / processing_time) if processing_time > 0 else 0
        }
        
        # Log summary
        logger.info("=" * 50)
        logger.info("COMPLETE PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total input records: {results['total_input_records']:,}")
        logger.info(f"Total output records: {results['total_output_records']:,}")
        logger.info(f"Unique movies identified: {results['unique_movies']:,}")
        logger.info(f"Compression ratio: {results['compression_ratio']}")
        logger.info(f"Processing time: {results['processing_time']:.2f} seconds")
        logger.info(f"Processing speed: {results['records_per_second']:,} records/second")
        logger.info("=" * 50)
        
        return results
    
    def process(self, enable_enrichment: bool = False, preserve_existing: bool = True) -> Dict:
        """
        Main processing method - processes ALL records and maps them to unique movie IDs
        """
        start_time = datetime.now()
        logger.info("Starting movie standardization process")
        
        # Load existing catalog for unique movie definitions
        existing_catalog = self.load_existing_catalog()
        existing_count = len(existing_catalog)
        logger.info(f"Existing catalog has {existing_count} unique movies")
        
        # Build a mapping of NOMBRE_UNICO to MOVIE_ID from existing catalog
        unique_to_id = {}
        if not existing_catalog.empty and 'NOMBRE_UNICO' in existing_catalog.columns:
            for _, row in existing_catalog.iterrows():
                unique_to_id[str(row['NOMBRE_UNICO']).upper()] = row['MOVIE_ID']
        
        # Process ALL input records
        all_records = []
        unique_movies = {}  # Track unique movies
        total_records = 0
        next_movie_id = max(unique_to_id.values()) + 1 if unique_to_id else 1
        
        try:
            # Read and process in chunks
            logger.info(f"Processing input file: {self.input_file}")
            
            # Get total number of lines for progress bar
            total_lines = sum(1 for line in open(self.input_file, 'r', encoding='utf-8', errors='ignore')) - 1
            
            with tqdm(total=total_lines, desc="Processing ALL records") as pbar:
                for chunk_num, chunk in enumerate(pd.read_csv(
                    self.input_file, 
                    chunksize=self.batch_size,
                    low_memory=False,
                    on_bad_lines='skip'
                )):
                    # Filter valid records
                    chunk = chunk[chunk['MOVIE_NAME'].notna()]
                    total_records += len(chunk)
                    
                    # Process ALL records in chunk
                    processed_chunk = self.process_batch(chunk)
                    
                    # Assign MOVIE_ID to each record based on NOMBRE_UNICO
                    movie_ids = []
                    for _, row in processed_chunk.iterrows():
                        nombre_unico = str(row['NOMBRE_UNICO']).upper()
                        
                        # Check if this unique name already has an ID
                        if nombre_unico in unique_to_id:
                            movie_id = unique_to_id[nombre_unico]
                        else:
                            # New unique movie - assign new ID
                            movie_id = next_movie_id
                            unique_to_id[nombre_unico] = movie_id
                            next_movie_id += 1
                            
                            # Track this as a unique movie
                            if nombre_unico not in unique_movies:
                                unique_movies[nombre_unico] = row.to_dict()
                                unique_movies[nombre_unico]['MOVIE_ID'] = movie_id
                        
                        movie_ids.append(movie_id)
                    
                    processed_chunk['MOVIE_ID'] = movie_ids
                    all_records.append(processed_chunk)
                    
                    pbar.update(len(chunk))
                    
                    if (chunk_num + 1) % 10 == 0:
                        logger.info(f"Processed {(chunk_num + 1) * self.batch_size} records")
            
            # Combine all processed records
            if all_records:
                final_catalog = pd.concat(all_records, ignore_index=True)
                
                logger.info(f"Processed {len(final_catalog)} total records")
                logger.info(f"Found {len(unique_movies)} new unique movies")
                logger.info(f"Total unique movies: {len(unique_to_id)}")
                
                # Reorder columns to match template
                column_order = [
                    'MOVIE_ID', 'MOVIE_NAME', 'TITULO_LIMPIO', 'FORMATO', 'IDIOMA',
                    'CATEGORIA', 'DESCRIPCION', 'FAMILIA', 'NOMBRE_ORIGINAL',
                    'DESCRIPCION2', 'ACTOR_PRINCIPAL', 'DIRECTOR', 'DURACION',
                    'CATEGORIA_CINEPOLIS', 'NOMBRE_ORIGINAL_CLEAN', 'TITULO_LIMPIO_CLEAN',
                    'NOMBRE_UNICO'
                ]
                
                # Ensure all columns exist
                for col in column_order:
                    if col not in final_catalog.columns:
                        final_catalog[col] = ''
                
                final_catalog = final_catalog[column_order]
                
                # Save to output file
                final_catalog.to_csv(self.output_file, index=False)
                logger.info(f"Saved {len(final_catalog)} records to {self.output_file}")
                
                # Calculate metrics
                processing_time = (datetime.now() - start_time).total_seconds()
                
                results = {
                    'total_records': total_records,
                    'output_records': len(final_catalog),
                    'unique_movies': len(unique_to_id),
                    'new_unique_movies': len(unique_movies),
                    'compression_ratio': f"{total_records/len(unique_to_id):.1f}:1" if len(unique_to_id) > 0 else "N/A",
                    'processing_time': processing_time
                }
                
            else:
                logger.info("No records found to process")
                results = {
                    'total_records': 0,
                    'output_records': 0,
                    'unique_movies': existing_count,
                    'new_unique_movies': 0,
                    'compression_ratio': "N/A",
                    'processing_time': (datetime.now() - start_time).total_seconds()
                }
            
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            raise
        
        # Log summary
        logger.info("=" * 50)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total input records: {results['total_records']:,}")
        logger.info(f"Total output records: {results['output_records']:,}")
        logger.info(f"Total unique movies: {results['unique_movies']:,}")
        logger.info(f"New unique movies found: {results['new_unique_movies']:,}")
        logger.info(f"Compression ratio: {results['compression_ratio']}")
        logger.info(f"Processing time: {results['processing_time']:.2f} seconds")
        logger.info("=" * 50)
        
        return results


def main():
    """
    Main execution function
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Movie Standardizer - Cinema Transaction Deduplication')
    parser.add_argument('--input', default='input_data/Cinepolis.csv', help='Input CSV file path')
    parser.add_argument('--output', default='output_data/output_data.csv', help='Output CSV file path')
    parser.add_argument('--template', default='output_data/example_output_data.csv', help='Template CSV file path')
    parser.add_argument('--batch-size', type=int, default=100000, help='Batch size for processing')
    parser.add_argument('--all-records', action='store_true', help='Process ALL records (not just unique movies)')
    parser.add_argument('--enable-enrichment', action='store_true', help='Enable external data enrichment')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing catalog instead of appending')
    
    args = parser.parse_args()
    
    # Initialize standardizer
    standardizer = MovieStandardizer(
        input_file=args.input,
        output_file=args.output,
        template_file=args.template,
        batch_size=args.batch_size
    )
    
    # Process based on mode
    if args.all_records:
        # Process ALL records with mapping to unique IDs
        results = standardizer.process_all_records()
    else:
        # Process only unique movies (original behavior)
        results = standardizer.process(
            enable_enrichment=args.enable_enrichment,
            preserve_existing=not args.overwrite
        )
    
    return 0


if __name__ == "__main__":
    sys.exit(main())