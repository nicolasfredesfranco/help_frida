#!/usr/bin/env python3
"""
Complete Movie Processing Pipeline
Processes ALL records, maps to unique IDs, and enriches with complete information
"""

import sys
import os
import logging
from datetime import datetime

# Import our modules
from estandarizer import MovieStandardizer
from movie_enricher import MovieEnricher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('complete_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_complete_pipeline():
    """
    Run the complete movie processing pipeline
    """
    start_time = datetime.now()
    logger.info("="*70)
    logger.info("COMPLETE MOVIE PROCESSING PIPELINE - STARTING")
    logger.info("="*70)
    
    # Step 1: Process ALL records with standardization
    logger.info("\n" + "="*70)
    logger.info("STEP 1: STANDARDIZING ALL RECORDS")
    logger.info("="*70)
    
    standardizer = MovieStandardizer(
        input_file='input_data/Cinepolis.csv',
        output_file='output_data/output_data_all.csv',
        template_file='output_data/example_output_data.csv',
        batch_size=500000  # Optimized batch size
    )
    
    # Process all records
    std_results = standardizer.process_all_records()
    
    # Step 2: Enrich the data
    logger.info("\n" + "="*70)
    logger.info("STEP 2: ENRICHING DATA")
    logger.info("="*70)
    
    enricher = MovieEnricher(
        input_file='output_data/output_data_all.csv',
        output_file='output_data/output_data_final.csv'
    )
    
    # Enrich the data
    enriched_df = enricher.process()
    
    # Final summary
    total_time = (datetime.now() - start_time).total_seconds()
    
    logger.info("\n" + "="*70)
    logger.info("COMPLETE PIPELINE SUMMARY")
    logger.info("="*70)
    logger.info(f"Total processing time: {total_time:.2f} seconds")
    logger.info(f"Total records processed: {std_results['total_output_records']:,}")
    logger.info(f"Unique movies identified: {std_results['unique_movies']:,}")
    logger.info(f"Compression ratio: {std_results['compression_ratio']}")
    logger.info(f"Processing speed: {std_results.get('records_per_second', 0):,} records/second")
    logger.info("="*70)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info(f"Final output: output_data/output_data_final.csv")
    logger.info("="*70)


if __name__ == "__main__":
    try:
        run_complete_pipeline()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
