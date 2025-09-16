#!/usr/bin/env python3
"""
Comprehensive Data Quality Evaluator for Standardized Movie Dataset
Analyzes completeness, quality, consistency, and richness of the enriched movie data
"""

import pandas as pd
import numpy as np
import re
import json
from collections import Counter
from typing import Dict, List, Tuple
import logging
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# Optional imports for visualizations
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_quality_report.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataQualityEvaluator:
    """
    Comprehensive data quality analysis for movie dataset
    """
    
    def __init__(self, file_path: str = 'standardized.csv', sample_size: int = None):
        """
        Initialize evaluator
        
        Args:
            file_path: Path to standardized.csv
            sample_size: Number of rows to sample (None for full dataset)
        """
        self.file_path = file_path
        self.sample_size = sample_size
        self.df = None
        self.metrics = {}
        self.report = {}
        
    def load_data(self):
        """Load dataset for analysis"""
        logger.info(f"Loading data from {self.file_path}...")
        
        if self.sample_size:
            self.df = pd.read_csv(self.file_path, nrows=self.sample_size)
            logger.info(f"Loaded sample of {len(self.df):,} rows")
        else:
            self.df = pd.read_csv(self.file_path)
            logger.info(f"Loaded full dataset: {len(self.df):,} rows")
            
        logger.info(f"Columns: {list(self.df.columns)}")
        logger.info(f"Memory usage: {self.df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
    def calculate_completeness_metrics(self) -> Dict:
        """Calculate completeness metrics for all fields"""
        logger.info("Calculating completeness metrics...")
        
        completeness = {}
        total_cells = self.df.size
        
        for column in self.df.columns:
            # Count non-empty cells
            non_empty = self.df[column].notna() & (self.df[column] != '')
            completeness[column] = {
                'filled_count': non_empty.sum(),
                'filled_percentage': (non_empty.sum() / len(self.df)) * 100,
                'empty_count': len(self.df) - non_empty.sum(),
                'empty_percentage': ((len(self.df) - non_empty.sum()) / len(self.df)) * 100
            }
        
        # Overall metrics
        total_empty = sum([col['empty_count'] for col in completeness.values()])
        overall_completeness = {
            'total_cells': total_cells,
            'total_empty_cells': total_empty,
            'total_filled_cells': total_cells - total_empty,
            'overall_completeness_percentage': ((total_cells - total_empty) / total_cells) * 100,
            'overall_empty_percentage': (total_empty / total_cells) * 100
        }
        
        self.metrics['completeness'] = completeness
        self.metrics['overall_completeness'] = overall_completeness
        
        return completeness
    
    def analyze_description_quality(self) -> Dict:
        """Analyze quality of description fields"""
        logger.info("Analyzing description quality...")
        
        desc_columns = ['DESCRIPCION', 'DESCRIPCION2']
        desc_analysis = {}
        
        for col in desc_columns:
            if col in self.df.columns:
                descriptions = self.df[col].dropna()
                descriptions = descriptions[descriptions != '']
                
                if len(descriptions) > 0:
                    # Length statistics
                    lengths = descriptions.str.len()
                    word_counts = descriptions.str.split().str.len()
                    
                    # Quality indicators
                    has_punctuation = descriptions.str.contains(r'[.!?]').sum()
                    has_capital = descriptions.str.contains(r'^[A-Z]').sum()
                    is_truncated = descriptions.str.contains(r'\.\.\.$').sum()
                    
                    # Richness indicators
                    unique_words = set()
                    for desc in descriptions.sample(min(1000, len(descriptions))):
                        words = str(desc).lower().split()
                        unique_words.update(words)
                    
                    desc_analysis[col] = {
                        'count': len(descriptions),
                        'avg_length': lengths.mean(),
                        'min_length': lengths.min(),
                        'max_length': lengths.max(),
                        'median_length': lengths.median(),
                        'avg_word_count': word_counts.mean(),
                        'min_word_count': word_counts.min(),
                        'max_word_count': word_counts.max(),
                        'has_punctuation_pct': (has_punctuation / len(descriptions)) * 100,
                        'starts_with_capital_pct': (has_capital / len(descriptions)) * 100,
                        'is_truncated_pct': (is_truncated / len(descriptions)) * 100,
                        'vocabulary_richness': len(unique_words),
                        'quality_score': self._calculate_quality_score(descriptions)
                    }
        
        self.metrics['description_quality'] = desc_analysis
        return desc_analysis
    
    def _calculate_quality_score(self, descriptions: pd.Series) -> float:
        """Calculate quality score for descriptions (0-100)"""
        scores = []
        
        for desc in descriptions.sample(min(1000, len(descriptions))):
            score = 0
            desc_str = str(desc)
            
            # Length score (optimal 100-500 chars)
            length = len(desc_str)
            if 100 <= length <= 500:
                score += 25
            elif 50 <= length < 100 or 500 < length <= 1000:
                score += 15
            elif length > 0:
                score += 5
            
            # Punctuation score
            if re.search(r'[.!?]', desc_str):
                score += 20
            
            # Capitalization score
            if desc_str and desc_str[0].isupper():
                score += 10
            
            # Word diversity score
            words = desc_str.lower().split()
            if len(words) > 0:
                unique_ratio = len(set(words)) / len(words)
                score += unique_ratio * 20
            
            # Completeness indicators
            if not desc_str.endswith('...'):
                score += 10
            
            # Information richness
            if any(keyword in desc_str.lower() for keyword in ['director', 'actor', 'año', 'género']):
                score += 15
            
            scores.append(min(score, 100))
        
        return np.mean(scores) if scores else 0
    
    def analyze_category_distribution(self) -> Dict:
        """Analyze category distribution and diversity"""
        logger.info("Analyzing category distribution...")
        
        cat_analysis = {}
        cat_columns = ['CATEGORIA', 'CATEGORIA_CINEPOLIS']
        
        for col in cat_columns:
            if col in self.df.columns:
                categories = self.df[col].dropna()
                categories = categories[categories != '']
                
                if len(categories) > 0:
                    # Category distribution
                    cat_counts = categories.value_counts()
                    
                    # Split multi-category entries
                    all_cats = []
                    for cat in categories:
                        if ',' in str(cat):
                            all_cats.extend([c.strip() for c in str(cat).split(',')])
                        else:
                            all_cats.append(str(cat).strip())
                    
                    unique_cats = Counter(all_cats)
                    
                    cat_analysis[col] = {
                        'total_entries': len(categories),
                        'unique_categories': len(cat_counts),
                        'unique_category_tokens': len(unique_cats),
                        'top_10_categories': dict(cat_counts.head(10)),
                        'top_10_tokens': dict(unique_cats.most_common(10)),
                        'diversity_index': len(unique_cats) / len(all_cats) if all_cats else 0,
                        'most_common': cat_counts.index[0] if len(cat_counts) > 0 else None,
                        'most_common_count': cat_counts.iloc[0] if len(cat_counts) > 0 else 0,
                        'coverage_top_5': (cat_counts.head(5).sum() / len(categories)) * 100 if len(categories) > 0 else 0
                    }
        
        self.metrics['category_distribution'] = cat_analysis
        return cat_analysis
    
    def analyze_data_consistency(self) -> Dict:
        """Analyze data consistency and coherence"""
        logger.info("Analyzing data consistency...")
        
        consistency = {}
        
        # Check ID uniqueness
        consistency['movie_id_uniqueness'] = {
            'total_ids': len(self.df['MOVIE_ID']) if 'MOVIE_ID' in self.df.columns else 0,
            'unique_ids': self.df['MOVIE_ID'].nunique() if 'MOVIE_ID' in self.df.columns else 0,
            'has_duplicates': len(self.df['MOVIE_ID']) != self.df['MOVIE_ID'].nunique() if 'MOVIE_ID' in self.df.columns else False
        }
        
        # Check NOMBRE_UNICO consistency
        if 'NOMBRE_UNICO' in self.df.columns and 'MOVIE_ID' in self.df.columns:
            grouped = self.df.groupby('NOMBRE_UNICO')['MOVIE_ID'].nunique()
            consistency['nombre_unico_consistency'] = {
                'unique_names': len(grouped),
                'names_with_single_id': (grouped == 1).sum(),
                'names_with_multiple_ids': (grouped > 1).sum(),
                'consistency_rate': ((grouped == 1).sum() / len(grouped)) * 100 if len(grouped) > 0 else 0
            }
        
        # Check format consistency
        if 'FORMATO' in self.df.columns:
            formats = self.df['FORMATO'].value_counts()
            consistency['format_distribution'] = {
                'unique_formats': len(formats),
                'formats': dict(formats),
                'most_common': formats.index[0] if len(formats) > 0 else None
            }
        
        # Check language consistency
        if 'IDIOMA' in self.df.columns:
            languages = self.df['IDIOMA'].value_counts()
            consistency['language_distribution'] = {
                'unique_languages': len(languages),
                'languages': dict(languages),
                'most_common': languages.index[0] if len(languages) > 0 else None
            }
        
        # Duration consistency
        if 'DURACION' in self.df.columns:
            durations = self.df['DURACION'].dropna()
            # Extract numeric duration
            numeric_durations = []
            for dur in durations:
                match = re.search(r'(\d+)', str(dur))
                if match:
                    numeric_durations.append(int(match.group(1)))
            
            if numeric_durations:
                consistency['duration_analysis'] = {
                    'avg_duration': np.mean(numeric_durations),
                    'min_duration': min(numeric_durations),
                    'max_duration': max(numeric_durations),
                    'median_duration': np.median(numeric_durations),
                    'std_duration': np.std(numeric_durations),
                    'outliers_below_60': sum(1 for d in numeric_durations if d < 60),
                    'outliers_above_240': sum(1 for d in numeric_durations if d > 240),
                    'format_consistency': (durations.str.contains('minutos').sum() / len(durations)) * 100
                }
        
        self.metrics['consistency'] = consistency
        return consistency
    
    def analyze_enrichment_patterns(self) -> Dict:
        """Analyze enrichment patterns and identify gaps"""
        logger.info("Analyzing enrichment patterns...")
        
        patterns = {}
        
        # Identify fields with highest missing rates
        missing_rates = {}
        for col in self.df.columns:
            missing = (self.df[col].isna() | (self.df[col] == '')).sum()
            missing_rates[col] = (missing / len(self.df)) * 100
        
        patterns['missing_rates'] = dict(sorted(missing_rates.items(), key=lambda x: x[1], reverse=True))
        patterns['most_incomplete_fields'] = list(patterns['missing_rates'].keys())[:5]
        
        # Correlation of completeness between fields
        if 'DIRECTOR' in self.df.columns and 'ACTOR_PRINCIPAL' in self.df.columns:
            has_director = ~(self.df['DIRECTOR'].isna() | (self.df['DIRECTOR'] == ''))
            has_actor = ~(self.df['ACTOR_PRINCIPAL'].isna() | (self.df['ACTOR_PRINCIPAL'] == ''))
            
            patterns['field_correlations'] = {
                'director_actor_correlation': {
                    'both_present': (has_director & has_actor).sum(),
                    'only_director': (has_director & ~has_actor).sum(),
                    'only_actor': (~has_director & has_actor).sum(),
                    'both_missing': (~has_director & ~has_actor).sum()
                }
            }
        
        # Identify movies needing most enrichment
        if 'NOMBRE_UNICO' in self.df.columns:
            movie_completeness = {}
            for movie in self.df['NOMBRE_UNICO'].unique():
                if pd.notna(movie):
                    movie_data = self.df[self.df['NOMBRE_UNICO'] == movie].iloc[0]
                    empty_count = sum(1 for col in ['CATEGORIA', 'DESCRIPCION', 'DIRECTOR', 'ACTOR_PRINCIPAL'] 
                                     if col in self.df.columns and (pd.isna(movie_data[col]) or movie_data[col] == ''))
                    movie_completeness[movie] = empty_count
            
            # Top movies needing enrichment
            needs_enrichment = sorted(movie_completeness.items(), key=lambda x: x[1], reverse=True)[:10]
            patterns['movies_needing_enrichment'] = needs_enrichment
        
        self.metrics['enrichment_patterns'] = patterns
        return patterns
    
    def calculate_data_richness_score(self) -> float:
        """Calculate overall data richness score (0-100)"""
        logger.info("Calculating data richness score...")
        
        scores = []
        weights = {
            'completeness': 0.3,
            'description_quality': 0.25,
            'category_diversity': 0.15,
            'consistency': 0.15,
            'metadata_presence': 0.15
        }
        
        # Completeness score
        if 'overall_completeness' in self.metrics:
            completeness_score = self.metrics['overall_completeness']['overall_completeness_percentage']
            scores.append(('completeness', completeness_score))
        
        # Description quality score
        if 'description_quality' in self.metrics:
            desc_scores = []
            for col, analysis in self.metrics['description_quality'].items():
                if 'quality_score' in analysis:
                    desc_scores.append(analysis['quality_score'])
            if desc_scores:
                scores.append(('description_quality', np.mean(desc_scores)))
        
        # Category diversity score
        if 'category_distribution' in self.metrics:
            div_scores = []
            for col, analysis in self.metrics['category_distribution'].items():
                if 'diversity_index' in analysis:
                    div_scores.append(analysis['diversity_index'] * 100)
            if div_scores:
                scores.append(('category_diversity', np.mean(div_scores)))
        
        # Consistency score
        if 'consistency' in self.metrics:
            if 'nombre_unico_consistency' in self.metrics['consistency']:
                consistency_score = self.metrics['consistency']['nombre_unico_consistency']['consistency_rate']
                scores.append(('consistency', consistency_score))
        
        # Metadata presence score
        metadata_fields = ['DIRECTOR', 'ACTOR_PRINCIPAL', 'CATEGORIA', 'DESCRIPCION']
        metadata_scores = []
        for field in metadata_fields:
            if field in self.df.columns:
                filled_pct = ((self.df[field].notna() & (self.df[field] != '')).sum() / len(self.df)) * 100
                metadata_scores.append(filled_pct)
        if metadata_scores:
            scores.append(('metadata_presence', np.mean(metadata_scores)))
        
        # Calculate weighted score
        final_score = 0
        for category, score in scores:
            if category in weights:
                final_score += score * weights[category]
        
        self.metrics['data_richness_score'] = final_score
        return final_score
    
    def identify_improvement_opportunities(self) -> List[str]:
        """Identify specific improvement opportunities"""
        logger.info("Identifying improvement opportunities...")
        
        opportunities = []
        
        # Check completeness issues
        if 'completeness' in self.metrics:
            for field, stats in self.metrics['completeness'].items():
                if stats['empty_percentage'] > 50:
                    opportunities.append(f"Field '{field}' is {stats['empty_percentage']:.1f}% empty - needs enrichment")
        
        # Check description quality
        if 'description_quality' in self.metrics:
            for col, analysis in self.metrics['description_quality'].items():
                if 'quality_score' in analysis and analysis['quality_score'] < 50:
                    opportunities.append(f"Description quality in '{col}' is low ({analysis['quality_score']:.1f}/100) - improve content")
                if 'is_truncated_pct' in analysis and analysis['is_truncated_pct'] > 10:
                    opportunities.append(f"{analysis['is_truncated_pct']:.1f}% of '{col}' are truncated - fetch complete descriptions")
        
        # Check category coverage
        if 'category_distribution' in self.metrics:
            for col, analysis in self.metrics['category_distribution'].items():
                if 'diversity_index' in analysis and analysis['diversity_index'] < 0.1:
                    opportunities.append(f"Low category diversity in '{col}' - expand category classification")
        
        # Check consistency issues
        if 'consistency' in self.metrics:
            if 'nombre_unico_consistency' in self.metrics['consistency']:
                if self.metrics['consistency']['nombre_unico_consistency']['consistency_rate'] < 95:
                    opportunities.append("Movie ID consistency issues detected - review deduplication logic")
        
        # Check for systematic gaps
        if 'DIRECTOR' in self.df.columns:
            director_missing = (self.df['DIRECTOR'].isna() | (self.df['DIRECTOR'] == '')).sum() / len(self.df)
            if director_missing > 0.7:
                opportunities.append(f"Director information missing for {director_missing*100:.1f}% - consider additional data sources")
        
        if 'ACTOR_PRINCIPAL' in self.df.columns:
            actor_missing = (self.df['ACTOR_PRINCIPAL'].isna() | (self.df['ACTOR_PRINCIPAL'] == '')).sum() / len(self.df)
            if actor_missing > 0.3:
                opportunities.append(f"Actor information missing for {actor_missing*100:.1f}% - enhance web scraping")
        
        self.metrics['improvement_opportunities'] = opportunities
        return opportunities
    
    def generate_report(self) -> Dict:
        """Generate comprehensive quality report"""
        logger.info("="*60)
        logger.info("DATA QUALITY EVALUATION REPORT")
        logger.info("="*60)
        
        # Run all analyses
        self.calculate_completeness_metrics()
        self.analyze_description_quality()
        self.analyze_category_distribution()
        self.analyze_data_consistency()
        self.analyze_enrichment_patterns()
        richness_score = self.calculate_data_richness_score()
        opportunities = self.identify_improvement_opportunities()
        
        # Compile report
        self.report = {
            'summary': {
                'timestamp': datetime.now().isoformat(),
                'file': self.file_path,
                'total_rows': len(self.df),
                'total_columns': len(self.df.columns),
                'unique_movies': self.df['NOMBRE_UNICO'].nunique() if 'NOMBRE_UNICO' in self.df.columns else 0,
                'data_richness_score': richness_score,
                'overall_empty_percentage': self.metrics['overall_completeness']['overall_empty_percentage']
            },
            'completeness': self.metrics.get('completeness', {}),
            'description_quality': self.metrics.get('description_quality', {}),
            'category_distribution': self.metrics.get('category_distribution', {}),
            'consistency': self.metrics.get('consistency', {}),
            'enrichment_patterns': self.metrics.get('enrichment_patterns', {}),
            'improvement_opportunities': opportunities
        }
        
        # Print summary
        print("\n" + "="*60)
        print("📊 DATA QUALITY REPORT SUMMARY")
        print("="*60)
        print(f"📁 File: {self.file_path}")
        print(f"📏 Dataset: {self.report['summary']['total_rows']:,} rows × {self.report['summary']['total_columns']} columns")
        print(f"🎬 Unique Movies: {self.report['summary']['unique_movies']:,}")
        print(f"⭐ Data Richness Score: {self.report['summary']['data_richness_score']:.1f}/100")
        print(f"📈 Overall Completeness: {100 - self.report['summary']['overall_empty_percentage']:.1f}%")
        print(f"📉 Empty Cells: {self.report['summary']['overall_empty_percentage']:.1f}%")
        
        print("\n📊 FIELD COMPLETENESS:")
        print("-"*40)
        for field in ['CATEGORIA', 'DESCRIPCION', 'DIRECTOR', 'ACTOR_PRINCIPAL', 'DURACION']:
            if field in self.metrics['completeness']:
                stats = self.metrics['completeness'][field]
                print(f"{field:20} {stats['filled_percentage']:6.1f}% complete")
        
        if self.metrics.get('description_quality'):
            print("\n📝 DESCRIPTION QUALITY:")
            print("-"*40)
            for col, analysis in self.metrics['description_quality'].items():
                print(f"{col}:")
                print(f"  - Average length: {analysis['avg_length']:.0f} chars")
                print(f"  - Quality score: {analysis['quality_score']:.1f}/100")
                print(f"  - Vocabulary richness: {analysis['vocabulary_richness']:,} unique words")
        
        if self.metrics.get('category_distribution'):
            print("\n🏷️ CATEGORY ANALYSIS:")
            print("-"*40)
            for col, analysis in self.metrics['category_distribution'].items():
                print(f"{col}:")
                print(f"  - Unique categories: {analysis['unique_categories']}")
                print(f"  - Most common: {analysis['most_common']}")
                print(f"  - Diversity index: {analysis['diversity_index']:.3f}")
        
        print("\n⚠️ IMPROVEMENT OPPORTUNITIES:")
        print("-"*40)
        for i, opportunity in enumerate(opportunities[:5], 1):
            print(f"{i}. {opportunity}")
        
        print("\n" + "="*60)
        
        # Save report to JSON
        with open('data_quality_report.json', 'w', encoding='utf-8') as f:
            json.dump(self.report, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info("Report saved to data_quality_report.json")
        
        return self.report
    
    def generate_visualizations(self):
        """Generate quality visualization charts"""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            fig.suptitle('Data Quality Analysis Dashboard', fontsize=16)
            
            # 1. Completeness by field
            if 'completeness' in self.metrics:
                fields = []
                percentages = []
                for field in ['CATEGORIA', 'DESCRIPCION', 'DIRECTOR', 'ACTOR_PRINCIPAL']:
                    if field in self.metrics['completeness']:
                        fields.append(field)
                        percentages.append(self.metrics['completeness'][field]['filled_percentage'])
                
                axes[0, 0].barh(fields, percentages, color='steelblue')
                axes[0, 0].set_xlabel('Completeness (%)')
                axes[0, 0].set_title('Field Completeness')
                axes[0, 0].set_xlim(0, 100)
                
            # 2. Category distribution
            if 'category_distribution' in self.metrics and 'CATEGORIA' in self.metrics['category_distribution']:
                cat_data = self.metrics['category_distribution']['CATEGORIA']
                if 'top_10_tokens' in cat_data:
                    categories = list(cat_data['top_10_tokens'].keys())[:5]
                    counts = list(cat_data['top_10_tokens'].values())[:5]
                    axes[0, 1].bar(range(len(categories)), counts, color='coral')
                    axes[0, 1].set_xticks(range(len(categories)))
                    axes[0, 1].set_xticklabels(categories, rotation=45, ha='right')
                    axes[0, 1].set_ylabel('Count')
                    axes[0, 1].set_title('Top 5 Categories')
            
            # 3. Description length distribution
            if 'DESCRIPCION' in self.df.columns:
                desc_lengths = self.df['DESCRIPCION'].dropna().str.len()
                if len(desc_lengths) > 0:
                    axes[1, 0].hist(desc_lengths, bins=30, color='green', alpha=0.7, edgecolor='black')
                    axes[1, 0].set_xlabel('Description Length (chars)')
                    axes[1, 0].set_ylabel('Frequency')
                    axes[1, 0].set_title('Description Length Distribution')
                    axes[1, 0].axvline(desc_lengths.mean(), color='red', linestyle='--', label=f'Mean: {desc_lengths.mean():.0f}')
                    axes[1, 0].legend()
            
            # 4. Data quality scores
            scores = {
                'Completeness': 100 - self.report['summary']['overall_empty_percentage'],
                'Richness': self.report['summary']['data_richness_score'],
            }
            if 'description_quality' in self.metrics and 'DESCRIPCION' in self.metrics['description_quality']:
                scores['Desc. Quality'] = self.metrics['description_quality']['DESCRIPCION'].get('quality_score', 0)
            
            axes[1, 1].bar(scores.keys(), scores.values(), color=['blue', 'green', 'orange'])
            axes[1, 1].set_ylabel('Score (0-100)')
            axes[1, 1].set_title('Quality Metrics Summary')
            axes[1, 1].set_ylim(0, 100)
            
            plt.tight_layout()
            plt.savefig('data_quality_dashboard.png', dpi=150, bbox_inches='tight')
            logger.info("Visualizations saved to data_quality_dashboard.png")
            
        except ImportError:
            logger.warning("Matplotlib not available - skipping visualizations")
        except Exception as e:
            logger.error(f"Error generating visualizations: {e}")
    
    def run_evaluation(self):
        """Run complete evaluation pipeline"""
        self.load_data()
        report = self.generate_report()
        self.generate_visualizations()
        return report


def main():
    """Main execution function"""
    print("\n🔍 COMPREHENSIVE DATA QUALITY EVALUATOR")
    print("="*60)
    
    # Check if standardized.csv exists
    import os
    if not os.path.exists('standardized.csv'):
        print("⚠️ Warning: standardized.csv not found!")
        print("Using sample data from output_data.csv for demonstration...")
        file_path = 'output_data.csv'
        sample_size = 10000
    else:
        file_path = 'standardized.csv'
        # Use sampling for large file
        print("📊 Analyzing standardized.csv...")
        print("Note: Using sample of 100,000 rows for performance")
        sample_size = 100000
    
    evaluator = DataQualityEvaluator(file_path, sample_size)
    report = evaluator.run_evaluation()
    
    print("\n✅ Evaluation complete!")
    print("📁 Reports saved:")
    print("   - data_quality_report.json")
    print("   - data_quality_report.log")
    print("   - data_quality_dashboard.png")
    
    return report


if __name__ == "__main__":
    main()
