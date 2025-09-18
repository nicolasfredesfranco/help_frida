#!/usr/bin/env python3
"""
Movies Master Final Processor
Procesador definitivo para generar MOVIES_MASTER_FINAL.csv con máxima calidad

Aplica:
- Categorías de una palabra más adecuada
- Limpieza de símbolos en campos clave
- Remapeo perfecto NOMBRE_UNICO ↔ MOVIE_ID
- Coherencia vertical completa
- Validación comprehensive
"""

import pandas as pd
import numpy as np
import re
import json
from collections import defaultdict

class MoviesMasterProcessor:
    """Procesador final definitivo para Movies Master Dataset"""
    
    def __init__(self):
        self.corrections = defaultdict(int)
        
    def process_complete_dataset(self, input_file='MOVIES_MASTER_FINAL.csv'):
        """Procesa dataset completo hasta perfección"""
        
        print("🎬 MOVIES MASTER FINAL PROCESSOR")
        print("="*60)
        
        # Cargar datos
        df = pd.read_csv(input_file)
        print(f"📊 Dataset cargado: {len(df):,} registros")
        
        # Aplicar todas las correcciones específicas
        df = self.fix_categories_single_word(df)
        df = self.clean_symbols_from_key_fields(df)
        df = self.remap_movie_ids_perfect(df)
        df = self.ensure_vertical_coherence(df)
        df = self.apply_professional_formatting(df)
        
        # Validar resultado final
        is_perfect = self.validate_final_quality(df)
        
        # Guardar resultado
        df.to_csv('MOVIES_MASTER_FINAL.csv', index=False)
        self.save_quality_metrics(df, is_perfect)
        
        return df, is_perfect
    
    def fix_categories_single_word(self, df):
        """Convierte categorías a UNA palabra la más adecuada"""
        
        print("\n📂 CATEGORÍAS: Una palabra más adecuada")
        print("-" * 50)
        
        category_map = {
            'ACCIÓN': 'ACCION', 'ACTION': 'ACCION', 'AVENTURA': 'AVENTURA',
            'DRAMA': 'DRAMA', 'BIOGRAFÍA': 'DRAMA', 'HISTÓRICO': 'DRAMA',
            'COMEDIA': 'COMEDIA', 'COMEDY': 'COMEDIA',
            'TERROR': 'TERROR', 'HORROR': 'TERROR',
            'THRILLER': 'THRILLER', 'SUSPENSE': 'THRILLER',
            'CIENCIA': 'CIENCIA_FICCION', 'FANTASÍA': 'FANTASIA',
            'ANIMACIÓN': 'ANIMACION', 'ANIMATION': 'ANIMACION',
            'ROMANCE': 'ROMANCE', 'DOCUMENTAL': 'DOCUMENTAL',
            'MUSICAL': 'MUSICAL'
        }
        
        def get_best_category(cat_text):
            if pd.isna(cat_text):
                return 'DRAMA'
            
            cat_str = str(cat_text).upper().strip()
            
            # Mapeo directo
            if cat_str in category_map:
                return category_map[cat_str]
            
            # Análisis de palabras múltiples
            words = re.split(r'[,;\s/]+', cat_str)
            priorities = ['ACCION', 'TERROR', 'COMEDIA', 'DRAMA', 'ANIMACION']
            
            for priority in priorities:
                for word in words:
                    if word.strip() in category_map and category_map[word.strip()] == priority:
                        return priority
            
            # Primera palabra válida o DRAMA
            for word in words:
                if len(word.strip()) > 2 and word.strip().isalpha():
                    return word.strip()[:12]
            
            return 'DRAMA'
        
        df['CATEGORIA'] = df['CATEGORIA'].apply(get_best_category)
        df['CATEGORIA_CINEPOLIS'] = df['CATEGORIA']
        
        print(f"✅ Categorías únicas: {df['CATEGORIA'].nunique()}")
        return df
    
    def clean_symbols_from_key_fields(self, df):
        """Elimina símbolos de campos clave (solo letras, números, espacios)"""
        
        print("\n🔧 LIMPIEZA: Símbolos de campos clave")
        print("-" * 50)
        
        def clean_field(text):
            if pd.isna(text):
                return ''
            text_str = str(text)
            cleaned = re.sub(r'[^\w\s]', ' ', text_str)
            cleaned = re.sub(r'\s+', ' ', cleaned).strip().upper()
            return cleaned
        
        fields = ['NOMBRE_UNICO', 'TITULO_LIMPIO_CLEAN', 'NOMBRE_ORIGINAL_CLEAN', 'FAMILIA']
        
        for field in fields:
            if field in df.columns:
                df[field] = df[field].apply(clean_field)
                print(f"✅ {field}: limpiado")
        
        return df
    
    def remap_movie_ids_perfect(self, df):
        """Remapea MOVIE_ID basado en NOMBRE_UNICO únicos"""
        
        print("\n🔄 REMAPEO: MOVIE_ID perfecto 1:1")
        print("-" * 50)
        
        # Crear mapeo único
        unique_names = sorted(df['NOMBRE_UNICO'].unique())
        name_to_id = {name: idx + 1 for idx, name in enumerate(unique_names)}
        
        # Aplicar mapeo
        df['MOVIE_ID'] = df['NOMBRE_UNICO'].map(name_to_id)
        
        print(f"✅ Nombres únicos: {len(unique_names)}")
        print(f"✅ Mapeo 1:1: perfecto")
        
        return df
    
    def ensure_vertical_coherence(self, df):
        """Asegura coherencia vertical (mismo NOMBRE_UNICO = mismos metadatos)"""
        
        print("\n🔗 COHERENCIA: Vertical completa")
        print("-" * 50)
        
        coherent_fields = ['CATEGORIA', 'CATEGORIA_CINEPOLIS', 'DIRECTOR', 
                          'ACTOR_PRINCIPAL', 'DESCRIPCION', 'DURACION', 'FAMILIA']
        
        for nombre_unico in df['NOMBRE_UNICO'].unique():
            mask = df['NOMBRE_UNICO'] == nombre_unico
            
            for field in coherent_fields:
                if field in df.columns:
                    values = df.loc[mask, field].dropna()
                    if len(values) > 0:
                        most_common = values.mode().iloc[0] if len(values.mode()) > 0 else values.iloc[0]
                        df.loc[mask, field] = most_common
        
        # Asegurar coherencia horizontal
        df['CATEGORIA_CINEPOLIS'] = df['CATEGORIA']
        
        print("✅ Coherencia vertical: aplicada")
        print("✅ Coherencia horizontal: CATEGORIA = CATEGORIA_CINEPOLIS")
        
        return df
    
    def apply_professional_formatting(self, df):
        """Aplica formato profesional consistente"""
        
        print("\n🎨 FORMATO: Profesional consistente")
        print("-" * 50)
        
        # Campos en mayúsculas
        for field in ['CATEGORIA', 'CATEGORIA_CINEPOLIS', 'FORMATO', 'IDIOMA']:
            if field in df.columns:
                df[field] = df[field].str.upper()
        
        # Campos en formato título
        if 'DIRECTOR' in df.columns:
            df['DIRECTOR'] = df['DIRECTOR'].apply(lambda x: str(x).title() if pd.notna(x) else '')
        
        # Limpiar actores
        if 'ACTOR_PRINCIPAL' in df.columns:
            df['ACTOR_PRINCIPAL'] = df['ACTOR_PRINCIPAL'].apply(self.clean_actor_field)
        
        # Estandarizar duraciones
        if 'DURACION' in df.columns:
            df['DURACION'] = df['DURACION'].apply(self.standardize_duration)
        
        print("✅ Formato profesional: aplicado")
        return df
    
    def clean_actor_field(self, actor_text):
        """Limpia campo de actores"""
        if pd.isna(actor_text):
            return ''
        actor_str = str(actor_text).strip()
        actor_str = re.sub(r'^\d+\s*personas?', '', actor_str, flags=re.IGNORECASE)
        actor_str = re.sub(r'\d+\s*$', '', actor_str)
        return actor_str.strip()
    
    def standardize_duration(self, duration):
        """Estandariza formato de duración"""
        if pd.isna(duration):
            return ''
        duration_str = str(duration)
        numbers = re.findall(r'\d+', duration_str)
        if numbers:
            return f"{numbers[0]} minutos"
        return duration_str
    
    def validate_final_quality(self, df):
        """Validación final comprehensive"""
        
        print(f"\n🔍 VALIDACIÓN FINAL")
        print("-" * 50)
        
        checks = [
            ("Estructura 2372×16", len(df) == 2372 and len(df.columns) == 16),
            ("Mapeo 1:1", df.groupby('NOMBRE_UNICO')['MOVIE_ID'].nunique().max() == 1),
            ("Sin duplicados", df.duplicated().sum() == 0),
            ("Coherencia horizontal", (df['CATEGORIA'] == df['CATEGORIA_CINEPOLIS']).all()),
            ("Categorías una palabra", all(len(str(cat).split()) == 1 for cat in df['CATEGORIA'].unique() if pd.notna(cat))),
            ("Campos limpios", not df['NOMBRE_UNICO'].str.contains(r'[^\w\s]', na=False, regex=True).any())
        ]
        
        passed = 0
        for check_name, result in checks:
            status = "✅" if result else "❌"
            print(f"{status} {check_name}")
            if result:
                passed += 1
        
        score = (passed / len(checks)) * 100
        is_perfect = passed == len(checks)
        
        print(f"\n🏆 CALIDAD: {score:.1f}/100")
        print(f"📊 Nombres únicos finales: {df['NOMBRE_UNICO'].nunique()}")
        
        return is_perfect
    
    def save_quality_metrics(self, df, is_perfect):
        """Guarda métricas finales"""
        
        metrics = {
            "dataset_final": {
                "structure": f"{len(df)}x{len(df.columns)}",
                "unique_movies": int(df['NOMBRE_UNICO'].nunique()),
                "quality_perfect": is_perfect,
                "validations": {
                    "perfect_mapping": bool(df.groupby('NOMBRE_UNICO')['MOVIE_ID'].nunique().max() == 1),
                    "categories_single_word": bool(all(len(str(cat).split()) == 1 for cat in df['CATEGORIA'].unique() if pd.notna(cat))),
                    "horizontal_coherence": bool((df['CATEGORIA'] == df['CATEGORIA_CINEPOLIS']).all()),
                    "no_duplicates": bool(df.duplicated().sum() == 0)
                }
            }
        }
        
        with open('FINAL_QUALITY_METRICS.json', 'w') as f:
            json.dump(metrics, f, indent=2)

def main():
    """Función principal"""
    
    processor = MoviesMasterProcessor()
    df, is_perfect = processor.process_complete_dataset()
    
    print(f"\n🎊 PROCESAMIENTO COMPLETADO")
    print(f"💎 Archivo: MOVIES_MASTER_FINAL.csv")
    print(f"🏆 Calidad: {'PERFECTA' if is_perfect else 'ALTA'}")
    print(f"🎯 Listo para: EXTERNAL_SOURCES.CINEPOLIS.MOVIES_MASTER")
    
    return is_perfect

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
