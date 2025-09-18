#!/usr/bin/env python3
"""
Quick Validation Script for MOVIES_MASTER_FINAL.csv
Ejecuta todas las validaciones críticas en segundos
"""

import pandas as pd
import sys

def validate_dataset(file_path='MOVIES_MASTER_FINAL.csv'):
    """Valida el dataset final con todas las verificaciones críticas"""
    
    print("🔍 Validando MOVIES_MASTER_FINAL.csv...")
    print("-" * 50)
    
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print("❌ ERROR: No se encuentra el archivo MOVIES_MASTER_FINAL.csv")
        return False
    except Exception as e:
        print(f"❌ ERROR al leer el archivo: {e}")
        return False
    
    # Lista de validaciones
    validations = []
    
    # 1. Estructura
    structure_ok = len(df) == 2372 and len(df.columns) == 16
    validations.append(("Estructura 2372×16", structure_ok))
    
    # 2. Películas únicas
    movies_ok = df['NOMBRE_UNICO'].nunique() == 885
    validations.append(("885 películas únicas", movies_ok))
    
    # 3. Mapeo 1:1
    mapping_ok = df.groupby('NOMBRE_UNICO')['MOVIE_ID'].nunique().max() == 1
    validations.append(("Mapeo 1:1 perfecto", mapping_ok))
    
    # 4. Sin duplicados
    no_duplicates = df.duplicated().sum() == 0
    validations.append(("Sin registros duplicados", no_duplicates))
    
    # 5. Coherencia horizontal categorías
    category_coherence = (df['CATEGORIA'] == df['CATEGORIA_CINEPOLIS']).all()
    validations.append(("Coherencia CATEGORIA", category_coherence))
    
    # 6. Categorías de una palabra
    single_word_categories = all(len(str(cat).split()) == 1 for cat in df['CATEGORIA'].unique())
    validations.append(("Categorías una palabra", single_word_categories))
    
    # 7. Completitud alta
    completeness = (df.notna().sum().sum() / (len(df) * len(df.columns))) * 100
    high_completeness = completeness > 95
    validations.append((f"Completitud > 95% ({completeness:.1f}%)", high_completeness))
    
    # 8. Coherencia IDIOMA
    idioma_errors = 0
    for idx, row in df.iterrows():
        movie_name = str(row['MOVIE_NAME']).upper()
        idioma = str(row['IDIOMA'])
        
        if movie_name.endswith(' ESP') and idioma != 'ESP':
            idioma_errors += 1
        elif movie_name.endswith(' SUB') and idioma != 'SUB':
            idioma_errors += 1
        elif (movie_name.endswith(' DOB') or movie_name.endswith(' DUB')) and idioma != 'DOB':
            idioma_errors += 1
    
    idioma_ok = idioma_errors == 0
    validations.append(("Coherencia IDIOMA", idioma_ok))
    
    # Imprimir resultados
    passed = 0
    for check_name, result in validations:
        status = "✅" if result else "❌"
        print(f"{status} {check_name}")
        if result:
            passed += 1
    
    # Resultado final
    print("-" * 50)
    score = (passed / len(validations)) * 100
    
    if score == 100:
        print(f"🎉 PERFECTO: {passed}/{len(validations)} validaciones pasadas")
        print("✅ Dataset certificado para producción")
        return True
    else:
        print(f"⚠️ ATENCIÓN: {passed}/{len(validations)} validaciones pasadas")
        print(f"Score: {score:.1f}/100")
        return False

def main():
    """Función principal"""
    is_valid = validate_dataset()
    
    if is_valid:
        print("\n✅ MOVIES_MASTER_FINAL.csv está listo para EXTERNAL_SOURCES.CINEPOLIS.MOVIES_MASTER")
        sys.exit(0)
    else:
        print("\n❌ El dataset requiere correcciones antes de producción")
        sys.exit(1)

if __name__ == "__main__":
    main()
