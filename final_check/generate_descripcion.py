#!/usr/bin/env python3
"""
Generate descripcion.csv - NOMBRE_UNICO to DESCRIPCION mapping
Extrae mapeo único de películas y descripciones desde MOVIES_MASTER_FINAL.csv
"""

import pandas as pd
import sys

def generate_descripcion_mapping():
    """Genera descripcion.csv con mapeo único NOMBRE_UNICO → DESCRIPCION"""
    
    print("📝 Generando descripcion.csv...")
    print("-" * 50)
    
    try:
        # Cargar dataset principal
        df = pd.read_csv('MOVIES_MASTER_FINAL.csv')
        print(f"✅ Dataset principal cargado: {len(df)} registros")
        
        # Extraer solo NOMBRE_UNICO y DESCRIPCION, eliminando duplicados
        desc_df = df[['NOMBRE_UNICO', 'DESCRIPCION']].drop_duplicates(subset=['NOMBRE_UNICO'])
        
        # Verificar que no hay valores nulos
        if desc_df['DESCRIPCION'].isna().any():
            print("⚠️ Se encontraron descripciones nulas, corrigiendo...")
            desc_df = desc_df.dropna(subset=['DESCRIPCION'])
        
        # Guardar archivo
        desc_df.to_csv('descripcion.csv', index=False)
        
        print(f"✅ Archivo descripcion.csv generado exitosamente")
        print(f"📊 Registros únicos: {len(desc_df)}")
        print(f"📝 Columnas: {list(desc_df.columns)}")
        print(f"🎬 Películas únicas mapeadas: {desc_df['NOMBRE_UNICO'].nunique()}")
        
        # Estadísticas de las descripciones
        avg_length = desc_df['DESCRIPCION'].str.len().mean()
        print(f"📏 Longitud promedio descripción: {avg_length:.1f} caracteres")
        
        # Muestra de los primeros registros
        print(f"\n🎭 Muestra de mapeos generados:")
        print("-" * 50)
        sample = desc_df.head(3)
        for _, row in sample.iterrows():
            nombre = row['NOMBRE_UNICO'][:40] + "..." if len(row['NOMBRE_UNICO']) > 40 else row['NOMBRE_UNICO']
            desc = row['DESCRIPCION'][:80] + "..." if len(row['DESCRIPCION']) > 80 else row['DESCRIPCION']
            print(f"• {nombre}")
            print(f"  → {desc}")
            print()
        
        return True
        
    except FileNotFoundError:
        print("❌ ERROR: No se encontró MOVIES_MASTER_FINAL.csv")
        print("   Asegúrate de estar en el directorio final_check/")
        return False
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def validate_descripcion_file():
    """Valida el archivo descripcion.csv generado"""
    
    try:
        desc_df = pd.read_csv('descripcion.csv')
        
        validations = [
            ("Tiene exactamente 2 columnas", len(desc_df.columns) == 2),
            ("Columnas correctas", list(desc_df.columns) == ['NOMBRE_UNICO', 'DESCRIPCION']),
            ("Sin nombres duplicados", desc_df['NOMBRE_UNICO'].nunique() == len(desc_df)),
            ("Sin descripciones vacías", desc_df['DESCRIPCION'].notna().all()),
            ("Registros > 800", len(desc_df) > 800)
        ]
        
        print(f"\n🔍 Validación de descripcion.csv:")
        print("-" * 50)
        
        all_passed = True
        for check_name, result in validations:
            status = "✅" if result else "❌"
            print(f"{status} {check_name}")
            if not result:
                all_passed = False
        
        if all_passed:
            print("\n🎉 descripcion.csv generado correctamente y validado ✅")
        else:
            print("\n⚠️ Hay problemas con el archivo generado")
            
        return all_passed
        
    except FileNotFoundError:
        print("❌ No se encontró descripcion.csv")
        return False

def main():
    """Función principal"""
    
    print("🎬 GENERADOR DE MAPEO DE DESCRIPCIONES")
    print("=" * 60)
    
    # Generar archivo
    success = generate_descripcion_mapping()
    
    if success:
        # Validar archivo generado
        valid = validate_descripcion_file()
        
        if valid:
            print("\n✅ PROCESO COMPLETADO EXITOSAMENTE")
            print("📄 Archivo: descripcion.csv")
            print("🎯 Uso: Para análisis de descripciones de películas únicas")
            return True
    
    print("\n❌ PROCESO FALLÓ")
    return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
