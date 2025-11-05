"""
Script para sincronizar usuarios de BigQuery a Firestore

USO:
  1. Obtener token de Firebase (desde la app móvil o Firebase Console)
  2. Ejecutar: python sync_users_firestore.py <TOKEN>
  
  O simplemente ejecutar y seguir las instrucciones en pantalla.
"""
import requests
import sys
import os

# Configuración
API_URL = "https://consultas-app-cliente-596669043554.us-east1.run.app"
ENDPOINT = "/api/admin/sync-users-to-firestore"

def sync_users(token: str):
    """Llama al endpoint de sincronización"""
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        print(f"[INFO] Llamando a {API_URL}{ENDPOINT}...")
        response = requests.post(
            f"{API_URL}{ENDPOINT}",
            headers=headers,
            timeout=300  # 5 minutos timeout
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"\n[OK] Sincronización completada exitosamente!")
            print(f"   Total usuarios: {result.get('total_users', 0)}")
            print(f"   Sincronizados: {result.get('synced', 0)}")
            print(f"   Errores: {result.get('errors', 0)}")
            
            if result.get('error_details'):
                print(f"\n[WARNING] Errores encontrados:")
                for error in result['error_details']:
                    print(f"   - {error}")
            
            print(f"\n{result.get('message', '')}")
            return True
        else:
            print(f"[ERROR] Error en la sincronización:")
            print(f"   Status: {response.status_code}")
            print(f"   Respuesta: {response.text}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Error ejecutando sincronización: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Sincronización de Usuarios BigQuery → Firestore")
    print("=" * 60)
    print()
    
    # Opción 1: Token desde argumento
    if len(sys.argv) > 1:
        token = sys.argv[1]
    else:
        # Opción 2: Pedir token
        print("Este script requiere un token de Firebase de un usuario ADMIN.")
        print("Puedes obtener el token de:")
        print("  1. La app móvil (logs de Android Studio)")
        print("  2. Firebase Console → Authentication")
        print("  3. O ejecutar: python sync_users_firestore.py <TOKEN>")
        print()
        token = input("Ingresa el token de Firebase (o presiona Enter para salir): ").strip()
        
        if not token:
            print("[INFO] Operación cancelada.")
            sys.exit(0)
    
    print()
    print(f"[INFO] Token recibido (primeros 20 caracteres): {token[:20]}...")
    print()
    
    # Confirmar
    confirm = input("¿Deseas continuar con la sincronización? (s/n): ").strip().lower()
    if confirm != 's':
        print("[INFO] Operación cancelada.")
        sys.exit(0)
    
    print()
    success = sync_users(token)
    
    if success:
        print("\n[OK] Proceso completado. Verifica en Firebase Console → Firestore → users")
    else:
        print("\n[ERROR] La sincronización falló. Revisa los errores arriba.")
        sys.exit(1)

