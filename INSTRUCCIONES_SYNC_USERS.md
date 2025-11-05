# 🔄 Instrucciones para Sincronizar Usuarios a Firestore

## 📋 Paso a Paso

### 1. Obtener Token de Firebase

Tienes varias opciones para obtener el token:

#### Opción A: Desde la App Móvil (Recomendado)
1. Abre la app en Android Studio o dispositivo
2. Inicia sesión con un usuario ADMIN
3. En los logs de Android Studio, busca:
   ```
   🔔 Token FCM: xxxxxx...
   ```
   O busca en los logs de Firebase Auth cuando se autentica el usuario.

#### Opción B: Desde Firebase Console
1. Ve a Firebase Console → Authentication
2. Busca el usuario ADMIN
3. Copia el UID (pero necesitas el token, no el UID)

#### Opción C: Desde la Terminal (si tienes Firebase CLI)
```bash
firebase auth:export users.json
```

### 2. Ejecutar el Script

#### Método 1: Con token como argumento
```bash
cd C:\Users\DiegV\Documents\Proyectos\Worldwide\Backend_App_Cliente
python sync_users_firestore.py <TU_TOKEN_FIREBASE>
```

#### Método 2: Interactivo
```bash
cd C:\Users\DiegV\Documents\Proyectos\Worldwide\Backend_App_Cliente
python sync_users_firestore.py
```
Luego sigue las instrucciones en pantalla.

### 3. Verificar Resultado

1. Ve a Firebase Console → Firestore → colección `users`
2. Deberías ver documentos con todos los usuarios sincronizados
3. Cada documento tiene:
   - `uid`: Firebase UID del usuario
   - `email`: Email del usuario
   - `nombre_completo`: Nombre completo
   - `role`: Rol (CLIENTE, ADMIN_WFSA, etc.)
   - `rol_nombre`: Nombre del rol
   - `cliente_rol`: Cliente asociado
   - `updatedAt`: Timestamp de última actualización

## ⚠️ Requisitos

- El usuario debe ser ADMIN (`es_admin = true` en BigQuery)
- El usuario debe tener `firebase_uid` en `v_permisos_usuarios`
- Solo se sincronizan usuarios activos con `firebase_uid` no nulo

## 🔍 Solución de Problemas

### Error: "Token inválido"
- Verifica que el token sea reciente (los tokens expiran)
- Asegúrate de usar el token de un usuario ADMIN

### Error: "Missing or insufficient permissions"
- Verifica que el usuario tenga `es_admin = true`
- Verifica que el token sea válido

### Error: "BigQuery client no está inicializado"
- El backend necesita credenciales de GCP configuradas
- Esto es normal si lo ejecutas localmente sin credenciales

## 📝 Nota

Este script llama al endpoint del backend desplegado. Si el backend no está desplegado o no está accesible, el script fallará.

