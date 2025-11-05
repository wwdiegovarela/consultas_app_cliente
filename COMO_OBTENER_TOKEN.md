# 🔑 Cómo Obtener el Token de Firebase

El token de Firebase es necesario para ejecutar el script de sincronización de usuarios. Aquí tienes **3 formas** de obtenerlo:

---

## 📱 **Opción 1: Desde la App Móvil (MÁS FÁCIL)** ⭐

### Pasos:
1. **Abre la app** en tu dispositivo o emulador
2. **Inicia sesión** con un usuario **ADMIN** (`es_admin = true`)
3. Ve a la pantalla de **"Cerrar Sesión"** (última pestaña del menú inferior)
4. Verás un botón azul **"Ver Token Firebase"** (solo visible para ADMIN)
5. Presiona el botón
6. Se abrirá un diálogo con el token completo
7. **Copia el token** (puedes seleccionarlo y copiarlo)

### ⚠️ Importante:
- El token **expira después de 1 hora**
- Si expira, simplemente vuelve a presionar el botón para obtener uno nuevo
- Solo usuarios ADMIN pueden ver este botón

---

## 💻 **Opción 2: Desde los Logs de Android Studio**

### Pasos:
1. Abre **Android Studio**
2. Conecta tu dispositivo o emulador
3. Ejecuta la app en modo debug
4. Inicia sesión con un usuario ADMIN
5. En la pestaña **"Logcat"**, busca:
   ```
   🔑 ID Token obtenido
   ```
   O busca en los logs cuando se autentica:
   ```
   ✅ Login exitoso: [email]
   🔑 ID Token obtenido, enviando petición...
   ```

### Nota:
El token completo no siempre se imprime en los logs por seguridad. Es mejor usar la **Opción 1**.

---

## 🌐 **Opción 3: Desde Firebase Console (NO RECOMENDADO)**

### Pasos:
1. Ve a [Firebase Console](https://console.firebase.google.com/)
2. Selecciona tu proyecto: **worldwide-470917**
3. Ve a **Authentication** → **Users**
4. Busca el usuario ADMIN
5. **Problema**: Firebase Console solo muestra el **UID**, no el **ID Token**

### ⚠️ Limitación:
Firebase Console **NO muestra el ID Token** directamente. Solo muestra el UID, que es diferente.

Para obtener el token desde la consola, necesitarías:
- Usar Firebase CLI
- O usar la API de Firebase Admin SDK

**Por eso, la Opción 1 es la más fácil.**

---

## 🚀 **Usar el Token**

Una vez que tengas el token:

### Método 1: Como argumento
```bash
cd C:\Users\DiegV\Documents\Proyectos\Worldwide\Backend_App_Cliente
python sync_users_firestore.py <TU_TOKEN_AQUI>
```

### Método 2: Interactivo
```bash
cd C:\Users\DiegV\Documents\Proyectos\Worldwide\Backend_App_Cliente
python sync_users_firestore.py
```
Luego pega el token cuando te lo pida.

---

## ❓ **Preguntas Frecuentes**

### ¿Por qué necesito un usuario ADMIN?
El endpoint `/api/admin/sync-users-to-firestore` requiere permisos de administrador por seguridad. Solo usuarios con `es_admin = true` pueden ejecutarlo.

### ¿El token es seguro compartirlo?
El token expira en 1 hora. Si lo compartes temporalmente para ejecutar el script, es relativamente seguro, pero **no lo compartas públicamente ni lo guardes en repositorios**.

### ¿Qué pasa si el token expira?
Si el token expira mientras ejecutas el script, obtendrás un error `401 Unauthorized`. Simplemente:
1. Obtén un nuevo token (Opción 1)
2. Ejecuta el script nuevamente

### ¿Puedo obtener el token de otra forma?
Sí, también puedes usar:
- **Firebase CLI**: `firebase auth:export users.json` (requiere configuración)
- **Postman/Insomnia**: Con la extensión de Firebase Auth
- **Código personalizado**: Usando Firebase Admin SDK

Pero la **Opción 1 (desde la app)** es la más simple y directa.

---

## ✅ **Resumen Rápido**

1. Abre la app → Inicia sesión como ADMIN
2. Ve a "Cerrar Sesión"
3. Presiona "Ver Token Firebase"
4. Copia el token
5. Ejecuta: `python sync_users_firestore.py <TOKEN>`

¡Listo! 🎉

