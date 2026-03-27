# Shopify Theme Utilities App

Una aplicación gráfica (GUI) construida en Python (Tkinter) para simplificar el desarrollo local de temas de Shopify utilizando Docker. 

Esta herramienta elimina la necesidad de instalar globalmente Node.js, Ruby o Shopify CLI en tu sistema, encapsulando y gestionando todo el entorno de desarrollo dentro de contenedores de Docker.

## Características Principales

### 1. Gestión de Contenedores
Gestiona tus entornos de desarrollo de Shopify directamente desde la aplicación.
- Iniciar, detener, pausar o eliminar contenedores.
- Abrir una terminal bash directamente en el contenedor.
- Ver los logs del contenedor en tiempo real.
- Abrir la tienda local en tu navegador con un solo clic.

### 2. Crear / Recrear Entorno
Esta función te permite inicializar un nuevo entorno de trabajo desde cero.
- **Crear**: Configura un nuevo contenedor Docker equipado con las dependencias necesarias (Node.js, Shopify CLI, Git). Descarga automáticamente el tema especificado desde tu tienda de Shopify y ejecuta `npm install`.
- **Recrear**: Si algo falla o el entorno se corrompe, puedes "Recrear" el contenedor. Esto elimina el contenedor antiguo y levanta uno nuevo limpio, volviendo a descargar el código fuente y las herramientas necesarias.

### 3. Importar Theme / Datos
Te permite cargar el código de un tema local (archivo ZIP o carpeta) y aplicarlo al contenedor.
- **Proceso automatizado**: Copia los archivos al contenedor (`/app/horizon`), ejecuta `npm install` y reinicia el servidor local automáticamente.
- **Autofix Inteligente (V4)**: Durante la importación, el sistema de Auto-fix escanea y repara discrepancias entre el `settings_data.json` y el schema. Por ejemplo, convierte valores numéricos a strings (requerimiento estricto de Shopify CLI) y ajusta valores que exceden los límites máximos permitidos por el schema (ej. `badge_corner_radius`).
- **Push a Remoto**: Opcionalmente, tras cargar el tema en el contenedor, se abre un asistente interactivo para subir (push) el código directamente a tu tienda de Shopify.

### 4. Exportar Theme / Datos
Te permite hacer una copia de seguridad o extraer el trabajo realizado en el contenedor hacia tu equipo local.
- **Modo Local**: Comprime y extrae la carpeta de trabajo actual del contenedor, guardándola de forma segura en tu escritorio o en la ruta elegida.
- **Modo Remoto**: Además de extraer los archivos locales, abre un asistente seguro en terminal para conectarse a tu tienda de Shopify y descargar (pull) la versión más reciente del tema alojado en producción, guardándolo todo organizado y empaquetado.

### 5. Mercados (Markets) y Utilidades Múltiples
Sincroniza información de los diferentes mercados configurados en tu panel de Shopify para probar la funcionalidad multi-región/divisa en local sin salir de la app.

---

## Requisitos de Sistema
- **Docker Admin / Docker Desktop**: Debe estar instalado y ejecutándose, ya que la aplicación utiliza comandos nativos de la CLI de Docker.
- **Python 3.10+**: Para ejecutar la interfaz gráfica.

## Cómo Usarlo
1. Asegúrate de tener **Docker** en ejecución en tu sistema Windows.
2. Ejecuta el archivo principal:  
   `python shopify_utilidades_app.py`
3. Usa la barra lateral para navegar entre la gestión de contenedores, la creación de entornos, o las pestañas de I/O de archivos (Importación y Exportación).
