# Security Application Registry (SAR)

**Inventario operativo de aplicaciones orientado a Application Security y arquitectura segura.**

SAR conecta cuatro perspectivas de un mismo sistema —proyecto, aplicación, componente de código y contexto de ejecución— para facilitar la trazabilidad, la revisión de datos y la identificación de inconsistencias.

La idea es deliberadamente sencilla: **un libro Excel mantiene los datos; Python aporta estructura, validaciones, relaciones, vistas y una interfaz para trabajar con ellos.** No es necesario implantar una nueva plataforma de gestión para empezar.

> Proyecto personal y de código abierto. SAR propone una forma práctica de organizar información que normalmente está dispersa; no pretende sustituir por completo una CMDB ni una plataforma de gobierno o descubrimiento de activos.

## El problema que aborda

Conocer el nombre de una aplicación no equivale a conocer cómo está construida o dónde se ejecuta. Para analizar su exposición y asignar responsabilidades es útil relacionar su contexto organizativo con los repositorios, los componentes técnicos y los entornos de ejecución.

SAR aporta **un modelo común para registrar esas relaciones, comprobar su consistencia y consultarlas**. No presupone que un inventario vaya a ser exhaustivo por utilizar una herramienta determinada: la cobertura y la vigencia de los datos dependen de cómo se alimenten y mantengan.

## Modelo C1–C4

| Nivel | Representa | Pregunta principal |
|---|---|---|
| **C1 — Proyecto** | Contexto organizativo y responsabilidad | ¿Bajo qué iniciativa o ámbito se gestiona? |
| **C2 — Aplicación** | Unidad funcional | ¿Qué aplicación presta la funcionalidad? |
| **C3 — Componente** | Unidad de código, por ejemplo API, frontend o proceso | ¿Qué software la implementa? |
| **C4 — Runtime** | Contexto concreto de ejecución de un componente | ¿Dónde y en qué condiciones se ejecuta? |

La relación estructural es `C1 → C2 → C3 → C4`. Una aplicación puede agrupar varios componentes, y un componente puede tener varios runtimes, por ejemplo en distintos entornos. Cada elemento mantiene una referencia a su nivel superior.

La estructura C1–C4 es fija; **los campos adicionales y las etiquetas visibles de los niveles pueden adaptarse** mediante el libro Excel.

## Qué permite hacer

- **Mantener el inventario** mediante Excel y una interfaz web para consultar, crear y editar registros.
- **Explorar las relaciones** entre proyectos, aplicaciones, componentes y runtimes, con vistas y diagramas contextuales.
- **Detectar inconsistencias** estructurales y de datos: identificadores, referencias jerárquicas, campos obligatorios y valores no válidos.
- **Definir validaciones desde el Excel**, usando las hojas `LOOKUPS` y `RULES` sin modificar el código para cada nuevo atributo.
- **Calcular vistas derivadas** y consultar incidencias (*issues*) para orientar la revisión del inventario.
- **Generar informes** de cadenas C1–C4 en HTML y DOCX; la imagen del diagrama en el DOCX depende de disponer de Mermaid CLI.
- **Gestionar el esquema** de forma explícita mediante una plantilla base y operaciones de comparación, promoción y migración de columnas.

La autenticación admite usuarios locales y roles `viewer`, `editor` y `admin`.

## Por qué Excel

Excel es una **decisión de diseño**, no un almacenamiento temporal a la espera de una base de datos.

Permite inspeccionar, modificar, compartir de forma controlada y conservar el inventario con herramientas ampliamente disponibles. El motor Python se ocupa de las validaciones y relaciones que una hoja de cálculo por sí sola no garantiza.

Esta elección es especialmente adecuada para **uso local o controlado, con un proceso claro de mantenimiento de datos**. No implica que Excel ofrezca por sí solo transacciones, resolución de ediciones simultáneas o aislamiento entre usuarios concurrentes.

Tampoco se equipara el descubrimiento automático de recursos con la comprensión de una aplicación: detectar un servidor o un repositorio no identifica necesariamente su función, responsables ni relaciones de negocio. SAR se centra en **estructurar y validar información conocida**, haciendo visibles las lagunas e inconsistencias de los registros existentes.

## Instalación y primer uso

**Requisitos:** Python 3.10 o posterior y las dependencias del proyecto. Los siguientes pasos están pensados para una ejecución **local**, desde la carpeta raíz del repositorio.

1. Clona el proyecto y entra en su carpeta:

   ```powershell
   git clone https://github.com/begobe1978/security-application-registry.git
   cd security-application-registry
   ```

2. Instala la aplicación:

   ```powershell
   python -m pip install -e .
   ```

3. Crea un usuario local. Elige el rol `admin` para poder crear un inventario a partir de la plantilla:

   ```powershell
   python scripts/create_user.py
   ```

   Las credenciales se guardan localmente en `data/users.yml`, que está excluido de Git. **No publiques este archivo.**

4. Configura una clave de sesión para este proceso y las cookies para HTTP local, y arranca el servidor:

   ```powershell
   $env:SECRET_KEY = python -c "import secrets; print(secrets.token_urlsafe(48))"
   $env:SAR_COOKIE_SECURE = "false"
   python -m uvicorn sar.app:app --host 127.0.0.1 --port 8000
   ```

5. Abre **http://127.0.0.1:8000**, inicia sesión y utiliza la opción de **crear un registro desde la plantilla**. La plantilla incluida es `data/registry_template.xlsx`.

La clave del ejemplo es válida mientras dure esa sesión del servidor: al reiniciarlo con otra clave, las sesiones anteriores dejan de ser válidas. Para un despliegue persistente debe configurarse una clave estable y secreta.

**Windows:** también existe `run_sar.bat` para facilitar el arranque. Para el primer uso se recomienda seguir los comandos anteriores porque dejan explícita la configuración local.

**Otros sistemas:** los comandos de instalación y `uvicorn` son equivalentes, pero la asignación de variables de entorno depende de la shell utilizada.

## Cómo se organiza el Excel

El libro contiene las hojas estructurales `C1`, `C2`, `C3` y `C4`, junto con:

- **`META`:** metadatos del inventario y etiquetas de presentación.
- **`LOOKUPS`:** conjuntos de valores admitidos por campo y nivel.
- **`RULES`:** reglas de validación declarativas.

Los identificadores (`id`) y referencias (`c1_id`, `c2_id`, `c3_id`) mantienen la integridad de las relaciones. Las validaciones esenciales del modelo no dependen de la configuración de `LOOKUPS` o `RULES`.

Una incidencia señala un dato que conviene revisar; **no equivale automáticamente a una vulnerabilidad confirmada**. El valor de la información resultante depende de la calidad y actualización del inventario.

Para más detalle técnico, consulta [los principios de diseño](docs/design.md).

## Alcance y condiciones de uso

SAR es una aplicación funcional de **alcance deliberadamente acotado**. Su objetivo es proporcionar un inventario trazable, editable y validable, no ofrecer todas las capacidades de una plataforma empresarial.

- **Despliegue:** pensado inicialmente para ejecución local o en un entorno controlado. No se presenta como un servicio multiusuario endurecido para Internet.
- **Concurrencia:** el libro Excel y el estado activo de la aplicación no están diseñados como un backend de edición simultánea independiente por usuario. Evita operar con varios procesos o editores concurrentes sobre el mismo inventario.
- **Acceso a la red:** para uso local, inicia el servidor en `127.0.0.1`. Exponerlo en otras interfaces o redes requiere evaluar autenticación, TLS, acceso a archivos, almacenamiento y operación segura.
- **Datos:** no introduzcas secretos en el inventario ni publiques libros con información interna o personal. Mantén copias de seguridad y controla quién puede acceder a ellas.
- **Cobertura:** SAR no descubre automáticamente los activos no registrados. La completitud del inventario depende de los datos facilitados y de su mantenimiento.

Posibles líneas de evolución, **sin constituir compromisos ni requisitos para el alcance actual**, incluyen la sincronización con fuentes externas, relaciones transversales entre sistemas y mecanismos específicos para operación concurrente.

## Desarrollo y pruebas

Para instalar las dependencias de desarrollo y ejecutar las pruebas:

```powershell
python -m pip install -e ".[dev]"
python -m pytest -q
```

La existencia de pruebas automatizadas no sustituye la validación del despliegue concreto ni garantiza ausencia de defectos.

## Licencia y autoría

Publicado bajo la **GNU Affero General Public License v3.0 (AGPLv3)**. Consulta [LICENSE](LICENSE).

Proyecto creado y mantenido por **Bernardo Gómez Bey**.
