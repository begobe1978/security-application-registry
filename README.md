![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)


# Security Application Registry (SAR)

SAR (Security Application Registry) es un registro operativo de aplicaciones orientado a **Application Security** y **Secure Architecture**. Su objetivo es hacer el riesgo **visible, derivable y accionable** a partir de un modelo estructurado, con **Excel como “source of truth”** y un motor en Python para validar, calcular vistas y detectar inconsistencias.

> Autor / Maintainer: Bernardo Gómez Bey

## Contexto y alcance

SAR no pretende sustituir herramientas de arquitectura empresarial, CMDBs ni plataformas de gobierno.

Proporciona una **visión operativa y específica** orientada a desarrollo y ciberseguridad, basada en cómo el software se desarrolla, se despliega y se ejecuta realmente. En lugar de modelar arquitecturas ideales o exhaustivas, SAR modela **proyectos, aplicaciones, componentes de código y sus contextos de ejecución concretos**, que es donde se materializan los cambios y el riesgo.

El objetivo es reducir fricción entre desarrollo, seguridad y herramientas corporativas, capturando únicamente información que los equipos pueden mantener de forma realista, pero con suficiente estructura para servir como input fiable a plataformas más amplias.

---

## Modelo (C1–C4)

SAR utiliza un modelo jerárquico de cuatro niveles para representar aplicaciones desde una perspectiva de **negocio, código y ejecución**, manteniendo cada responsabilidad claramente separada.

### C1 — Proyecto (unidad de negocio)
Unidad de negocio y gobierno. Representa el nivel más alto de responsabilidad (sponsor/ownership) bajo el cual existen una o más aplicaciones.

Los proyectos **no contienen código** ni describen ejecución técnica.

### C2 — Aplicación (unidad funcional)
Unidad funcional de negocio. Vive bajo un proyecto (C1) y agrupa uno o varios componentes técnicos que la implementan.

La aplicación describe el **qué** (función), no el **cómo** (implementación) ni el **dónde** (ejecución).

### C3 — Componente (unidad de código)
Unidad técnica de código dentro de una aplicación (por ejemplo: frontend, backend, API, worker).

Es el nivel donde:
- existe el código,
- se define el repositorio,
- viven decisiones técnicas y el SDLC.

Un componente **no está ligado a un único entorno ni a un único runtime**.  
El mismo C3 puede ejecutarse en múltiples contextos distintos.

### C4 — Runtime (contexto de ejecución)
Contexto concreto donde se ejecuta un componente. No describe infraestructura física, pero sí atributos relevantes para seguridad (entorno, exposición, red/zona, riesgos).

Cada C4 está asociado a **un único C3**, pero un C3 puede tener **múltiples C4**.

### Invariantes del modelo
- No existe C4 sin C3.
- No existe C3 sin C2.
- No existe C2 sin C1.
- Debe existir al menos una cadena completa `C1 → C2 → C3 → C4`.

---

## Requisitos

- Python **3.10+**
- Dependencias en `requirements.txt`

---

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Ejecución

```bash
uvicorn app:app --reload
```

Abrir: http://localhost:8000

---

## Licencia

Este proyecto se publica bajo **GNU Affero General Public License v3.0 (AGPLv3)**.

© **2026** — **Bernardo Gómez Bey**
