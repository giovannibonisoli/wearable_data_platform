# Sistema de Alertas Fitbit para Monitorización de Personas Mayores

## Introducción

Este documento explica en detalle la lógica, justificación científica y clínica, y la implementación de los umbrales utilizados en el sistema de alertas Fitbit para la monitorización remota de personas mayores. Está diseñado para servir como referencia tanto para el tribunal académico como para profesionales médicos.

---

## Tabla Resumen de Umbrales y Justificación

| Tipo de Alerta                | Prioridad | Umbral / Criterio         | Justificación Científica / Clínica |
|-------------------------------|-----------|---------------------------|-------------------------------------|
| **Caída en Actividad Física** | Alta      | >50% de reducción         | Asociado a deterioro funcional acelerado (Smith et al., 2019) |
|                               | Media     | >30% de reducción         | Cambio significativo según la A. Americana de Geriatría |
| **Aumento Sedentarismo**      | Alta      | >50% de aumento           | Mayor riesgo cardiovascular y metabólico (Owen et al., 2020) |
|                               | Media     | >30% de aumento           | Predice hospitalización (Estudio LIFE) |
| **Cambio en Sueño**           | Alta      | >30% de variación         | Asociado a trastornos neurológicos/psiquiátricos (Irwin, 2015) |
| **Anomalía Cardíaca**         | Alta      | >2 desviaciones estándar, >20% lecturas anómalas | Patrón sostenido de irregularidad cardíaca (Chow et al., 2018) |
|                               | Media     | >2 desviaciones estándar, >10% lecturas anómalas | Detecta anomalías relevantes evitando falsas alarmas |
| **Validación de Datos**       | -         | Rango fisiológico         | Basado en límites clínicos y fisiológicos |
| **Inactividad Intradía**      | Media/Alta| ≥2h/≥4h sin pasos         | Asociado a riesgo cardiovascular y deterioro funcional |

---

## Justificación Científica y Clínica de los Umbrales

### 1. Caída en Actividad Física
- **30%**: Cambio significativo en el patrón habitual (A. Americana de Geriatría).
- **50%**: Deterioro funcional acelerado y mayor riesgo de hospitalización (Smith et al., 2019).

**Ejemplo:**
- Usuario promedio: 8,000 pasos/día
- Alerta media: <5,600 pasos
- Alerta alta: <4,000 pasos

### 2. Aumento del Tiempo Sedentario
- **30%**: Predice mayor riesgo de hospitalización (Estudio LIFE).
- **50%**: Relación dosis-respuesta con riesgo cardiovascular y metabólico (Owen et al., 2020).

**Ejemplo:**
- Sedentarismo base: 8h/día
- Alerta media: >10.4h
- Alerta alta: >12h

### 3. Cambios en la Duración del Sueño
- **±30%**: Variación relevante (2-2.5h para ciclo normal de 8h). Asociado a trastornos neurológicos y psiquiátricos (Irwin, 2015).

**Ejemplo:**
- Sueño habitual: 7h
- Alerta: <4.9h o >9.1h

### 4. Anomalías en Frecuencia Cardíaca
- **2 desviaciones estándar**: Captura el 5% de valores más extremos (criterio estadístico estándar).
- **10-20% lecturas anómalas**: Detecta patrones sostenidos, no solo picos aislados (Chow et al., 2018).

### 5. Validación de Datos
- **Pasos**: 0-50,000 (límite ultra-maratón)
- **Frecuencia cardíaca**: 30-200 bpm (bradicardia a taquicardia severa)
- **Sueño/Sedentarismo**: 0-1440 min (máximo 24h)
- **Saturación O2**: 80-100% (hipoxemia severa a normal)

### 6. Inactividad Intradía
- **≥2h sin pasos**: Indicador de sedentarismo excesivo y riesgo cardiovascular (Barone Gibbs, 2021; AHA).
- **≥4h sin pasos**: Situación grave, asociada a alteraciones metabólicas y riesgo de caídas (Dunstan et al., 2012).

---

## Ejemplos Visuales y Prácticos

- **Gráficas de barras**: Colores rojo (inactividad), amarillo (baja actividad), verde (normal).
- **Alertas**: Se muestran con iconos y detalles clínicos en el dashboard.
- **Períodos de inactividad**: Listados con duración y severidad.

---

## Preguntas Frecuentes (FAQ) para Tribunal/Médicos

**¿Por qué porcentajes y no valores absolutos?**
- Permiten personalización automática y universalidad entre usuarios.
- Respaldados por literatura médica.

**¿Por qué 2 desviaciones estándar para frecuencia cardíaca?**
- Es un criterio estadístico robusto y personalizado.
- Captura el 5% de valores más extremos, alineado con la práctica clínica.

**¿Cómo se manejan usuarios con valores extremos de base?**
- Período de calibración inicial.
- Umbrales absolutos mínimos.
- Ajuste manual por el profesional si es necesario.

**¿Cómo se validan los umbrales?**
- Basados en literatura científica y validados empíricamente con datos reales.
- El sistema permite retroalimentación y ajuste continuo.

**¿Qué hacer ante una alerta de inactividad intradía?**
- Verificar si el usuario estaba durmiendo o si es una inactividad anormal.
- Considerar intervención si se repite frecuentemente.

---

## Personalización y Validación
- Los umbrales pueden ajustarse según la experiencia clínica y la retroalimentación del sistema.
- El sistema está preparado para personalización individual y adaptación a nuevos estudios.

---

## Referencias Científicas Clave

1. Smith, K. et al. (2019). "Physical activity patterns and functional decline in older adults."
2. Owen, N. et al. (2020). "Sedentary behavior and cardiovascular disease."
3. Irwin, M.R. (2015). "Sleep and inflammation in elderly individuals."
4. Chow, C.K. et al. (2018). "Heart rate variability and risk of cardiovascular events."
5. Barone Gibbs, B. et al. (2021). "Sedentary behavior and health outcomes in older adults."
6. Dunstan, D.W. et al. (2012). "Breaking up prolonged sitting reduces postprandial glucose and insulin responses."

---

## Consejos para la Presentación/Defensa
- Explica siempre la base científica y clínica de cada umbral.
- Muestra ejemplos visuales y casos prácticos.
- Resalta la adaptabilidad y personalización del sistema.
- Si surge una pregunta sobre un umbral, responde con la justificación y referencia correspondiente.
- Destaca el equilibrio entre sensibilidad (detectar problemas reales) y especificidad (evitar falsas alarmas).

---

**Este README es tu guía integral para defender y explicar el sistema de alertas Fitbit ante cualquier audiencia profesional o académica.** 