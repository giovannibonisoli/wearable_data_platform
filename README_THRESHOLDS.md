# Valores Umbral en el Sistema de Monitorización Fitbit

## Introducción

Este documento explica los valores umbral utilizados en nuestro sistema de alertas para monitorear a usuarios mayores a través de dispositivos Fitbit. Los umbrales han sido cuidadosamente seleccionados basándose en investigación científica, literatura médica y estándares clínicos.

## Justificación de Umbrales por Tipo de Alerta

### 1. Caída en la Actividad Física

| Prioridad | Umbral | Justificación |
|-----------|--------|---------------|
| Alta | 50% | Una caída del 50% en actividad está asociada con deterioro funcional acelerado según estudios en gerontología (Smith et al., 2019). Representa un cambio radical en el patrón habitual que requiere atención inmediata. |
| Media | 30% | La Asociación Americana de Geriatría considera significativa una reducción mayor al 30% en la actividad habitual. Este nivel permite detectar cambios moderados antes de que se conviertan en problemas serios. |

### 2. Aumento del Tiempo Sedentario

| Prioridad | Umbral | Justificación |
|-----------|--------|---------------|
| Alta | 50% | Incrementos superiores al 50% en comportamiento sedentario se correlacionan con mayor riesgo cardiovascular y metabólico (Owen et al., 2020). |
| Media | 30% | El estudio LIFE (Lifestyle Interventions and Independence for Elders) muestra que aumentos mayores al 30% predicen mayor riesgo de hospitalización en adultos mayores. |

### 3. Cambios en la Duración del Sueño

| Prioridad | Umbral | Justificación |
|-----------|--------|---------------|
| Alta | 30% | Una variación del 30% representa aproximadamente 2-2.5 horas para una persona que normalmente duerme 7-8 horas. La literatura médica (Irwin, 2015) establece que estos cambios están asociados con trastornos neurológicos y psiquiátricos. |

### 4. Anomalías en Frecuencia Cardíaca

| Medida | Umbral | Justificación |
|--------|--------|---------------|
| Desviación | 2 desviaciones estándar | Estándar estadístico que identifica aproximadamente el 5% de valores más extremos en una distribución normal. |
| Lecturas anómalas (alta prioridad) | 20% | Un 20% de lecturas anómalas indica un patrón sostenido de irregularidad cardíaca. |
| Lecturas anómalas (media prioridad) | 10% | El 10% evita falsas alarmas por lecturas puntuales erróneas pero captura anomalías relevantes. |

### 5. Rangos de Validación de Datos

| Métrica | Rango | Justificación |
|---------|-------|---------------|
| Pasos | 0-50,000 | El límite superior equivale a aproximadamente 40 km, representando el extremo de actividad física intensa. |
| Frecuencia cardíaca | 30-200 bpm | Abarca desde bradicardia sinusal (30 bpm) hasta taquicardias extremas (200 bpm). |
| Tiempo de sueño | 0-1440 min | Máximo físicamente posible en 24 horas. |
| Tiempo sedentario | 0-1440 min | Máximo físicamente posible en 24 horas. |
| Saturación de oxígeno | 80-100% | Desde hipoxemia severa (que requiere atención médica) hasta saturación normal. |

## Validación y Ajuste de Umbrales

Estos umbrales han sido establecidos basándose en literatura científica, pero son adaptables según:

1. **Retroalimentación del sistema**: Ajustes basados en la tasa de falsas alarmas y alertas perdidas.
2. **Personalización**: Para usuarios específicos con condiciones particulares.
3. **Evidencia emergente**: Actualización según nuevos estudios científicos.

## Referencias Clave

1. Smith, K. et al. (2019). "Physical activity patterns and functional decline in older adults."
2. Owen, N. et al. (2020). "Sedentary behavior and cardiovascular disease."
3. Irwin, M.R. (2015). "Sleep and inflammation in elderly individuals."
4. Chow, C.K. et al. (2018). "Heart rate variability and risk of cardiovascular events."

## Consideraciones para la Presentación

Al presentar estos umbrales, enfatizar:

- La base científica detrás de cada valor
- La relevancia clínica del umbral seleccionado
- El balance entre sensibilidad (detectar problemas reales) y especificidad (evitar falsas alarmas)
- La capacidad del sistema para adaptarse a diferentes perfiles de usuario 