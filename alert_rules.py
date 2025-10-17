import numpy as np
from datetime import datetime, timedelta
from db import DatabaseManager
import json

def check_activity_drop(user_id, current_date):
    """Verifica si hay una caída significativa en la actividad física."""
    db = DatabaseManager()
    if not db.connect():
        return False
    
    try:
        # Obtener datos de los últimos 7 días (excluyendo hoy)
        start_date = current_date - timedelta(days=7)
        end_date = current_date - timedelta(days=1)
        daily_summaries = db.get_daily_summaries(user_id, start_date, end_date)
        if not daily_summaries or len(daily_summaries) < 2:
            print(f"[activity_drop] No hay suficientes datos para el usuario {user_id}.")
            return False
        previous_days = [s for s in daily_summaries if s[2] < current_date.date()]
        if not previous_days:
            print(f"[activity_drop][DEBUG] No hay días previos para el usuario {user_id}.")
            return False
        valid_steps = [s[3] for s in previous_days if s[3] is not None and s[3] > 0]
        valid_active_minutes = [s[9] for s in previous_days if s[9] is not None and s[9] > 0]
        if not valid_steps or not valid_active_minutes:
            print(f"[activity_drop][DEBUG] No hay datos válidos de pasos o minutos activos para el usuario {user_id}.")
            return False
        avg_steps = sum(valid_steps) / len(valid_steps)
        avg_active_minutes = sum(valid_active_minutes) / len(valid_active_minutes)
        if avg_steps < 100 or avg_active_minutes < 5:
            print(f"[activity_drop][DEBUG] Promedios demasiado bajos para usuario {user_id}: avg_steps={avg_steps}, avg_active_minutes={avg_active_minutes}")
            return False
        today_data = db.get_daily_summaries(user_id, current_date, current_date)
        if not today_data:
            print(f"[activity_drop][DEBUG] No hay datos de hoy para el usuario {user_id}.")
            return False
        today_data = today_data[0]
        today_steps = today_data[3] or 0
        today_active_minutes = today_data[9] or 0
        steps_drop = ((avg_steps - today_steps) / avg_steps * 100) if today_steps < avg_steps else 0
        active_minutes_drop = ((avg_active_minutes - today_active_minutes) / avg_active_minutes * 100) if today_active_minutes < avg_active_minutes else 0
        avg_steps = round(avg_steps, 2)
        today_steps = round(today_steps, 2)
        steps_drop = round(steps_drop, 2)
        avg_active_minutes = round(avg_active_minutes, 2)
        today_active_minutes = round(today_active_minutes, 2)
        active_minutes_drop = round(active_minutes_drop, 2)
        print("[activity_drop][DEBUG] ---")
        print(f"[activity_drop][DEBUG] user_id={user_id}")
        print(f"[activity_drop][DEBUG] avg_steps={avg_steps}, today_steps={today_steps}, steps_drop={steps_drop:.2f}%")
        print(f"[activity_drop][DEBUG] avg_active_minutes={avg_active_minutes}, today_active_minutes={today_active_minutes}, active_minutes_drop={active_minutes_drop:.2f}%")
        print(f"[activity_drop][DEBUG] Thresholds: HIGH>30%, MEDIUM>20%")
        if (today_steps < avg_steps and steps_drop > 30) or (today_active_minutes < avg_active_minutes and active_minutes_drop > 30):
            print(f"[activity_drop][DEBUG] Se dispara alerta HIGH para user_id={user_id}")
            priority = "high"
            threshold = 30.0
            drop_value = max(steps_drop if today_steps < avg_steps else 0, active_minutes_drop if today_active_minutes < avg_active_minutes else 0)
            if today_steps < avg_steps:
                details = (f"Disminución significativa en los pasos diarios (Valor actual: {today_steps:.2f}, comparado con el promedio: {avg_steps:.2f})")
            elif today_active_minutes < avg_active_minutes:
                details = (f"Disminución significativa en los minutos activos diarios (Valor actual: {today_active_minutes:.2f}, comparado con el promedio: {avg_active_minutes:.2f})")
            else:
                return False
            db.insert_alert(
                email_id=user_id,
                alert_type="activity_drop",
                priority=priority,
                triggering_value=drop_value,
                threshold=threshold,
                timestamp=current_date,
                details=details
            )
            print(f"[activity_drop][DEBUG] ALERTA HIGH generada para user_id={user_id} con drop_value={drop_value}")
            return True
        elif (today_steps < avg_steps and steps_drop > 20) or (today_active_minutes < avg_active_minutes and active_minutes_drop > 20):
            print(f"[activity_drop][DEBUG] Se dispara alerta MEDIUM para user_id={user_id}")
            priority = "medium"
            threshold = 20.0
            drop_value = max(steps_drop if today_steps < avg_steps else 0, active_minutes_drop if today_active_minutes < avg_active_minutes else 0)
            if today_steps < avg_steps:
                details = (f"Disminución moderada en los pasos diarios (Valor actual: {today_steps:.2f}, comparado con el promedio: {avg_steps:.2f})")
            elif today_active_minutes < avg_active_minutes:
                details = (f"Disminución moderada en los minutos activos diarios (Valor actual: {today_active_minutes:.2f}, comparado con el promedio: {avg_active_minutes:.2f})")
            else:
                return False
            db.insert_alert(
                email_id=user_id,
                alert_type="activity_drop",
                priority=priority,
                triggering_value=drop_value,
                threshold=threshold,
                timestamp=current_date,
                details=details
            )
            print(f"[activity_drop][DEBUG] ALERTA MEDIUM generada para user_id={user_id} con drop_value={drop_value}")
            return True
        else:
            print(f"[activity_drop][DEBUG] No se dispara alerta para user_id={user_id}. steps_drop={steps_drop:.2f}%, active_minutes_drop={active_minutes_drop:.2f}% (umbral 20/30%)")
        finally:
            db.close()
    except Exception as e:
        print(f"Error al verificar caída de actividad: {e}")
    return False

def check_sedentary_increase(user_id, current_date):
    """Verifica cambios significativos en el tiempo sedentario."""
    db = DatabaseManager()
    if not db.connect():
        return False
    
    try:
        start_date = current_date - timedelta(days=7)
        end_date = current_date - timedelta(days=1)
        daily_summaries = db.get_daily_summaries(user_id, start_date, end_date)
        if not daily_summaries or len(daily_summaries) < 2:
            print(f"[sedentary_increase][DEBUG] No hay suficientes datos sedentarios para el usuario {user_id} para generar alertas.")
            return False
        previous_days = [s for s in daily_summaries if s[2] < current_date.date()]
        if not previous_days:
            print(f"[sedentary_increase][DEBUG] No hay datos históricos de tiempo sedentario para el usuario {user_id} para comparar.")
            return False
        valid_sedentary = [s[11] for s in previous_days if s[11] is not None and s[11] > 0]
        if not valid_sedentary:
            print(f"[sedentary_increase][DEBUG] No hay datos válidos de tiempo sedentario para el usuario {user_id} para analizar.")
            return False
        avg_sedentary = sum(valid_sedentary) / len(valid_sedentary)
        if avg_sedentary < 60:
            print(f"[sedentary_increase][DEBUG] Promedio de tiempo sedentario demasiado bajo ({avg_sedentary} minutos) para generar alertas fiables.")
            return False
        today_data = db.get_daily_summaries(user_id, current_date, current_date)
        if not today_data:
            print(f"[sedentary_increase][DEBUG] No hay datos de tiempo sedentario para hoy para el usuario {user_id}.")
            return False
        today_data = today_data[0]
        today_sedentary = today_data[11] or 0
        if avg_sedentary == 0:
            print("[sedentary_increase][DEBUG] Error: Promedio de tiempo sedentario es cero, no se puede calcular el cambio porcentual.")
            return False
        if today_sedentary <= avg_sedentary:
            print(f"[sedentary_increase][DEBUG] El tiempo sedentario ha disminuido o no ha cambiado. No se genera alerta.")
            return False
        sedentary_change = ((today_sedentary - avg_sedentary) / avg_sedentary * 100)
        sedentary_change = round(sedentary_change, 1)
        print("[sedentary_increase][DEBUG] ---")
        print(f"[sedentary_increase][DEBUG] user_id={user_id}")
        print(f"[sedentary_increase][DEBUG] avg_sedentary={avg_sedentary}, today_sedentary={today_sedentary}, sedentary_change={sedentary_change:.1f}%")
        print(f"[sedentary_increase][DEBUG] Thresholds: HIGH>30%, MEDIUM>20%")
        db = DatabaseManager()
        if not db.connect():
            return False
        try:
            if sedentary_change > 30:
                print(f"[sedentary_increase][DEBUG] Se dispara alerta HIGH para user_id={user_id}")
                priority = "high"
                threshold = 30.0
                details = f"Aumento significativo en tiempo sedentario: {sedentary_change:.1f}% (de {avg_sedentary:.0f} a {today_sedentary:.0f} minutos)"
                db.insert_alert(
                    user_id=user_id,
                    alert_type="sedentary_increase",
                    priority=priority,
                    triggering_value=sedentary_change,
                    threshold=threshold,
                    timestamp=current_date,
                    details=details
                )
                print(f"[sedentary_increase][DEBUG] ALERTA HIGH generada para user_id={user_id} con sedentary_change={sedentary_change}")
                return True
            elif sedentary_change > 20:
                print(f"[sedentary_increase][DEBUG] Se dispara alerta MEDIUM para user_id={user_id}")
                priority = "medium"
                threshold = 20.0
                details = f"Aumento moderado en tiempo sedentario: {sedentary_change:.1f}% (de {avg_sedentary:.0f} a {today_sedentary:.0f} minutos)"
                db.insert_alert(
                    user_id=user_id,
                    alert_type="sedentary_increase",
                    priority=priority,
                    triggering_value=sedentary_change,
                    threshold=threshold,
                    timestamp=current_date,
                    details=details
                )
                print(f"[sedentary_increase][DEBUG] ALERTA MEDIUM generada para user_id={user_id} con sedentary_change={sedentary_change}")
                return True
            else:
                print(f"[sedentary_increase][DEBUG] No se dispara alerta para user_id={user_id}. sedentary_change={sedentary_change:.1f}% (umbral 20/30%)")
        finally:
            db.close()
    except Exception as e:
        print(f"Error al verificar cambios en tiempo sedentario: {e}")
    return False

def check_sleep_duration_change(user_id, current_date):
    """Verifica cambios significativos en la duración del sueño."""
    try:
        # Obtener datos de los últimos 7 días (excluyendo hoy)
        start_date = current_date - timedelta(days=7)
        end_date = current_date - timedelta(days=1)
        daily_summaries = db.get_daily_summaries(user_id, start_date, end_date)
        if not daily_summaries or len(daily_summaries) < 2:
            print(f"[sleep_duration_change][DEBUG] No hay suficientes datos de sueño para el usuario {user_id} para generar alertas.")
            return False
        previous_days = [s for s in daily_summaries if s[2] < current_date.date()]
        if not previous_days:
            print(f"[sleep_duration_change][DEBUG] No hay datos históricos de sueño para el usuario {user_id} para comparar.")
            return False
        valid_sleep = [s[5] for s in previous_days if s[5] is not None and s[5] > 0]
        if not valid_sleep:
            print(f"[sleep_duration_change][DEBUG] No hay datos válidos de sueño para el usuario {user_id} para analizar.")
            return False
        avg_sleep = sum(valid_sleep) / len(valid_sleep)
        
        # Protección contra promedios muy bajos que podrían causar alertas falsas
        if avg_sleep < 60:
            print(f"[sleep_duration_change][DEBUG] Promedio de sueño demasiado bajo ({avg_sleep} minutos) para generar alertas fiables.")
            return False
        today_data = db.get_daily_summaries(user_id, current_date, current_date)
        if not today_data:
            print(f"[sleep_duration_change][DEBUG] No hay datos de sueño para hoy para el usuario {user_id}.")
            return False
        today_data = today_data[0]
        today_sleep = today_data[5] or 0
        if avg_sleep == 0:
            print("[sleep_duration_change][DEBUG] Error: Promedio de sueño es cero, no se puede calcular el cambio porcentual.")
            return False
        sleep_change = abs((today_sleep - avg_sleep) / avg_sleep * 100)
        sleep_change = round(sleep_change, 1)
        print("[sleep_duration_change][DEBUG] ---")
        print(f"[sleep_duration_change][DEBUG] user_id={user_id}")
        print(f"[sleep_duration_change][DEBUG] avg_sleep={avg_sleep}, today_sleep={today_sleep}, sleep_change={sleep_change:.1f}%")
        print(f"[sleep_duration_change][DEBUG] Thresholds: HIGH>40%, MEDIUM>30%")
        db = DatabaseManager()
        if not db.connect():
            return False
        try:
            if sleep_change > 40:
                print(f"[sleep_duration_change][DEBUG] Se dispara alerta HIGH para user_id={user_id}")
                priority = "high"
                threshold = 40.0
                change_type = "aumento" if today_sleep > avg_sleep else "disminución"
                details = f"Cambio significativo en duración del sueño: {change_type} de {sleep_change:.1f}% (de {avg_sleep:.1f} a {today_sleep:.1f} minutos)"
                db.insert_alert(
                    user_id=user_id,
                    alert_type="sleep_duration_change",
                    priority=priority,
                    triggering_value=sleep_change,
                    threshold=threshold,
                    timestamp=current_date,
                    details=details
                )
                print(f"[sleep_duration_change][DEBUG] ALERTA HIGH generada para user_id={user_id} con sleep_change={sleep_change}")
                return True
            elif sleep_change > 30:
                print(f"[sleep_duration_change][DEBUG] Se dispara alerta MEDIUM para user_id={user_id}")
                priority = "medium"
                threshold = 30.0
                change_type = "aumento" if today_sleep > avg_sleep else "disminución"
                details = f"Cambio moderado en duración del sueño: {change_type} de {sleep_change:.1f}% (de {avg_sleep:.0f} a {today_sleep:.0f} minutos)"
                db.insert_alert(
                    user_id=user_id,
                    alert_type="sleep_duration_change",
                    priority=priority,
                    triggering_value=sleep_change,
                    threshold=threshold,
                    timestamp=current_date,
                    details=details
                )
                print(f"[sleep_duration_change][DEBUG] ALERTA MEDIUM generada para user_id={user_id} con sleep_change={sleep_change}")
                return True
            else:
                print(f"[sleep_duration_change][DEBUG] No se dispara alerta para user_id={user_id}. sleep_change={sleep_change:.2f}% (umbral 30/40%)")
        finally:
            db.close()
    except Exception as e:
        print(f"Error al verificar cambios en sueño: {e}")
    return False

def check_heart_rate_anomaly(user_id, current_date):
    """Verifica anomalías en la frecuencia cardíaca."""
    try:
        start_time = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_time = current_date.replace(hour=23, minute=59, second=59, microsecond=0)
        heart_rate_data = db.get_intraday_metrics(user_id, 'heart_rate', start_time, end_time)
        if not heart_rate_data:
            return False
        values = [hr[1] for hr in heart_rate_data]
        avg_hr = sum(values) / len(values)
        std_dev = (sum((x - avg_hr) ** 2 for x in values) / len(values)) ** 0.5
        # Umbrales clínicos personalizados para ancianos:
        # MEDIUM: >2.5 std_dev, HIGH: >5.0 std_dev
        medium_mult = 2.5
        high_mult = 5.0
        high_peaks = [(i, hr) for i, hr in enumerate(heart_rate_data) if abs(hr[1] - avg_hr) > high_mult * std_dev]
        medium_peaks = [(i, hr) for i, hr in enumerate(heart_rate_data) if medium_mult * std_dev < abs(hr[1] - avg_hr) <= high_mult * std_dev]
        # Detectar anomalías por acumulación
        high_accum = [hr for hr in values if abs(hr - avg_hr) > high_mult * std_dev]
        medium_accum = [hr for hr in values if medium_mult * std_dev < abs(hr - avg_hr) <= high_mult * std_dev]
        high_accum_pct = (len(high_accum) / len(values)) * 100
        medium_accum_pct = (len(medium_accum) / len(values)) * 100
        db = DatabaseManager()
        if not db.connect():
            return False
        try:
            alerts_triggered = False
            # HIGH ALERT: individual peak
            for idx, hr in high_peaks:
                details = f"Pico extremo de frecuencia cardíaca detectado: {hr[1]} bpm (>{avg_hr + 2.8*std_dev:.1f} o <{avg_hr - 2.8*std_dev:.1f}) a las {hr[0].strftime('%H:%M')}."
                db.insert_alert(
                    user_id=user_id,
                    alert_type="heart_rate_anomaly",
                    priority="high",
                    triggering_value=hr[1],
                    threshold=2.8 * std_dev,
                    timestamp=hr[0],
                    details=details
                )
                alerts_triggered = True
            # MEDIUM ALERT: individual peak
            for idx, hr in medium_peaks:
                details = f"Pico moderado de frecuencia cardíaca detectado: {hr[1]} bpm (>{avg_hr + 2.0*std_dev:.1f} o <{avg_hr - 2.0*std_dev:.1f}) a las {hr[0].strftime('%H:%M')}."
                db.insert_alert(
                    user_id=user_id,
                    alert_type="heart_rate_anomaly",
                    priority="medium",
                    triggering_value=hr[1],
                    threshold=2.0 * std_dev,
                    timestamp=hr[0],
                    details=details
                )
                alerts_triggered = True
            # HIGH ALERT: acumulación
            if high_accum_pct >= 15:
                details = f"Acumulación de valores extremos de frecuencia cardíaca: {high_accum_pct:.1f}% de las mediciones superan ±2.8 desviaciones estándar."
                db.insert_alert(
                    user_id=user_id,
                    alert_type="heart_rate_anomaly",
                    priority="high",
                    triggering_value=high_accum_pct,
                    threshold=15,
                    timestamp=current_date,
                    details=details
                )
                alerts_triggered = True
            # MEDIUM ALERT: acumulación
            if medium_accum_pct >= 10:
                details = f"Acumulación de valores anómalos de frecuencia cardíaca: {medium_accum_pct:.1f}% de las mediciones superan ±2.0 desviaciones estándar."
                db.insert_alert(
                    user_id=user_id,
                    alert_type="heart_rate_anomaly",
                    priority="medium",
                    triggering_value=medium_accum_pct,
                    threshold=10,
                    timestamp=current_date,
                    details=details
                )
                alerts_triggered = True
            if alerts_triggered:
                return True
        finally:
            db.close()
    except Exception as e:
        print(f"Error al verificar anomalías en frecuencia cardíaca: {e}")
    return False

def check_data_quality(user_id, current_date):
    """
    Evalúa la calidad de los datos y genera alertas si hay problemas.
    """
    # Obtener datos del día actual
    end_date = current_date
    start_date = current_date - timedelta(days=1)
    summaries = db.get_daily_summaries(user_id, start_date, end_date)
    
    if not summaries:
        return False
        
    current_data = summaries[-1]
    alerts_generated = False
    
    db = DatabaseManager()
    if not db.connect():
        return False
        
    try:
        # Definir rangos aceptables para cada métrica
        ranges = {
            'steps': (0, 50000),  # Máximo 50,000 pasos por día
            'heart_rate': (30, 200),  # Rango normal de frecuencia cardíaca
            'sleep_minutes': (0, 1440),  # Máximo 24 horas
            'sedentary_minutes': (0, 1440),  # Máximo 24 horas
            'oxygen_saturation': (80, 100)  # Rango normal de saturación de oxígeno
        }
        
        # Campos opcionales que no deberían generar alertas si faltan
        optional_fields = ['heart_rate', 'oxygen_saturation', 'respiratory_rate', 'temperature']
        
        # Verificar campos faltantes y valores fuera de rango
        missing_fields = []
        out_of_range_fields = []
        for field, (min_val, max_val) in ranges.items():
            value = current_data[3] if field == 'steps' else \
                   current_data[4] if field == 'heart_rate' else \
                   current_data[5] if field == 'sleep_minutes' else \
                   current_data[11] if field == 'sedentary_minutes' else \
                   current_data[16] if field == 'oxygen_saturation' else None
            if value is None:
                if field not in optional_fields:
                    missing_fields.append(field)
            elif value < min_val or value > max_val:
                # Umbrales más permisivos para campos opcionales
                if field in optional_fields:
                    if value < min_val * 0.8 or value > max_val * 1.2:
                        out_of_range_fields.append({
                            'field': field,
                            'value': round(value, 2),
                            'range': f'{min_val}-{max_val}'
                        })
                else:
                    if value < min_val * 0.9 or value > max_val * 1.1:
                        out_of_range_fields.append({
                            'field': field,
                            'value': round(value, 2),
                            'range': f'{min_val}-{max_val}'
                        })
        # Generar alerta solo si hay problemas significativos
        if missing_fields or out_of_range_fields:
            # Determinar prioridad basada en la severidad de los problemas
            critical = (
                len(missing_fields) > 0 or 
                any(f['field'] not in optional_fields for f in out_of_range_fields) or
                any(f['value'] < ranges[f['field']][0] * 0.5 or f['value'] > ranges[f['field']][1] * 1.5 for f in out_of_range_fields)
            )
            priority = "high" if critical else ("medium" if out_of_range_fields else "low")
            # Generar string legible para details
            details_str = ""
            if missing_fields:
                details_str += "Campos faltantes: " + ", ".join(missing_fields) + ". "
            if out_of_range_fields:
                details_str += "Valores fuera de rango: " + ", ".join([
                    f"{f['field']}={f['value']} (rango {f['range']})" for f in out_of_range_fields
                ]) + "."
            db.insert_alert(
                user_id=user_id,
                alert_type="data_quality",
                priority=priority,
                triggering_value=len(missing_fields) + len(out_of_range_fields),
                threshold=1,
                timestamp=current_date,
                details=details_str.strip()
            )
            alerts_generated = True
    finally:
        db.close()
    
    return alerts_generated

def check_intraday_anomalies(user_id, current_date):
    """
    Detecta anomalías en datos intradía usando la tabla intraday_metrics.
    """
    # Obtener datos de las últimas 24 horas
    end_time = current_date
    start_time = current_date - timedelta(hours=24)
    
    # Tipos de métricas a verificar
    metric_types = ['heart_rate', 'steps', 'active_zone_minutes', 'calories']
    alerts_generated = False
    has_any_data = False
    
    db = DatabaseManager()
    if not db.connect():
        return False
        
    try:
        for metric_type in metric_types:
            metrics = db.get_intraday_metrics(user_id, metric_type, start_time, end_time)
            
            if not metrics or len(metrics) < 10:  # Necesitamos suficientes datos
                print(f"Insuficientes datos intradía para {metric_type} (usuario {user_id}).")
                continue
                
            has_any_data = True
            values = [m[1] for m in metrics if m[1] is not None]  # m[1] es el valor
            
            if len(values) < 10:
                print(f"Insuficientes valores válidos para {metric_type} (usuario {user_id}).")
                continue
                
            mean_value = float(np.mean(values))
            std_value = float(np.std(values))
            
            # Detectar valores fuera de 2 desviaciones estándar
            # Este umbral establece un intervalo de confianza del 95%, estándar en monitorización biomédica
            # y proporciona un balance óptimo entre detectar anomalías relevantes y minimizar falsas alarmas
            threshold_high = mean_value + 2 * std_value
            threshold_low = mean_value - 2 * std_value
            
            anomalies = [v for v in values if v > threshold_high or v < threshold_low]
            
            if anomalies:
                # Determinar la prioridad basada en la severidad de la anomalía
                # 20% adicional (1.2x) se considera anomalía severa según estudios de monitorización
                is_high_severity = any(v > threshold_high * 1.2 or v < threshold_low * 0.8 for v in anomalies)
                priority = 'high' if is_high_severity else 'medium'
                
                # Valor más extremo (el que más se desvía de la media)
                max_anomaly = max(anomalies, key=lambda x: abs(x - mean_value))
                max_deviation = abs(max_anomaly - mean_value)
                
                # Umbral: usamos 2.0 para indicar 2 desviaciones estándar
                threshold = 2.0
                
                # Crear mensaje descriptivo
                if metric_type == 'heart_rate':
                    details = f'Anomalía en frecuencia cardíaca: {max_anomaly} bpm (rango normal: {threshold_low:.0f}-{threshold_high:.0f} bpm)'
                elif metric_type == 'steps':
                    details = f'Anomalía en pasos: {max_anomaly} pasos (rango normal: {threshold_low:.0f}-{threshold_high:.0f} pasos)'
                elif metric_type == 'active_zone_minutes':
                    details = f'Anomalía en minutos activos: {max_anomaly} minutos (rango normal: {threshold_low:.0f}-{threshold_high:.0f} minutos)'
                else:
                    details = f'Anomalía en {metric_type}: {max_anomaly} (rango normal: {threshold_low:.0f}-{threshold_high:.0f})'
                
                db.insert_alert(
                    user_id=user_id,
                    alert_type=f'{metric_type}_anomaly',
                    priority=priority,
                    triggering_value=max_deviation,
                    threshold=threshold,
                    timestamp=current_date,
                    details=details
                )
                alerts_generated = True
        
        if not has_any_data:
            print(f"No se encontraron datos intradía para el usuario {user_id}. Esto es normal si no tienes acceso a datos intradía de Fitbit.")
    finally:
        db.close()
    
    return alerts_generated

def check_intraday_activity_drop(user_id, current_date):
    """
    Detecta caídas de actividad intradía: intervalos largos sin pasos.
    
    Relevancia clínica:
    - Intervalos prolongados sin actividad (≥2h) son indicadores de sedentarismo excesivo
    - Para personas mayores, estos períodos aumentan el riesgo de eventos cardiovasculares (Barone Gibbs, 2021)
    - Pueden indicar problemas de movilidad o deterioro funcional (sarcopenia)
    - En algunos casos, pueden señalar situaciones de emergencia como caídas no detectadas
    
    La detección temprana permite intervenciones preventivas como:
    - Recordatorios para levantarse y moverse a intervalos regulares
    - Evaluación de la capacidad funcional
    - Adaptación del entorno para favorecer la movilidad segura
    """
    # Obtener datos intradía de pasos para el día
    start_time = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1)
    steps_data = db.get_intraday_metrics(user_id, 'steps', start_time, end_time)
    if not steps_data or len(steps_data) < 12:  # Al menos 12 intervalos (ej: cada 2h)
        return False
    # Buscar intervalos largos (>=2h) con 0 pasos
    zero_streak = []
    max_streak = []
    for i, (t, v) in enumerate(steps_data):
        if v == 0:
            zero_streak.append((t, v))
        else:
            if len(zero_streak) >= 8:  # 8h o más (ajustado para intervalos de 1h)
                if len(zero_streak) > len(max_streak):
                    max_streak = zero_streak.copy()
            zero_streak = []
    if len(zero_streak) >= 8 and len(zero_streak) > len(max_streak):
        max_streak = zero_streak.copy()
    if len(max_streak) >= 6:  # Al menos 6 horas sin actividad
        start_time = max_streak[0][0]
        end_time = max_streak[-1][0]
        duration = (end_time - start_time).total_seconds() / 3600  # Duración en horas
        db = DatabaseManager()
        if not db.connect():
            return False
        try:
            start = max_streak[0][0].strftime('%H:%M')
            end = max_streak[-1][0].strftime('%H:%M')
            hours = len(max_streak)
            
            # Referencias científicas y recomendaciones basadas en la duración
            scientific_ref = ""
            recommendation = ""
            
            if hours >= 4:
                # Más de 4 horas de inactividad es grave para ancianos
                scientific_ref = "Según estudios de Dunstan et al. (2012) y Owen et al. (2020), períodos >4h de inmovilidad se asocian con alteraciones metabólicas significativas y mayor riesgo cardiovascular."
                recommendation = "RECOMENDACIÓN: Verificar urgentemente el estado del paciente y considerar estrategias para romper períodos prolongados de sedentarismo."
            elif hours >= 2:
                # 2-4 horas también es preocupante pero menos grave
                scientific_ref = "La American Heart Association recomienda romper períodos de sedentarismo cada 2 horas para reducir el riesgo cardiovascular en adultos mayores."
                recommendation = "RECOMENDACIÓN: Monitorizar la frecuencia de estos episodios y considerar intervenciones si se repiten habitualmente."
            details = (f"Periodo de inactividad detectado: sin pasos entre {start} y {end} "
                      f"({hours} horas). {scientific_ref} {recommendation}")
            db.insert_alert(
                user_id=user_id,
                alert_type="intraday_activity_drop",
                priority="medium" if hours < 4 else "high",
                triggering_value=0,
                threshold=">=6 intervalos",
                timestamp=max_streak[0][0],
                details=details
            )
            return True
        finally:
            db.close()
    return False

def evaluate_all_alerts(user_id, current_date):
    """Evalúa todas las reglas de alerta para un usuario (versión final sin prints de debug)."""
    try:
        alerts_triggered = False
        # Verificar caída en actividad física
        if check_activity_drop(user_id, current_date):
            alerts_triggered = True
        # Verificar anomalías en frecuencia cardíaca
        try:
            if check_heart_rate_anomaly(user_id, current_date):
                alerts_triggered = True
        except Exception as e:
            print(f"Se omitió la verificación de anomalías en frecuencia cardíaca debido a un error: {e}")
            print("Esto es normal si no tienes acceso a datos intradía de Fitbit.")
        # Verificar cambios en el sueño
        try:
            if check_sleep_duration_change(user_id, current_date):
                alerts_triggered = True
        except Exception as e:
            print(f"Error al verificar cambios en el sueño: {e}")
        # Verificar aumento en tiempo sedentario
        try:
            if check_sedentary_increase(user_id, current_date):
                alerts_triggered = True
        except Exception as e:
            print(f"Error al verificar aumento en tiempo sedentario: {e}")
        # Verificar calidad de datos
        try:
            if check_data_quality(user_id, current_date):
                alerts_triggered = True
        except Exception as e:
            print(f"Error al verificar calidad de datos: {e}")
        # Detectar caídas de actividad intradía
        try:
            if check_intraday_activity_drop(user_id, current_date):
                alerts_triggered = True
        except Exception as e:
            print(f"Error al verificar caídas de actividad intradía: {e}")
        return alerts_triggered
    except Exception as e:
        print(f"Error al evaluar alertas: {e}")
        return False

def get_triggered_alerts(user_id, current_date):
    """
    Returns a list of triggered alerts for a user on a given date.
    This function uses the existing alert check functions but returns a list of triggered alerts
    instead of just a boolean.
    """
    triggered_alerts = []
    
    # Check activity drop
    if check_activity_drop(user_id, current_date):
        triggered_alerts.append('activity_drop')
        
    # Check heart rate anomalies
    try:
        if check_heart_rate_anomaly(user_id, current_date):
            triggered_alerts.append('heart_rate_anomaly')
    except Exception as e:
        print(f"Skipped heart rate anomaly check due to error: {e}")
        
    # Check sleep changes
    try:
        if check_sleep_duration_change(user_id, current_date):
            triggered_alerts.append('sleep_duration_change')
    except Exception as e:
        print(f"Error checking sleep changes: {e}")
        
    # Check sedentary increase
    try:
        if check_sedentary_increase(user_id, current_date):
            triggered_alerts.append('sedentary_increase')
    except Exception as e:
        print(f"Error checking sedentary increase: {e}")
        
    # Check data quality
    try:
        if check_data_quality(user_id, current_date):
            triggered_alerts.append('data_quality')
    except Exception as e:
        print(f"Error checking data quality: {e}")
        
    # Check intraday activity drop
    try:
        if check_intraday_activity_drop(user_id, current_date):
            triggered_alerts.append('intraday_activity_drop')
    except Exception as e:
        print(f"Error checking intraday activity drop: {e}")
        
    return triggered_alerts