#!/bin/bash

# Configurar el directorio de logs
LOG_DIR="/home/pablo.morenomunoz/fitbit_project/logs"
LOG_FILE="$LOG_DIR/fitbit.log"
ERROR_LOG="$LOG_DIR/fitbit_error.log"

# Crear directorio de logs si no existe
mkdir -p "$LOG_DIR"

# Activar el entorno virtual
source /home/pablo.morenomunoz/fitbit_project/venv/bin/activate

# Timestamp para el log
echo "=== Inicio de ejecución: $(date) ===" >> "$LOG_FILE"

# Ejecutar el script de Python
/home/pablo.morenomunoz/fitbit_project/venv/bin/python /home/pablo.morenomunoz/fitbit_project/fitbit.py >> "$LOG_FILE" 2>> "$ERROR_LOG"

# Registrar el código de salida
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "Error en la ejecución. Código de salida: $EXIT_CODE" >> "$ERROR_LOG"
fi

echo "=== Fin de ejecución: $(date) ===" >> "$LOG_FILE"

# Desactivar el entorno virtual
deactivate
