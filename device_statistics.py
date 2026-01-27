from db import DatabaseManager
from datetime import datetime, timedelta, timezone, time


# DEVICE USAGE STATISTICS
def get_device_sync_data(device_id):
    db = DatabaseManager()
    if db.connect():
        try:

            data_reception_status = None
            data_reception_details = {}


            last_sync = db.get_last_synch(device_id)
            now = datetime.now()

            last_sync = last_sync.replace(tzinfo=now.tzinfo)
            data_reception_details['sync_days'] = (now - last_sync).days
            data_reception_details['sync_hours'] = (now - last_sync).seconds // 3600
            data_reception_details['sync_minutes'] = (now - last_sync).seconds // 60


            intraday_checkpoint = db.get_intraday_checkpoint(device_id)
                            
            if intraday_checkpoint:
                intraday_checkpoint = intraday_checkpoint.replace(tzinfo=last_sync.tzinfo)
                data_reception_details['gap_days'] = max((last_sync - intraday_checkpoint).days, 0)
                
            else:
                data_reception_details['gap_days'] = 0
                            
                # Determine overall status
            if data_reception_details['sync_days'] > 7:
                data_reception_status = 'sync_warning'
            else:
                if data_reception_details['gap_days'] > 3:
                    data_reception_status = 'gap_warning'

                else:
                    data_reception_status = 'ok'

            return data_reception_status, data_reception_details

        except Exception as e:

            error_msg = f"Error while computing data reception details: {e}"
        
        
        raise Exception(error_msg)
    else:
        raise Exception("Error connecting to the db")

    

def compute_device_usage_statistics(device_id):
    pass

    

    # Calcolare il non uso medio

    # Calcolare il ritardo medio di sincronizzazione e gli intervalli medi di dati persi 
    # a causa della mancata sicronizzazione

    # Calcolare l'utilizzo medio consecutivo del device sia in termini di minuti, ore, giorni, settimane e mesi

    # Fare questi calcoli coprendo varie intervalli di tempo (ore, giorni, settimane e mesi)

    


if __name__ == "__main__":
    try:
        print(get_device_sync_data(1))

    except Exception as e:
        print(e)