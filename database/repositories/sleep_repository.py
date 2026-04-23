from typing import Optional, List, Dict, Any
from datetime import datetime
from database.connection import SqlalchemyConnection
from database.orm_models import (
    SleepSessionModel,
    SleepLogModel,
    SleepLevelModel,
    SleepShortLevelModel,
    SpO2IntradayModel,
    HRVIntradayModel,
    BreathingRateIntradayModel,
)


class SleepRepository:
    def __init__(self, connection_manager: SqlalchemyConnection):
        self.db = connection_manager

    def create_session(self, device_id: int) -> Optional[int]:
        session = SleepSessionModel(device_id=device_id)
        self.db.session.add(session)
        self.db.session.flush()
        self.db.session.commit()
        print(f"Sleep session {session.id} inserted for device {device_id}")
        return session.id

    def get_sleep_logs(
        self,
        device_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> List[SleepLogModel]:
        query = self.db.session.query(SleepLogModel).join(
            SleepSessionModel, SleepLogModel.sleep_session_id == SleepSessionModel.id
        ).filter(SleepSessionModel.device_id == device_id)

        if start_date:
            query = query.filter(SleepLogModel.start_time >= start_date)
        if end_date:
            query = query.filter(SleepLogModel.start_time <= end_date)

        return query.order_by(SleepLogModel.start_time.desc()).all()

    def insert_sleep_log(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        start_time = data['startTime']
        existing = self.db.session.query(SleepLogModel).filter(
            SleepLogModel.sleep_session_id == sleep_session_id,
            SleepLogModel.start_time == start_time
        ).first()

        if existing:
            existing.end_time = data['endTime']
            existing.is_main_sleep = data['isMainSleep']
            existing.duration = data['duration'] / 1000
            existing.minutes_asleep = data['minutesAsleep']
            existing.minutes_awake = data['minutesAwake']
            existing.minutes_in_the_bed = data['timeInBed']
            existing.log_type = data['logType']
            existing.type = data['type']
        else:
            log = SleepLogModel(
                sleep_session_id=sleep_session_id,
                start_time=start_time,
                end_time=data['endTime'],
                is_main_sleep=data['isMainSleep'],
                duration=data['duration'] / 1000,
                minutes_asleep=data['minutesAsleep'],
                minutes_awake=data['minutesAwake'],
                minutes_in_the_bed=data['timeInBed'],
                log_type=data['logType'],
                type=data['type'],
            )
            self.db.session.add(log)

        self.db.session.commit()
        print(f"Sleep log inserted/updated for sleep session {sleep_session_id}")
        return True

    def get_sleep_levels(self, sleep_session_id: int) -> List[SleepLevelModel]:
        return (
            self.db.session.query(SleepLevelModel)
            .filter(SleepLevelModel.sleep_session_id == sleep_session_id)
            .order_by(SleepLevelModel.time.asc())
            .all()
        )

    def insert_sleep_level(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        time = data['dateTime']
        existing = self.db.session.query(SleepLevelModel).filter(
            SleepLevelModel.sleep_session_id == sleep_session_id,
            SleepLevelModel.time == time
        ).first()

        if existing:
            existing.level = data['level']
            existing.seconds = data['seconds']
        else:
            level = SleepLevelModel(
                sleep_session_id=sleep_session_id,
                time=time,
                level=data['level'],
                seconds=data['seconds'],
            )
            self.db.session.add(level)

        self.db.session.commit()
        print(f"Sleep level record upserted for sleep session {sleep_session_id}")
        return True

    def insert_sleep_short_level(self, sleep_session_id: int, short: Dict[str, Any]) -> bool:
        time = short['dateTime']
        existing = self.db.session.query(SleepShortLevelModel).filter(
            SleepShortLevelModel.sleep_session_id == sleep_session_id,
            SleepShortLevelModel.time == time
        ).first()

        if existing:
            existing.seconds = short['seconds']
        else:
            short_level = SleepShortLevelModel(
                sleep_session_id=sleep_session_id,
                time=time,
                seconds=short['seconds'],
            )
            self.db.session.add(short_level)

        self.db.session.commit()
        print(f"Sleep short level record upserted for sleep session {sleep_session_id}")
        return True

    def insert_complete_sleep_data(self, device_id: int, sleep_data: Dict[str, Any]) -> Optional[int]:
        session_id = self.create_session(device_id)
        if not session_id:
            return None

        if not self.insert_sleep_log(session_id, sleep_data):
            return None

        if 'levels' in sleep_data and 'data' in sleep_data['levels']:
            for level_data in sleep_data['levels']['data']:
                self.insert_sleep_level(session_id, level_data)

        if 'levels' in sleep_data and 'shortData' in sleep_data['levels']:
            for short_data in sleep_data['levels']['shortData']:
                self.insert_sleep_short_level(session_id, short_data)

        return session_id

    def insert_spo2(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        spo2 = SpO2IntradayModel(
            sleep_session_id=sleep_session_id,
            timestamp=datetime.fromisoformat(data['dateTime']),
            spo2_value=float(data['value'])
        )
        self.db.session.add(spo2)
        self.db.session.commit()
        return True

    def insert_hrv(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        value = data.get('value', {})
        hrv = HRVIntradayModel(
            sleep_session_id=sleep_session_id,
            timestamp=datetime.fromisoformat(data['dateTime']),
            rmssd=float(value.get('rmssd', 0)),
            lf=float(value.get('lf')) if value.get('lf') else None,
            hf=float(value.get('hf')) if value.get('hf') else None,
            coverage=float(value.get('coverage')) if value.get('coverage') else None,
        )
        self.db.session.add(hrv)
        self.db.session.commit()
        return True

    def insert_breathing_rate(self, sleep_session_id: int, data: Dict[str, Any]) -> bool:
        br = BreathingRateIntradayModel(
            sleep_session_id=sleep_session_id,
            timestamp=datetime.fromisoformat(data['dateTime']),
            breathing_rate=float(data['value'])
        )
        self.db.session.add(br)
        self.db.session.commit()
        return True