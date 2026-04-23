from typing import Optional, List, Tuple
from datetime import datetime, date
from database.connection import SqlalchemyConnection
from database.orm_models import DailySummaryModel, IntradayMetricModel

_INTRADAY_METRIC_COLUMNS = frozenset({"heart_rate", "steps", "calories", "distance"})


class MetricsRepository:
    def __init__(self, connection_manager: SqlalchemyConnection):
        self.db = connection_manager

    def get_daily_summaries(
        self,
        device_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[DailySummaryModel]:
        query = self.db.session.query(DailySummaryModel).filter(
            DailySummaryModel.device_id == device_id
        )
        if start_date:
            query = query.filter(DailySummaryModel.date >= start_date)
        if end_date:
            query = query.filter(DailySummaryModel.date <= end_date)
        return query.order_by(DailySummaryModel.date.asc()).all()

    def insert_daily_summary(
        self,
        device_id: int,
        date_value: date,
        **data
    ) -> bool:
        summary = self.db.session.query(DailySummaryModel).filter(
            DailySummaryModel.device_id == device_id,
            DailySummaryModel.date == date_value
        ).first()

        if summary:
            for key, value in data.items():
                if hasattr(summary, key):
                    setattr(summary, key, value)
        else:
            summary = DailySummaryModel(device_id=device_id, date=date_value, **data)
            self.db.session.add(summary)

        self.db.session.commit()
        return True

    def get_device_history(self, device_id: int) -> List[DailySummaryModel]:
        return self.get_daily_summaries(device_id)

    def get_intraday_metrics(
        self,
        device_id: int,
        metric_type: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Tuple[datetime, any]]:
        if metric_type not in _INTRADAY_METRIC_COLUMNS:
            return []

        query = self.db.session.query(IntradayMetricModel).filter(
            IntradayMetricModel.device_id == device_id
        )
        if start_time:
            query = query.filter(IntradayMetricModel.time >= start_time)
        if end_time:
            query = query.filter(IntradayMetricModel.time <= end_time)

        results = query.order_by(IntradayMetricModel.time.asc()).all()
        return [(r.time, getattr(r, metric_type)) for r in results if getattr(r, metric_type) is not None]

    def check_intraday_timestamp_exists(self, device_id: int, timestamp: datetime) -> bool:
        return self.db.session.query(IntradayMetricModel).filter(
            IntradayMetricModel.device_id == device_id,
            IntradayMetricModel.time == timestamp
        ).first() is not None

    def insert_intraday_metric(
        self,
        device_id: int,
        timestamp: datetime,
        data_type: str = "heart_rate",
        value: Optional[float] = None,
    ) -> bool:
        if data_type not in _INTRADAY_METRIC_COLUMNS:
            return False

        metric = self.db.session.query(IntradayMetricModel).filter(
            IntradayMetricModel.device_id == device_id,
            IntradayMetricModel.time == timestamp
        ).first()

        if metric:
            setattr(metric, data_type, value)
        else:
            metric = IntradayMetricModel(device_id=device_id, time=timestamp, **{data_type: value})
            self.db.session.add(metric)

        self.db.session.commit()
        print(f"Intraday {data_type} data for device {device_id} successfully {'updated' if metric.id else 'inserted'}.")
        return True

    def get_intraday_timestamps_by_range(
        self,
        device_id: int,
        start_date: datetime,
        end_date: datetime
    ) -> List[datetime]:
        results = self.db.session.query(IntradayMetricModel.time).filter(
            IntradayMetricModel.device_id == device_id,
            IntradayMetricModel.time > start_date,
            IntradayMetricModel.time < end_date
        ).order_by(IntradayMetricModel.time.asc()).all()
        return [r[0] for r in results]