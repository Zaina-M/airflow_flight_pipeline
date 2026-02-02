
import logging
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class LineageEventType(Enum):
    # Types of lineage events.
    READ = "read"
    WRITE = "write"
    TRANSFORM = "transform"
    VALIDATE = "validate"
    AGGREGATE = "aggregate"


@dataclass
class DatasetInfo:
    # Information about a dataset (source or target).
    name: str
    namespace: str  # e.g., 'mysql.staging', 'postgres.analytics'
    schema_version: Optional[str] = None
    row_count: Optional[int] = None
    columns: Optional[List[str]] = None
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'namespace': self.namespace,
            'schema_version': self.schema_version,
            'row_count': self.row_count,
            'columns': self.columns
        }


@dataclass
class TransformationInfo:
    # Information about a transformation applied.
    name: str
    description: str
    input_columns: List[str]
    output_columns: List[str]
    logic: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'name': self.name,
            'description': self.description,
            'input_columns': self.input_columns,
            'output_columns': self.output_columns,
            'logic': self.logic
        }


@dataclass
class LineageEvent:
    # A single lineage event in the pipeline.
    event_type: LineageEventType
    timestamp: datetime
    task_id: str
    run_id: str
    source_datasets: List[DatasetInfo]
    target_datasets: List[DatasetInfo]
    transformations: List[TransformationInfo] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            'event_type': self.event_type.value,
            'timestamp': self.timestamp.isoformat(),
            'task_id': self.task_id,
            'run_id': self.run_id,
            'source_datasets': [s.to_dict() for s in self.source_datasets],
            'target_datasets': [t.to_dict() for t in self.target_datasets],
            'transformations': [t.to_dict() for t in self.transformations],
            'metadata': self.metadata
        }


class LineageTracker:
    # Tracks data lineage throughout the pipeline.
    
    def __init__(self, dag_id: str, run_id: str):
        self.dag_id = dag_id
        self.run_id = run_id
        self.events: List[LineageEvent] = []
        self.metadata: Dict[str, Any] = {
            'dag_id': dag_id,
            'run_id': run_id,
            'started_at': datetime.now().isoformat()
        }
    
    def track_read(
        self,
        task_id: str,
        source_name: str,
        source_namespace: str,
        row_count: Optional[int] = None,
        columns: Optional[List[str]] = None,
        schema_version: Optional[str] = None
    ) -> LineageEvent:
        # Track a data read operation.
        event = LineageEvent(
            event_type=LineageEventType.READ,
            timestamp=datetime.now(),
            task_id=task_id,
            run_id=self.run_id,
            source_datasets=[DatasetInfo(
                name=source_name,
                namespace=source_namespace,
                row_count=row_count,
                columns=columns,
                schema_version=schema_version
            )],
            target_datasets=[]
        )
        self.events.append(event)
        logger.debug(f"Lineage: READ from {source_namespace}.{source_name}")
        return event
    
    def track_write(
        self,
        task_id: str,
        target_name: str,
        target_namespace: str,
        source_name: Optional[str] = None,
        source_namespace: Optional[str] = None,
        row_count: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> LineageEvent:
        # Track a data write operation.
        source_datasets = []
        if source_name and source_namespace:
            source_datasets.append(DatasetInfo(
                name=source_name,
                namespace=source_namespace
            ))
        
        event = LineageEvent(
            event_type=LineageEventType.WRITE,
            timestamp=datetime.now(),
            task_id=task_id,
            run_id=self.run_id,
            source_datasets=source_datasets,
            target_datasets=[DatasetInfo(
                name=target_name,
                namespace=target_namespace,
                row_count=row_count,
                columns=columns
            )]
        )
        self.events.append(event)
        logger.debug(f"Lineage: WRITE to {target_namespace}.{target_name}")
        return event
    
    def track_transform(
        self,
        task_id: str,
        source_name: str,
        source_namespace: str,
        target_name: str,
        target_namespace: str,
        transformations: List[TransformationInfo],
        input_row_count: Optional[int] = None,
        output_row_count: Optional[int] = None
    ) -> LineageEvent:
        # Track a transformation operation.
        event = LineageEvent(
            event_type=LineageEventType.TRANSFORM,
            timestamp=datetime.now(),
            task_id=task_id,
            run_id=self.run_id,
            source_datasets=[DatasetInfo(
                name=source_name,
                namespace=source_namespace,
                row_count=input_row_count
            )],
            target_datasets=[DatasetInfo(
                name=target_name,
                namespace=target_namespace,
                row_count=output_row_count
            )],
            transformations=transformations,
            metadata={
                'input_row_count': input_row_count,
                'output_row_count': output_row_count,
                'row_change': output_row_count - input_row_count if input_row_count and output_row_count else None
            }
        )
        self.events.append(event)
        logger.debug(f"Lineage: TRANSFORM {source_namespace}.{source_name} -> {target_namespace}.{target_name}")
        return event
    
    def track_validation(
        self,
        task_id: str,
        dataset_name: str,
        dataset_namespace: str,
        validation_result: Dict[str, Any]
    ) -> LineageEvent:
        # Track a validation operation.
        event = LineageEvent(
            event_type=LineageEventType.VALIDATE,
            timestamp=datetime.now(),
            task_id=task_id,
            run_id=self.run_id,
            source_datasets=[DatasetInfo(
                name=dataset_name,
                namespace=dataset_namespace
            )],
            target_datasets=[],
            metadata={'validation_result': validation_result}
        )
        self.events.append(event)
        logger.debug(f"Lineage: VALIDATE {dataset_namespace}.{dataset_name}")
        return event
    
    def track_aggregation(
        self,
        task_id: str,
        source_name: str,
        source_namespace: str,
        kpi_tables: List[str],
        target_namespace: str,
        metrics: Dict[str, Any]
    ) -> LineageEvent:
        # Track a KPI/aggregation operation.
        event = LineageEvent(
            event_type=LineageEventType.AGGREGATE,
            timestamp=datetime.now(),
            task_id=task_id,
            run_id=self.run_id,
            source_datasets=[DatasetInfo(
                name=source_name,
                namespace=source_namespace
            )],
            target_datasets=[
                DatasetInfo(name=table, namespace=target_namespace)
                for table in kpi_tables
            ],
            metadata={'metrics': metrics}
        )
        self.events.append(event)
        logger.debug(f"Lineage: AGGREGATE to {len(kpi_tables)} KPI tables")
        return event
    
    def get_lineage_summary(self) -> Dict[str, Any]:
        # Get a summary of all lineage events.
        return {
            'dag_id': self.dag_id,
            'run_id': self.run_id,
            'total_events': len(self.events),
            'events_by_type': {
                event_type.value: len([e for e in self.events if e.event_type == event_type])
                for event_type in LineageEventType
            },
            'datasets_read': list(set(
                f"{d.namespace}.{d.name}"
                for e in self.events
                for d in e.source_datasets
            )),
            'datasets_written': list(set(
                f"{d.namespace}.{d.name}"
                for e in self.events
                for d in e.target_datasets
            )),
            'events': [e.to_dict() for e in self.events]
        }
    
    def to_json(self) -> str:
        # Serialize lineage to JSON.
        return json.dumps(self.get_lineage_summary(), indent=2, default=str)


class MetadataCollector:
    # Collects and manages pipeline metadata.
    
    def __init__(self):
        self.metadata: Dict[str, Any] = {}
        self.task_metrics: Dict[str, Dict[str, Any]] = {}
    
    def set_pipeline_metadata(
        self,
        dag_id: str,
        run_id: str,
        execution_date: Optional[datetime] = None
    ) -> None:
        # Set pipeline-level metadata.
        self.metadata = {
            'dag_id': dag_id,
            'run_id': run_id,
            'execution_date': execution_date.isoformat() if execution_date else None,
            'collected_at': datetime.now().isoformat()
        }
    
    def add_task_metrics(
        self,
        task_id: str,
        metrics: Dict[str, Any]
    ) -> None:
        # Add metrics for a specific task.
        self.task_metrics[task_id] = {
            **metrics,
            'recorded_at': datetime.now().isoformat()
        }
    
    def add_data_quality_metrics(
        self,
        task_id: str,
        total_records: int,
        valid_records: int,
        null_counts: Dict[str, int],
        validation_details: Optional[Dict] = None
    ) -> None:
        # Add data quality metrics.
        quality_metrics = {
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': total_records - valid_records,
            'validity_ratio': valid_records / total_records if total_records > 0 else 0,
            'null_counts': null_counts,
            'validation_details': validation_details
        }
        self.add_task_metrics(f"{task_id}_quality", quality_metrics)
    
    def add_performance_metrics(
        self,
        task_id: str,
        duration_seconds: float,
        rows_processed: int,
        memory_mb: Optional[float] = None
    ) -> None:
        # Add performance metrics.
        perf_metrics = {
            'duration_seconds': duration_seconds,
            'rows_processed': rows_processed,
            'rows_per_second': rows_processed / duration_seconds if duration_seconds > 0 else 0,
            'memory_mb': memory_mb
        }
        self.add_task_metrics(f"{task_id}_performance", perf_metrics)
    
    def get_all_metadata(self) -> Dict[str, Any]:
        # Get all collected metadata.
        return {
            'pipeline': self.metadata,
            'tasks': self.task_metrics
        }
    
    def to_json(self) -> str:
        # Serialize metadata to JSON.
        return json.dumps(self.get_all_metadata(), indent=2, default=str)


# Global instances for use across tasks
_lineage_tracker: Optional[LineageTracker] = None
_metadata_collector: Optional[MetadataCollector] = None


def get_lineage_tracker(dag_id: str, run_id: str) -> LineageTracker:
    # Get or create a lineage tracker instance.
    global _lineage_tracker
    if _lineage_tracker is None or _lineage_tracker.run_id != run_id:
        _lineage_tracker = LineageTracker(dag_id, run_id)
    return _lineage_tracker


def get_metadata_collector() -> MetadataCollector:
    # Get or create a metadata collector instance.
    global _metadata_collector
    if _metadata_collector is None:
        _metadata_collector = MetadataCollector()
    return _metadata_collector
