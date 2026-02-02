"""
Schema Evolution Handler for Flight Price Pipeline.
Detects and handles schema changes in source data.
"""

import logging
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from datetime import datetime
import pandas as pd
import json

logger = logging.getLogger(__name__)


@dataclass
class SchemaChange:
    # Represents a detected schema change.
    change_type: str  # 'new_column', 'removed_column', 'type_change'
    column_name: str
    old_value: Optional[str]
    new_value: Optional[str]
    detected_at: datetime
    
    def to_dict(self) -> Dict:
        return {
            'change_type': self.change_type,
            'column_name': self.column_name,
            'old_value': self.old_value,
            'new_value': self.new_value,
            'detected_at': self.detected_at.isoformat()
        }


@dataclass
class SchemaReport:
    # Complete schema analysis report.
    has_changes: bool
    is_compatible: bool  # True if changes are backward compatible
    new_columns: List[str]
    removed_columns: List[str]
    type_changes: List[Dict]
    changes: List[SchemaChange]
    
    def to_dict(self) -> Dict:
        return {
            'has_changes': self.has_changes,
            'is_compatible': self.is_compatible,
            'new_columns': self.new_columns,
            'removed_columns': self.removed_columns,
            'type_changes': self.type_changes,
            'changes': [c.to_dict() for c in self.changes]
        }


class SchemaEvolutionHandler:
    
    # Handles schema evolution detection and management.
    # Detects new columns, removed columns, and type changes.
    
    
    # Expected schema definition
    EXPECTED_SCHEMA = {
        'airline': 'object',
        'source': 'object',
        'source_name': 'object',
        'destination': 'object',
        'destination_name': 'object',
        'departure_datetime': 'object',
        'arrival_datetime': 'object',
        'duration_hrs': 'float64',
        'stopovers': 'object',
        'aircraft_type': 'object',
        'class': 'object',
        'booking_source': 'object',
        'base_fare_bdt': 'float64',
        'tax_surcharge_bdt': 'float64',
        'total_fare_bdt': 'float64',
        'seasonality': 'object',
        'days_before_departure': 'int64'
    }
    
    # Required columns that must exist
    REQUIRED_COLUMNS = {
        'airline', 'source', 'destination', 
        'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt'
    }
    
    # Type compatibility mapping (what types can be coerced to what)
    TYPE_COMPATIBILITY = {
        'int64': {'float64', 'object'},
        'float64': {'object'},
        'object': set(),
        'datetime64[ns]': {'object'},
        'bool': {'int64', 'object'}
    }
    
    def __init__(self, expected_schema: Optional[Dict[str, str]] = None):
        # Initialize with optional custom expected schema.
        self.expected_schema = expected_schema or self.EXPECTED_SCHEMA.copy()
        self.changes: List[SchemaChange] = []
        
    def detect_schema_changes(self, df: pd.DataFrame) -> SchemaReport:
        
        # Detect schema changes between expected and actual schema.
        
        self.changes = []
        actual_schema = self._get_dataframe_schema(df)
        
        expected_cols = set(self.expected_schema.keys())
        actual_cols = set(actual_schema.keys())
        
        # Detect new columns
        new_columns = list(actual_cols - expected_cols)
        for col in new_columns:
            self.changes.append(SchemaChange(
                change_type='new_column',
                column_name=col,
                old_value=None,
                new_value=str(actual_schema[col]),
                detected_at=datetime.now()
            ))
            logger.info(f"Schema change detected: New column '{col}' with type {actual_schema[col]}")
        
        # Detect removed columns
        removed_columns = list(expected_cols - actual_cols)
        for col in removed_columns:
            self.changes.append(SchemaChange(
                change_type='removed_column',
                column_name=col,
                old_value=str(self.expected_schema[col]),
                new_value=None,
                detected_at=datetime.now()
            ))
            logger.warning(f"Schema change detected: Column '{col}' was removed")
        
        # Detect type changes in common columns
        type_changes = []
        common_cols = expected_cols & actual_cols
        for col in common_cols:
            expected_type = str(self.expected_schema[col])
            actual_type = str(actual_schema[col])
            
            if not self._types_compatible(expected_type, actual_type):
                type_changes.append({
                    'column': col,
                    'expected_type': expected_type,
                    'actual_type': actual_type
                })
                self.changes.append(SchemaChange(
                    change_type='type_change',
                    column_name=col,
                    old_value=expected_type,
                    new_value=actual_type,
                    detected_at=datetime.now()
                ))
                logger.warning(
                    f"Schema change detected: Column '{col}' type changed "
                    f"from {expected_type} to {actual_type}"
                )
        
        # Determine if changes are backward compatible
        is_compatible = self._check_compatibility(removed_columns, type_changes)
        
        has_changes = len(self.changes) > 0
        
        if has_changes:
            logger.info(
                f"Schema evolution detected: {len(new_columns)} new, "
                f"{len(removed_columns)} removed, {len(type_changes)} type changes"
            )
        
        return SchemaReport(
            has_changes=has_changes,
            is_compatible=is_compatible,
            new_columns=new_columns,
            removed_columns=removed_columns,
            type_changes=type_changes,
            changes=self.changes
        )
    
    def _get_dataframe_schema(self, df: pd.DataFrame) -> Dict[str, str]:
        # Extract schema from DataFrame.
        return {col: str(dtype) for col, dtype in df.dtypes.items()}
    
    def _types_compatible(self, expected: str, actual: str) -> bool:
        # Check if two types are compatible.
        if expected == actual:
            return True
        
        # Normalize type names
        expected = expected.replace('float64', 'float64').replace('int64', 'int64')
        actual = actual.replace('float64', 'float64').replace('int64', 'int64')
        
        # int can be stored as float
        if expected == 'int64' and actual == 'float64':
            return True
        
        # Both are numeric types
        if expected in ('int64', 'float64') and actual in ('int64', 'float64'):
            return True
            
        return False
    
    def _check_compatibility(
        self, 
        removed_columns: List[str], 
        type_changes: List[Dict]
    ) -> bool:
        
        #Check if schema changes are backward compatible.
        # Check if any required columns were removed
        removed_required = set(removed_columns) & self.REQUIRED_COLUMNS
        if removed_required:
            logger.error(f"Breaking change: Required columns removed: {removed_required}")
            return False
        
        # Check if type changes are safe
        for change in type_changes:
            if change['column'] in self.REQUIRED_COLUMNS:
                logger.warning(
                    f"Type change in required column: {change['column']}"
                )
        
        return True
    
    def adapt_dataframe(
        self, 
        df: pd.DataFrame, 
        report: SchemaReport
    ) -> pd.DataFrame:
       
        df = df.copy()
        
        # Add missing required columns with defaults
        for col in report.removed_columns:
            if col in self.REQUIRED_COLUMNS:
                default = self._get_default_for_type(self.expected_schema.get(col, 'object'))
                df[col] = default
                logger.info(f"Added missing column '{col}' with default value")
        
        # Handle type conversions
        for change in report.type_changes:
            col = change['column']
            expected_type = change['expected_type']
            try:
                if expected_type in ('int64', 'float64'):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if expected_type == 'int64':
                        df[col] = df[col].fillna(0).astype('int64')
                logger.info(f"Converted column '{col}' to {expected_type}")
            except Exception as e:
                logger.warning(f"Could not convert column '{col}': {e}")
        
        return df
    
    def _get_default_for_type(self, dtype: str) -> any:
        # Get default value for a data type.
        defaults = {
            'object': 'Unknown',
            'int64': 0,
            'float64': 0.0,
            'bool': False,
            'datetime64[ns]': None
        }
        return defaults.get(dtype, None)
    
    def update_expected_schema(self, new_columns: Dict[str, str]) -> None:
        # Update expected schema with new columns.
        self.expected_schema.update(new_columns)
        logger.info(f"Updated expected schema with {len(new_columns)} new columns")
    
    def get_schema_version(self) -> str:
        # Generate a version hash for the current expected schema.
        import hashlib
        schema_str = json.dumps(self.expected_schema, sort_keys=True)
        return hashlib.md5(schema_str.encode()).hexdigest()[:8]


def validate_schema_compatibility(df: pd.DataFrame) -> Tuple[bool, SchemaReport]:
   
    handler = SchemaEvolutionHandler()
    report = handler.detect_schema_changes(df)
    return report.is_compatible, report
