# Provides structured validation similar to Great Expectations.

import logging
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ExpectationResult:
    # Result of a single expectation check.
    expectation_type: str
    column: Optional[str]
    success: bool
    observed_value: Any
    expected_value: Any
    exception_info: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'expectation_type': self.expectation_type,
            'column': self.column,
            'success': bool(self.success),
            'observed_value': self.observed_value,
            'expected_value': self.expected_value,
            'exception_info': self.exception_info
        }


@dataclass
class ValidationResult:
    # Aggregated validation results.
    success: bool
    evaluated_expectations: int
    successful_expectations: int
    failed_expectations: int
    results: List[ExpectationResult] = field(default_factory=list)
    run_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        return {
            'success': bool(self.success),
            'evaluated_expectations': int(self.evaluated_expectations),
            'successful_expectations': int(self.successful_expectations),
            'failed_expectations': int(self.failed_expectations),
            'run_time': self.run_time.isoformat() if self.run_time else None,
            'results': [r.to_dict() for r in self.results]
        }


class DataQualityValidator:
 
    
    def __init__(self, df: pd.DataFrame, dataset_name: str = "dataset"):
        self.df = df
        self.dataset_name = dataset_name
        self.expectations: List[Callable] = []
        self.results: List[ExpectationResult] = []
        
    def expect_column_to_exist(self, column: str) -> 'DataQualityValidator':
        # Expect a column to exist in the dataframe.
        success = column in self.df.columns
        self.results.append(ExpectationResult(
            expectation_type='expect_column_to_exist',
            column=column,
            success=success,
            observed_value=list(self.df.columns) if not success else column,
            expected_value=column
        ))
        return self
    
    def expect_column_values_to_not_be_null(
        self, 
        column: str, 
        mostly: float = 1.0
    ) -> 'DataQualityValidator':
        # Expect column values to not be null (with optional threshold).
        if column not in self.df.columns:
            self.results.append(ExpectationResult(
                expectation_type='expect_column_values_to_not_be_null',
                column=column,
                success=False,
                observed_value="Column not found",
                expected_value=f"Column '{column}' exists"
            ))
            return self
            
        null_count = self.df[column].isnull().sum()
        total_count = len(self.df)
        non_null_ratio = (total_count - null_count) / total_count if total_count > 0 else 0
        success = non_null_ratio >= mostly
        
        self.results.append(ExpectationResult(
            expectation_type='expect_column_values_to_not_be_null',
            column=column,
            success=success,
            observed_value={'null_count': int(null_count), 'non_null_ratio': round(non_null_ratio, 4)},
            expected_value={'mostly': mostly}
        ))
        return self
    
    def expect_column_values_to_be_between(
        self, 
        column: str, 
        min_value: Optional[float] = None, 
        max_value: Optional[float] = None,
        mostly: float = 1.0
    ) -> 'DataQualityValidator':
        # Expect column values to be within a range.
        if column not in self.df.columns:
            self.results.append(ExpectationResult(
                expectation_type='expect_column_values_to_be_between',
                column=column,
                success=False,
                observed_value="Column not found",
                expected_value=f"Column '{column}' exists"
            ))
            return self
        
        valid_mask = pd.Series([True] * len(self.df))
        
        if min_value is not None:
            valid_mask &= self.df[column] >= min_value
        if max_value is not None:
            valid_mask &= self.df[column] <= max_value
            
        valid_ratio = valid_mask.sum() / len(self.df) if len(self.df) > 0 else 0
        success = valid_ratio >= mostly
        
        out_of_range = (~valid_mask).sum()
        
        self.results.append(ExpectationResult(
            expectation_type='expect_column_values_to_be_between',
            column=column,
            success=success,
            observed_value={
                'out_of_range_count': int(out_of_range),
                'valid_ratio': round(valid_ratio, 4),
                'min_observed': float(self.df[column].min()) if len(self.df) > 0 else None,
                'max_observed': float(self.df[column].max()) if len(self.df) > 0 else None
            },
            expected_value={'min': min_value, 'max': max_value, 'mostly': mostly}
        ))
        return self
    
    def expect_column_values_to_be_in_set(
        self, 
        column: str, 
        value_set: List[Any],
        mostly: float = 1.0
    ) -> 'DataQualityValidator':
        # Expect column values to be in a predefined set.
        if column not in self.df.columns:
            self.results.append(ExpectationResult(
                expectation_type='expect_column_values_to_be_in_set',
                column=column,
                success=False,
                observed_value="Column not found",
                expected_value=f"Column '{column}' exists"
            ))
            return self
        
        valid_mask = self.df[column].isin(value_set)
        valid_ratio = valid_mask.sum() / len(self.df) if len(self.df) > 0 else 0
        success = valid_ratio >= mostly
        
        unexpected_values = self.df[~valid_mask][column].unique().tolist()[:10]  # Limit to 10
        
        self.results.append(ExpectationResult(
            expectation_type='expect_column_values_to_be_in_set',
            column=column,
            success=success,
            observed_value={
                'unexpected_count': int((~valid_mask).sum()),
                'unexpected_values_sample': unexpected_values,
                'valid_ratio': round(valid_ratio, 4)
            },
            expected_value={'value_set': value_set[:20], 'mostly': mostly}  # Limit set size in output
        ))
        return self
    
    def expect_column_values_to_be_unique(
        self, 
        column: str
    ) -> 'DataQualityValidator':
        # Expect column values to be unique.
        if column not in self.df.columns:
            self.results.append(ExpectationResult(
                expectation_type='expect_column_values_to_be_unique',
                column=column,
                success=False,
                observed_value="Column not found",
                expected_value=f"Column '{column}' exists"
            ))
            return self
        
        duplicate_count = self.df[column].duplicated().sum()
        success = duplicate_count == 0
        
        self.results.append(ExpectationResult(
            expectation_type='expect_column_values_to_be_unique',
            column=column,
            success=success,
            observed_value={'duplicate_count': int(duplicate_count)},
            expected_value={'duplicates': 0}
        ))
        return self
    
    def expect_column_values_to_match_regex(
        self, 
        column: str, 
        regex: str,
        mostly: float = 1.0
    ) -> 'DataQualityValidator':
        # Expect column values to match a regex pattern.
        if column not in self.df.columns:
            self.results.append(ExpectationResult(
                expectation_type='expect_column_values_to_match_regex',
                column=column,
                success=False,
                observed_value="Column not found",
                expected_value=f"Column '{column}' exists"
            ))
            return self
        
        valid_mask = self.df[column].astype(str).str.match(regex, na=False)
        valid_ratio = valid_mask.sum() / len(self.df) if len(self.df) > 0 else 0
        success = valid_ratio >= mostly
        
        self.results.append(ExpectationResult(
            expectation_type='expect_column_values_to_match_regex',
            column=column,
            success=success,
            observed_value={'valid_ratio': round(valid_ratio, 4)},
            expected_value={'regex': regex, 'mostly': mostly}
        ))
        return self
    
    def expect_table_row_count_to_be_between(
        self, 
        min_value: int, 
        max_value: Optional[int] = None
    ) -> 'DataQualityValidator':
        # Expect table row count to be within a range.
        row_count = len(self.df)
        
        success = row_count >= min_value
        if max_value is not None:
            success = success and row_count <= max_value
        
        self.results.append(ExpectationResult(
            expectation_type='expect_table_row_count_to_be_between',
            column=None,
            success=success,
            observed_value={'row_count': row_count},
            expected_value={'min': min_value, 'max': max_value}
        ))
        return self
    
    def expect_column_mean_to_be_between(
        self, 
        column: str, 
        min_value: Optional[float] = None, 
        max_value: Optional[float] = None
    ) -> 'DataQualityValidator':
        # Expect column mean to be within a range.
        if column not in self.df.columns:
            self.results.append(ExpectationResult(
                expectation_type='expect_column_mean_to_be_between',
                column=column,
                success=False,
                observed_value="Column not found",
                expected_value=f"Column '{column}' exists"
            ))
            return self
        
        mean_value = self.df[column].mean()
        
        success = True
        if min_value is not None:
            success = success and mean_value >= min_value
        if max_value is not None:
            success = success and mean_value <= max_value
        
        self.results.append(ExpectationResult(
            expectation_type='expect_column_mean_to_be_between',
            column=column,
            success=success,
            observed_value={'mean': float(mean_value) if pd.notna(mean_value) else None},
            expected_value={'min': min_value, 'max': max_value}
        ))
        return self
    
    def validate(self) -> ValidationResult:
        # Run all expectations and return aggregated results.
        successful = sum(1 for r in self.results if r.success)
        failed = len(self.results) - successful
        
        validation_result = ValidationResult(
            success=failed == 0,
            evaluated_expectations=len(self.results),
            successful_expectations=successful,
            failed_expectations=failed,
            results=self.results,
            run_time=datetime.now()
        )
        
        logger.info(
            f"Validation complete for {self.dataset_name}: "
            f"{successful}/{len(self.results)} expectations passed"
        )
        
        return validation_result
    
    def reset(self) -> 'DataQualityValidator':
        # Reset all results for a new validation run.
        self.results = []
        return self


def create_flight_data_expectations(df: pd.DataFrame) -> DataQualityValidator:
    # Create standard expectations for flight price data.
   
    validator = DataQualityValidator(df, "flight_price_data")
    
    # Column existence checks
    required_columns = [
        'airline', 'source', 'destination', 
        'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt'
    ]
    for col in required_columns:
        validator.expect_column_to_exist(col)
    
    # Null value checks (allow 5% missing for some fields)
    validator.expect_column_values_to_not_be_null('airline', mostly=0.95)
    validator.expect_column_values_to_not_be_null('source', mostly=0.99)
    validator.expect_column_values_to_not_be_null('destination', mostly=0.99)
    validator.expect_column_values_to_not_be_null('base_fare_bdt', mostly=0.99)
    validator.expect_column_values_to_not_be_null('total_fare_bdt', mostly=0.99)
    
    # Value range checks
    validator.expect_column_values_to_be_between('base_fare_bdt', min_value=0, mostly=0.99)
    validator.expect_column_values_to_be_between('tax_surcharge_bdt', min_value=0, mostly=0.99)
    validator.expect_column_values_to_be_between('total_fare_bdt', min_value=0, mostly=0.99)
    
    # Row count check (ensure data was loaded)
    validator.expect_table_row_count_to_be_between(min_value=1)
    
    # Statistical checks
    validator.expect_column_mean_to_be_between('total_fare_bdt', min_value=1000)
    
    return validator
