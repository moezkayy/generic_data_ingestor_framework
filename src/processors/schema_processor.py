import logging
from typing import Dict, List, Any, Set, Union, Optional
from collections import defaultdict, Counter
import json
from datetime import datetime


class SchemaProcessor:
    """
    Advanced schema processor with inference, validation, and analysis capabilities
    """

    def __init__(self):
        """Initialize the schema processor"""
        self.logger = logging.getLogger('data_ingestion.schema_processor')
        self.schema_cache = {}
        self.inference_stats = {
            'schemas_inferred': 0,
            'validation_attempts': 0,
            'validation_successes': 0,
            'merge_operations': 0
        }

    def infer_json_schema(self, data: Union[Dict, List[Dict]],
                          schema_name: str = None,
                          max_samples: int = 1000) -> Dict[str, Any]:
        """
        Infer comprehensive schema from JSON data

        Args:
            data: JSON data (dict or list of dicts)
            schema_name (str, optional): Name for the schema
            max_samples (int): Maximum samples to analyze for large datasets

        Returns:
            Dict[str, Any]: Comprehensive inferred schema
        """
        self.logger.info(f"Inferring schema for {schema_name or 'unnamed'} data")

        try:
            # Handle different data types
            if isinstance(data, list):
                schema = self._infer_array_schema(data, max_samples)
            elif isinstance(data, dict):
                schema = self._infer_object_schema(data)
            else:
                schema = {"type": self._get_json_type(data)}

            # Add metadata
            schema['$metadata'] = {
                'inferred_at': datetime.now().isoformat(),
                'schema_name': schema_name,
                'sample_size': min(len(data) if isinstance(data, list) else 1, max_samples),
                'inference_version': '1.0'
            }

            # Cache schema if named
            if schema_name:
                self.schema_cache[schema_name] = schema

            self.inference_stats['schemas_inferred'] += 1
            self.logger.info(f"Schema inference completed for {schema_name or 'unnamed'}")

            return schema

        except Exception as e:
            self.logger.error(f"Error inferring schema: {e}")
            raise

    def _infer_array_schema(self, data_list: List, max_samples: int) -> Dict[str, Any]:
        """
        Infer schema from array of objects

        Args:
            data_list (List): List of data objects
            max_samples (int): Maximum samples to analyze

        Returns:
            Dict[str, Any]: Array schema
        """
        if not data_list:
            return {
                "type": "array",
                "items": {},
                "minItems": 0,
                "maxItems": 0
            }

        # Sample data if too large
        sample_data = data_list[:max_samples] if len(data_list) > max_samples else data_list

        # Analyze each item
        item_schemas = []
        type_distribution = Counter()

        for item in sample_data:
            item_type = self._get_json_type(item)
            type_distribution[item_type] += 1

            if isinstance(item, dict):
                item_schemas.append(self._infer_object_schema(item))

        # Determine item schema
        if item_schemas:
            # Merge all object schemas
            merged_schema = self._merge_multiple_schemas(item_schemas)
            items_schema = merged_schema
        else:
            # Handle primitive types
            most_common_type = type_distribution.most_common(1)[0][0]
            items_schema = {"type": most_common_type}

        return {
            "type": "array",
            "items": items_schema,
            "minItems": len(data_list),
            "maxItems": len(data_list),
            "typeDistribution": dict(type_distribution),
            "sampleSize": len(sample_data),
            "totalSize": len(data_list)
        }

    def _infer_object_schema(self, obj: Dict, parent_path: str = "") -> Dict[str, Any]:
        """
        Infer schema from a single object with detailed analysis

        Args:
            obj (Dict): Object to analyze
            parent_path (str): Path for nested objects

        Returns:
            Dict[str, Any]: Object schema
        """
        if not isinstance(obj, dict):
            return {"type": self._get_json_type(obj)}

        properties = {}
        required = []
        field_stats = {}

        for key, value in obj.items():
            field_path = f"{parent_path}.{key}" if parent_path else key

            # Track required fields (non-null values)
            if value is not None:
                required.append(key)

            # Analyze field
            if isinstance(value, dict):
                properties[key] = self._infer_object_schema(value, field_path)
                field_stats[key] = {"type": "object", "nested_fields": len(value)}

            elif isinstance(value, list):
                properties[key] = self._infer_list_field_schema(value, field_path)
                field_stats[key] = {"type": "array", "length": len(value)}

            else:
                field_type = self._get_json_type(value)
                properties[key] = {
                    "type": field_type,
                    "nullable": value is None
                }

                # Add type-specific constraints
                if field_type == "string" and value:
                    properties[key].update(self._analyze_string_field(value))
                elif field_type in ["integer", "number"] and value is not None:
                    properties[key].update(self._analyze_numeric_field(value))

                field_stats[key] = {
                    "type": field_type,
                    "value_sample": str(value)[:50] if value else None
                }

        return {
            "type": "object",
            "properties": properties,
            "required": required,
            "additionalProperties": False,
            "fieldCount": len(properties),
            "fieldStats": field_stats
        }

    def _infer_list_field_schema(self, arr: List, field_path: str) -> Dict[str, Any]:
        """
        Infer schema for list fields with detailed analysis

        Args:
            arr (List): List to analyze
            field_path (str): Field path

        Returns:
            Dict[str, Any]: List field schema
        """
        if not arr:
            return {
                "type": "array",
                "items": {},
                "minItems": 0,
                "maxItems": 0
            }

        # Analyze item types
        item_types = Counter()
        item_schemas = []

        for item in arr:
            item_type = self._get_json_type(item)
            item_types[item_type] += 1

            if isinstance(item, dict):
                item_schemas.append(self._infer_object_schema(item, f"{field_path}[]"))

        # Determine items schema
        if len(item_types) == 1:
            # Homogeneous array
            single_type = list(item_types.keys())[0]
            if single_type == "object" and item_schemas:
                items_schema = self._merge_multiple_schemas(item_schemas)
            else:
                items_schema = {"type": single_type}
        else:
            # Heterogeneous array
            items_schema = {"type": "mixed", "typeDistribution": dict(item_types)}

        return {
            "type": "array",
            "items": items_schema,
            "minItems": len(arr),
            "maxItems": len(arr),
            "isHomogeneous": len(item_types) == 1,
            "typeDistribution": dict(item_types)
        }

    def _analyze_string_field(self, value: str) -> Dict[str, Any]:
        """Analyze string field for patterns and constraints"""
        analysis = {
            "minLength": len(value),
            "maxLength": len(value)
        }

        # Check for common patterns
        if value.count('@') == 1 and '.' in value:
            analysis["pattern"] = "email"
        elif value.startswith(('http://', 'https://')):
            analysis["pattern"] = "url"
        elif value.replace('-', '').replace(' ', '').isdigit():
            analysis["pattern"] = "phone_or_id"
        elif len(value) == 36 and value.count('-') == 4:
            analysis["pattern"] = "uuid"

        return analysis

    def _analyze_numeric_field(self, value: Union[int, float]) -> Dict[str, Any]:
        """Analyze numeric field for constraints"""
        return {
            "minimum": value,
            "maximum": value,
            "isInteger": isinstance(value, int)
        }

    def _get_json_type(self, value: Any) -> str:
        """
        Get JSON schema type for a Python value

        Args:
            value: Python value

        Returns:
            str: JSON schema type
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            return "array"
        elif isinstance(value, dict):
            return "object"
        else:
            return "unknown"

    def _merge_multiple_schemas(self, schemas: List[Dict]) -> Dict[str, Any]:
        """
        Merge multiple schemas into one comprehensive schema

        Args:
            schemas (List[Dict]): List of schemas to merge

        Returns:
            Dict[str, Any]: Merged schema
        """
        if not schemas:
            return {}

        if len(schemas) == 1:
            return schemas[0]

        self.inference_stats['merge_operations'] += 1

        # Start with the first schema
        merged = schemas[0].copy()

        for schema in schemas[1:]:
            merged = self._merge_two_schemas(merged, schema)

        return merged

    def _merge_two_schemas(self, schema1: Dict, schema2: Dict) -> Dict[str, Any]:
        """
        Merge two schemas with conflict resolution

        Args:
            schema1 (Dict): First schema
            schema2 (Dict): Second schema

        Returns:
            Dict[str, Any]: Merged schema
        """
        if schema1.get("type") != schema2.get("type"):
            return {
                "type": "mixed",
                "possibleTypes": [schema1.get("type"), schema2.get("type")]
            }

        if schema1.get("type") == "object":
            return self._merge_object_schemas(schema1, schema2)
        elif schema1.get("type") == "array":
            return self._merge_array_schemas(schema1, schema2)
        else:
            return self._merge_primitive_schemas(schema1, schema2)

    def _merge_object_schemas(self, schema1: Dict, schema2: Dict) -> Dict[str, Any]:
        """Merge object schemas"""
        merged_properties = {}
        all_keys = set(schema1.get("properties", {}).keys()) | set(schema2.get("properties", {}).keys())

        for key in all_keys:
            prop1 = schema1.get("properties", {}).get(key)
            prop2 = schema2.get("properties", {}).get(key)

            if prop1 and prop2:
                merged_properties[key] = self._merge_two_schemas(prop1, prop2)
            elif prop1:
                prop1_copy = prop1.copy()
                prop1_copy["nullable"] = True
                merged_properties[key] = prop1_copy
            elif prop2:
                prop2_copy = prop2.copy()
                prop2_copy["nullable"] = True
                merged_properties[key] = prop2_copy

        # Merge required fields (intersection for strict, union for loose)
        required1 = set(schema1.get("required", []))
        required2 = set(schema2.get("required", []))
        merged_required = list(required1 & required2)  # Intersection (strict)

        return {
            "type": "object",
            "properties": merged_properties,
            "required": merged_required,
            "additionalProperties": schema1.get("additionalProperties", True) or schema2.get("additionalProperties",
                                                                                             True),
            "fieldCount": len(merged_properties)
        }

    def _merge_array_schemas(self, schema1: Dict, schema2: Dict) -> Dict[str, Any]:
        """Merge array schemas"""
        items1 = schema1.get("items", {})
        items2 = schema2.get("items", {})

        merged_items = self._merge_two_schemas(items1, items2) if items1 and items2 else (items1 or items2)

        return {
            "type": "array",
            "items": merged_items,
            "minItems": min(schema1.get("minItems", 0), schema2.get("minItems", 0)),
            "maxItems": max(schema1.get("maxItems", 0), schema2.get("maxItems", 0))
        }

    def _merge_primitive_schemas(self, schema1: Dict, schema2: Dict) -> Dict[str, Any]:
        """Merge primitive type schemas"""
        merged = schema1.copy()

        # Merge constraints
        if "minimum" in schema1 and "minimum" in schema2:
            merged["minimum"] = min(schema1["minimum"], schema2["minimum"])
        if "maximum" in schema1 and "maximum" in schema2:
            merged["maximum"] = max(schema1["maximum"], schema2["maximum"])
        if "minLength" in schema1 and "minLength" in schema2:
            merged["minLength"] = min(schema1["minLength"], schema2["minLength"])
        if "maxLength" in schema1 and "maxLength" in schema2:
            merged["maxLength"] = max(schema1["maxLength"], schema2["maxLength"])

        # Handle nullability
        merged["nullable"] = schema1.get("nullable", False) or schema2.get("nullable", False)

        return merged

    def flatten_schema(self, schema: Dict, prefix: str = "",
                       max_depth: int = 10, current_depth: int = 0) -> Dict[str, Any]:
        """
        Flatten a nested schema into a flat structure with depth control

        Args:
            schema (Dict): Schema to flatten
            prefix (str): Prefix for field names
            max_depth (int): Maximum nesting depth
            current_depth (int): Current nesting depth

        Returns:
            Dict[str, Any]: Flattened schema mapping
        """
        if current_depth >= max_depth:
            return {prefix or "root": "max_depth_exceeded"}

        flattened = {}

        if schema.get("type") == "object":
            properties = schema.get("properties", {})

            for key, prop_schema in properties.items():
                field_name = f"{prefix}.{key}" if prefix else key

                if prop_schema.get("type") == "object":
                    # Recursively flatten nested objects
                    nested_flattened = self.flatten_schema(
                        prop_schema, field_name, max_depth, current_depth + 1
                    )
                    flattened.update(nested_flattened)
                elif prop_schema.get("type") == "array":
                    # Handle arrays
                    items_schema = prop_schema.get("items", {})
                    if items_schema.get("type") == "object":
                        nested_flattened = self.flatten_schema(
                            items_schema, f"{field_name}[]", max_depth, current_depth + 1
                        )
                        flattened.update(nested_flattened)
                    else:
                        flattened[f"{field_name}[]"] = items_schema.get("type", "unknown")
                else:
                    flattened[field_name] = prop_schema.get("type", "unknown")

        elif schema.get("type") == "array":
            items_schema = schema.get("items", {})
            if items_schema.get("type") == "object":
                nested_flattened = self.flatten_schema(
                    items_schema, f"{prefix}[]" if prefix else "[]", max_depth, current_depth + 1
                )
                flattened.update(nested_flattened)
            else:
                array_name = f"{prefix}[]" if prefix else "[]"
                flattened[array_name] = items_schema.get("type", "unknown")

        else:
            field_name = prefix if prefix else "root"
            flattened[field_name] = schema.get("type", "unknown")

        return flattened

    def validate_schema_consistency(self, schemas: List[Dict],
                                    tolerance: float = 0.8) -> Dict[str, Any]:
        """
        Validate consistency across multiple schemas with tolerance

        Args:
            schemas (List[Dict]): List of schemas to validate
            tolerance (float): Consistency tolerance (0.0 to 1.0)

        Returns:
            Dict[str, Any]: Detailed validation results
        """
        if not schemas:
            return {"consistent": True, "issues": [], "confidence": 1.0}

        self.inference_stats['validation_attempts'] += 1

        # Flatten all schemas
        flattened_schemas = [self.flatten_schema(schema) for schema in schemas]

        # Get all unique field paths
        all_fields = set()
        for flat_schema in flattened_schemas:
            all_fields.update(flat_schema.keys())

        issues = []
        field_consistency_scores = {}

        # Check consistency for each field
        for field in all_fields:
            field_types = []
            presence_count = 0

            for flat_schema in flattened_schemas:
                if field in flat_schema:
                    field_types.append(flat_schema[field])
                    presence_count += 1

            # Calculate consistency metrics
            unique_types = set(field_types)
            presence_rate = presence_count / len(schemas)
            type_consistency = len(field_types) / len(unique_types) if unique_types else 1.0

            field_consistency_scores[field] = {
                'presence_rate': presence_rate,
                'type_consistency': type_consistency,
                'overall_score': (presence_rate + type_consistency) / 2
            }

            # Identify issues
            if len(unique_types) > 1:
                issues.append({
                    "field": field,
                    "issue_type": "type_inconsistency",
                    "types": list(unique_types),
                    "occurrence_rate": presence_rate,
                    "consistency_score": field_consistency_scores[field]['overall_score']
                })

            if presence_rate < tolerance:
                issues.append({
                    "field": field,
                    "issue_type": "low_presence",
                    "presence_rate": presence_rate,
                    "consistency_score": field_consistency_scores[field]['overall_score']
                })

        # Calculate overall consistency
        if field_consistency_scores:
            overall_consistency = sum(
                score['overall_score'] for score in field_consistency_scores.values()
            ) / len(field_consistency_scores)
        else:
            overall_consistency = 1.0

        is_consistent = overall_consistency >= tolerance and len(issues) == 0

        if is_consistent:
            self.inference_stats['validation_successes'] += 1

        return {
            "consistent": is_consistent,
            "overall_consistency_score": round(overall_consistency, 3),
            "tolerance_threshold": tolerance,
            "issues": issues,
            "field_analysis": field_consistency_scores,
            "total_fields": len(all_fields),
            "total_schemas": len(schemas),
            "summary": {
                "critical_issues": len([i for i in issues if i.get('consistency_score', 1) < 0.5]),
                "minor_issues": len([i for i in issues if i.get('consistency_score', 1) >= 0.5])
            }
        }

    def generate_schema_report(self, schema: Dict, schema_name: str = "unnamed") -> Dict[str, Any]:
        """
        Generate comprehensive schema analysis report

        Args:
            schema (Dict): Schema to analyze
            schema_name (str): Schema name

        Returns:
            Dict[str, Any]: Comprehensive schema report
        """
        flat_schema = self.flatten_schema(schema)

        # Analyze schema complexity
        complexity_metrics = {
            'total_fields': len(flat_schema),
            'nested_levels': max(field.count('.') for field in flat_schema.keys()) if flat_schema else 0,
            'array_fields': len([f for f in flat_schema.keys() if '[]' in f]),
            'object_fields': 0,
            'primitive_fields': 0
        }

        # Count field types
        type_distribution = Counter()
        for field_type in flat_schema.values():
            type_distribution[field_type] += 1
            if field_type in ['string', 'integer', 'number', 'boolean']:
                complexity_metrics['primitive_fields'] += 1
            elif field_type == 'object':
                complexity_metrics['object_fields'] += 1

        return {
            'schema_name': schema_name,
            'generated_at': datetime.now().isoformat(),
            'schema_type': schema.get('type', 'unknown'),
            'complexity_metrics': complexity_metrics,
            'type_distribution': dict(type_distribution),
            'flat_schema': flat_schema,
            'metadata': schema.get('$metadata', {}),
            'recommendations': self._generate_schema_recommendations(schema, complexity_metrics)
        }

    def _generate_schema_recommendations(self, schema: Dict, complexity_metrics: Dict) -> List[str]:
        """Generate recommendations based on schema analysis"""
        recommendations = []

        if complexity_metrics['nested_levels'] > 5:
            recommendations.append("Consider flattening deeply nested structures for better performance")

        if complexity_metrics['total_fields'] > 100:
            recommendations.append("Large number of fields detected - consider data normalization")

        if complexity_metrics['array_fields'] > complexity_metrics['total_fields'] * 0.3:
            recommendations.append("High proportion of array fields - validate data structure design")

        if schema.get('type') == 'object' and not schema.get('required'):
            recommendations.append("No required fields specified - consider adding field requirements")

        return recommendations

    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get schema processing statistics"""
        return {
            'inference_stats': self.inference_stats.copy(),
            'cached_schemas': len(self.schema_cache),
            'cache_keys': list(self.schema_cache.keys())
        }

    def clear_cache(self):
        """Clear schema cache"""
        self.schema_cache.clear()
        self.logger.info("Schema cache cleared")