from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
import re

from testdata_generation.engine.feature_item_schema import (
    get_feature_item_fields,
    normalize_feature_name,
)

_ALLOWED_TECHNIQUES = {"EP", "BVA"}
_ALLOWED_VALIDITY = {"valid", "invalid"}
_ALLOWED_BOUNDARY_KINDS = {"range", "exact"}
_ALLOWED_BOUNDARY_POINTS = {
    "MIN-1", "MIN", "MIN+1",
    "MAX-1", "MAX", "MAX+1",
    "N-1", "N", "N+1",
}
_ALLOWED_PRIORITIES = {"High", "Medium", "Low"}
_ALLOWED_RULE_TYPES = {"happy_path", "single_fault", "boundary", "business_rule"}

_EXACT_POINTS = {"N-1", "N", "N+1"}
_RANGE_POINTS = {"MIN-1", "MIN", "MIN+1", "MAX-1", "MAX", "MAX+1"}
_PLACEHOLDER_VALUES = {"string", "valid", "invalid", "error", "success", "number"}


@dataclass
class ValidationResult:
    ok: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def raise_if_invalid(self, title: str = "Validation failed") -> None:
        if self.ok:
            return
        raise ValueError(f"{title}:\n- " + "\n- ".join(self.errors))


class _ValidationCommon:
    @staticmethod
    def _validate_top_level_object(data: Any) -> Optional[ValidationResult]:
        if not isinstance(data, dict):
            return ValidationResult(ok=False, errors=["Top-level output must be a JSON object."])
        return None

    @staticmethod
    def _normalize_scalar(value: Any) -> str:
        return "" if value is None else str(value).strip()

    @staticmethod
    def _validate_non_empty_string(value: Any, field_name: str, errors: List[str]) -> None:
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field_name} is missing or empty.")

    @staticmethod
    def _is_placeholder(value: Any) -> bool:
        return isinstance(value, str) and value.strip().lower() in _PLACEHOLDER_VALUES

    @staticmethod
    def _load_feature_and_expected_fields(
        data: Dict[str, Any],
        errors: List[str],
        feature_key: str = "feature",
    ) -> Tuple[Optional[str], List[str]]:
        feature = data.get(feature_key)
        if not isinstance(feature, str) or not feature.strip():
            errors.append(f"Missing or invalid '{feature_key}'.")
            return None, []

        feature = normalize_feature_name(feature)
        try:
            expected_fields = get_feature_item_fields(feature)
        except ValueError as exc:
            errors.append(str(exc))
            return None, []

        return feature, expected_fields

    @staticmethod
    def _validate_required_dict(
        data: Dict[str, Any],
        key: str,
        errors: List[str],
    ) -> Optional[Dict[str, Any]]:
        value = data.get(key)
        if not isinstance(value, dict):
            errors.append(f"Missing or invalid '{key}'.")
            return None
        return value

    @staticmethod
    def _validate_required_list(
        data: Dict[str, Any],
        key: str,
        errors: List[str],
        message: Optional[str] = None,
    ) -> Optional[List[Any]]:
        value = data.get(key)
        if not isinstance(value, list):
            errors.append(message or f"Missing or invalid '{key}'. It must be a list.")
            return None
        return value

    @staticmethod
    def _validate_inputs_shape(
        prefix: str,
        inputs: Any,
        expected_fields: List[str],
        errors: List[str],
    ) -> Dict[str, Any]:
        if not isinstance(inputs, dict):
            errors.append(f"{prefix}.inputs must be an object.")
            return {}

        actual_keys = list(inputs.keys())
        missing = [f for f in expected_fields if f not in inputs]
        extra = [k for k in actual_keys if k not in expected_fields]

        if missing:
            errors.append(f"{prefix}.inputs is missing required fields: {missing}.")
        if extra:
            errors.append(f"{prefix}.inputs contains unexpected fields: {extra}.")

        return inputs

    @staticmethod
    def _load_step1_coverage_reference(
        step1_data: Optional[Dict[str, Any]],
        errors: List[str],
    ) -> Tuple[Optional[str], Dict[str, Dict[str, Any]]]:
        if step1_data is None:
            return None, {}

        if not isinstance(step1_data, dict):
            errors.append("Step 1 data must be a JSON object.")
            return None, {}

        feature = step1_data.get("feature")
        if not isinstance(feature, str) or not feature.strip():
            errors.append("Step 1 data is missing valid 'feature'.")
            return None, {}

        feature = normalize_feature_name(feature)

        coverage_items = step1_data.get("coverage_items")
        if not isinstance(coverage_items, list):
            errors.append("Step 1 data is missing valid 'coverage_items'.")
            return feature, {}

        coverage_map: Dict[str, Dict[str, Any]] = {}
        for item in coverage_items:
            if not isinstance(item, dict):
                continue
            item_id = item.get("id")
            if isinstance(item_id, str) and item_id.strip():
                coverage_map[item_id] = item

        return feature, coverage_map

    @staticmethod
    def _check_atomic_text(value: Any) -> bool:
        if not isinstance(value, str):
            return True
        lowered = value.lower()
        separators = [" hoặc ", " và ", " and/or ", " / "]
        return not any(sep in lowered for sep in separators)

    @staticmethod
    def _normalize_condition_list(values: Any) -> List[Dict[str, str]]:
        if not isinstance(values, list):
            return []

        out: List[Dict[str, str]] = []
        for item in values:
            if not isinstance(item, dict):
                continue
            field = str(item.get("field", "")).strip()
            state = str(item.get("state", "")).strip()
            if not field or not state:
                continue
            out.append({"field": field, "state": state})
        return out

    @staticmethod
    def _validate_step1_boundary(
        prefix: str,
        boundary: Any,
        technique: Any,
        errors: List[str],
    ) -> Tuple[Optional[str], Optional[str], Optional[float]]:
        if technique == "EP":
            if boundary is not None:
                errors.append(f"{prefix}.boundary must be null when technique='EP'.")
            return None, None, None

        if technique != "BVA":
            return None, None, None

        if not isinstance(boundary, dict):
            errors.append(f"{prefix}.boundary must be an object when technique='BVA'.")
            return None, None, None

        kind = boundary.get("kind")
        reference = boundary.get("reference")
        point = boundary.get("point")

        if kind not in _ALLOWED_BOUNDARY_KINDS:
            errors.append(
                f"{prefix}.boundary.kind must be one of {sorted(_ALLOWED_BOUNDARY_KINDS)}, got '{kind}'."
            )

        if not isinstance(reference, (int, float)):
            errors.append(f"{prefix}.boundary.reference must be numeric.")

        if point not in _ALLOWED_BOUNDARY_POINTS:
            errors.append(
                f"{prefix}.boundary.point must be one of {sorted(_ALLOWED_BOUNDARY_POINTS)}, got '{point}'."
            )

        if kind == "exact" and point not in _EXACT_POINTS:
            errors.append(
                f"{prefix}.boundary.point must be one of {sorted(_EXACT_POINTS)} when boundary.kind='exact'."
            )

        if kind == "range" and point not in _RANGE_POINTS:
            errors.append(
                f"{prefix}.boundary.point must be one of {sorted(_RANGE_POINTS)} when boundary.kind='range'."
            )

        return (
            kind if isinstance(kind, str) else None,
            point if isinstance(point, str) else None,
            float(reference) if isinstance(reference, (int, float)) else None,
        )


class ConditionsValidator(_ValidationCommon):
    """
    Validate Step 1 JSON output.
    """

    REQUIRED_RULE_MARKERS = (
        "bắt buộc",
        "bat buoc",
        "required",
        "not empty",
        "không được để trống",
        "khong duoc de trong",
        "không rỗng",
        "khong rong",
    )

    @classmethod
    def _is_required_rule(cls, value: Any) -> bool:
        text = str(value or "").strip().lower()
        return any(marker in text for marker in cls.REQUIRED_RULE_MARKERS)

    @staticmethod
    def _is_empty_representative(value: Any) -> bool:
        return value is None or str(value) == ""


    def _feature_specific_step1_rules(
        self,
        feature: str,
        per_field_exact_points: Dict[str, Set[str]],
        per_field_range_points: Dict[str, Set[str]],
        per_field_bva_rules: Dict[str, Set[str]],
        errors: List[str],
    ) -> None:
        if feature != "register":
            return

        if per_field_bva_rules.get("Phone") and per_field_exact_points.get("Phone", set()) != _EXACT_POINTS:
            missing = sorted(_EXACT_POINTS - per_field_exact_points.get("Phone", set()))
            errors.append(
                f"Field 'Phone' must include exact boundary points {sorted(_EXACT_POINTS)}. Missing: {missing}."
            )

        if per_field_bva_rules.get("Password") and per_field_range_points.get("Password", set()) != _RANGE_POINTS:
            missing = sorted(_RANGE_POINTS - per_field_range_points.get("Password", set()))
            errors.append(
                f"Field 'Password' must include range boundary points {sorted(_RANGE_POINTS)}. Missing: {missing}."
            )

        if per_field_bva_rules.get("Username"):
            errors.append(
                "Field 'Username' must not use BVA for register because the specification does not define a numeric boundary for Username."
            )

        if per_field_bva_rules.get("ConfirmPassword"):
            errors.append(
                "Field 'ConfirmPassword' must not use BVA for register because this field is validated by match/not-match relation, not by numeric boundary."
            )

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        top_level_error = self._validate_top_level_object(data)
        if top_level_error is not None:
            return top_level_error

        errors: List[str] = []
        warnings: List[str] = []

        feature, expected_fields = self._load_feature_and_expected_fields(data, errors)
        if feature is None:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        self._validate_non_empty_string(data.get("description"), "description", errors)

        summary = self._validate_required_dict(data, "coverage_summary", errors)
        coverage_items = self._validate_required_list(
            data,
            "coverage_items",
            errors,
            message="Missing or invalid 'coverage_items'. It must be a list.",
        )

        if summary is None or coverage_items is None:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        if not coverage_items:
            errors.append("coverage_items must not be empty.")
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        ep_count = 0
        bva_count = 0
        seen_ids: Set[str] = set()
        covered_fields: Set[str] = set()

        valid_by_field: Dict[str, int] = {f: 0 for f in expected_fields}
        invalid_by_field: Dict[str, int] = {f: 0 for f in expected_fields}
        per_field_exact_points: Dict[str, Set[str]] = {f: set() for f in expected_fields}
        per_field_range_points: Dict[str, Set[str]] = {f: set() for f in expected_fields}
        per_field_bva_rules: Dict[str, Set[str]] = {f: set() for f in expected_fields}
        required_fields: Set[str] = set()
        empty_invalid_fields: Set[str] = set()

        for idx, item in enumerate(coverage_items):
            prefix = f"coverage_items[{idx}]"

            if not isinstance(item, dict):
                errors.append(f"{prefix} must be an object.")
                continue

            item_id = item.get("id")
            field_name = item.get("field")
            technique = item.get("technique")
            item_description = item.get("description")
            validity = item.get("validity")
            partition_type = item.get("partition_type")
            representative_value = item.get("representative_value")
            boundary = item.get("boundary")
            rule = item.get("rule")
            expected_class = item.get("expected_class")

            if not isinstance(item_id, str) or not item_id.strip():
                errors.append(f"{prefix}.id is missing or invalid.")
            elif item_id in seen_ids:
                errors.append(f"Duplicate coverage item id: '{item_id}'.")
            else:
                seen_ids.add(item_id)

            if not isinstance(field_name, str) or field_name not in expected_fields:
                errors.append(f"{prefix}.field must be one of {expected_fields}, got '{field_name}'.")
                continue

            covered_fields.add(field_name)

            self._validate_non_empty_string(rule, f"{prefix}.rule", errors)
            self._validate_non_empty_string(item_description, f"{prefix}.description", errors)
            self._validate_non_empty_string(expected_class, f"{prefix}.expected_class", errors)

            if self._is_required_rule(rule) or self._is_required_rule(item_description):
                required_fields.add(field_name)

            if self._is_placeholder(expected_class):
                errors.append(f"{prefix}.expected_class must not be placeholder '{expected_class}'.")

            if not self._check_atomic_text(item_description):
                errors.append(f"{prefix}.description appears over-grouped and not atomic: '{item_description}'.")

            if not self._check_atomic_text(rule):
                warnings.append(f"{prefix}.rule may be over-grouped: '{rule}'.")

            if technique not in _ALLOWED_TECHNIQUES:
                errors.append(f"{prefix}.technique must be one of {sorted(_ALLOWED_TECHNIQUES)}, got '{technique}'.")
            elif technique == "EP":
                ep_count += 1
            elif technique == "BVA":
                bva_count += 1

            if validity not in _ALLOWED_VALIDITY:
                errors.append(f"{prefix}.validity must be one of {sorted(_ALLOWED_VALIDITY)}, got '{validity}'.")
            else:
                if validity == "valid":
                    valid_by_field[field_name] += 1
                else:
                    invalid_by_field[field_name] += 1
                    if technique == "EP" and self._is_empty_representative(representative_value):
                        empty_invalid_fields.add(field_name)

            if technique == "EP":
                if partition_type not in {"valid", "invalid"}:
                    errors.append(f"{prefix}.partition_type must be 'valid' or 'invalid' when technique='EP'.")
                elif validity in _ALLOWED_VALIDITY and partition_type != validity:
                    errors.append(f"{prefix}.partition_type must equal validity when technique='EP'.")

            if technique == "BVA":
                if partition_type is not None:
                    errors.append(f"{prefix}.partition_type must be null when technique='BVA'.")

            if not isinstance(representative_value, str):
                errors.append(f"{prefix}.representative_value must be a string.")

            kind, point, _reference = self._validate_step1_boundary(prefix, boundary, technique, errors)

            if technique == "BVA":
                per_field_bva_rules[field_name].add(self._normalize_scalar(rule))
                if kind == "exact" and point:
                    per_field_exact_points[field_name].add(point)
                if kind == "range" and point:
                    per_field_range_points[field_name].add(point)

        declared_ep = summary.get("EP_count")
        declared_bva = summary.get("BVA_count")
        declared_total = summary.get("TOTAL")

        if not isinstance(declared_ep, int) or declared_ep < 0:
            errors.append("coverage_summary.EP_count must be a non-negative integer.")
        if not isinstance(declared_bva, int) or declared_bva < 0:
            errors.append("coverage_summary.BVA_count must be a non-negative integer.")
        if not isinstance(declared_total, int) or declared_total < 0:
            errors.append("coverage_summary.TOTAL must be a non-negative integer.")

        if isinstance(declared_ep, int) and declared_ep != ep_count:
            errors.append(f"coverage_summary.EP_count={declared_ep} but actual EP items={ep_count}.")
        if isinstance(declared_bva, int) and declared_bva != bva_count:
            errors.append(f"coverage_summary.BVA_count={declared_bva} but actual BVA items={bva_count}.")
        if isinstance(declared_total, int) and declared_total != len(coverage_items):
            errors.append(f"coverage_summary.TOTAL={declared_total} but actual coverage_items={len(coverage_items)}.")
        if (
            isinstance(declared_ep, int)
            and isinstance(declared_bva, int)
            and isinstance(declared_total, int)
            and declared_total != declared_ep + declared_bva
        ):
            errors.append("coverage_summary.TOTAL must equal EP_count + BVA_count.")

        missing_fields = [f for f in expected_fields if f not in covered_fields]
        if missing_fields:
            errors.append(f"No coverage items found for fields: {missing_fields}.")

        for field in expected_fields:
            if valid_by_field[field] == 0:
                errors.append(f"Field '{field}' has no valid coverage item.")
            if invalid_by_field[field] == 0:
                errors.append(f"Field '{field}' has no invalid coverage item.")

        for field in sorted(required_fields):
            if field not in empty_invalid_fields:
                errors.append(
                    f"Field '{field}' is required but has no separate EP invalid coverage item for empty input."
                )

        for field, points in per_field_exact_points.items():
            if points and points != _EXACT_POINTS:
                missing = sorted(_EXACT_POINTS - points)
                errors.append(f"Field '{field}' uses BVA exact but does not include full exact boundary set. Missing: {missing}.")

        for field, points in per_field_range_points.items():
            if points and points != _RANGE_POINTS:
                missing = sorted(_RANGE_POINTS - points)
                errors.append(f"Field '{field}' uses BVA range but does not include full range boundary set. Missing: {missing}.")

        self._feature_specific_step1_rules(
            feature,
            per_field_exact_points,
            per_field_range_points,
            per_field_bva_rules,
            errors,
        )

        return ValidationResult(ok=not errors, errors=errors, warnings=warnings)

    def validate_or_raise(self, data: Dict[str, Any]) -> ValidationResult:
        result = self.validate(data)
        result.raise_if_invalid("Step 1 coverage validation failed")
        return result


class DecisionTableValidator(_ValidationCommon):
    """
    Validate Step 2 Decision Table theo schema mới:
    - conditions[]
    - actions[]
    - decision_rules[].condition_states
    - decision_rules[].action_refs
    - decision_rules[].reduction_note

    Không validate theo coverage_refs của Step 1 ở Step 2.
    Step 2 chỉ là bảng quyết định logic; việc map coverage để tạo test data nằm ở Step 3.
    """

    def validate(
        self,
        dt_data: Dict[str, Any],
        step1_data: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        top_level_error = self._validate_top_level_object(dt_data)
        if top_level_error is not None:
            return top_level_error

        errors: List[str] = []
        warnings: List[str] = []

        feature, _ = self._load_feature_and_expected_fields(dt_data, errors)
        if feature is None:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        self._validate_non_empty_string(dt_data.get("description"), "description", errors)

        decision_summary = self._validate_required_dict(dt_data, "decision_summary", errors)
        conditions = self._validate_required_list(dt_data, "conditions", errors)
        actions = self._validate_required_list(dt_data, "actions", errors)
        decision_rules = self._validate_required_list(dt_data, "decision_rules", errors)

        if decision_summary is None or conditions is None or actions is None or decision_rules is None:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        if not conditions:
            errors.append("conditions must not be empty.")
        if not actions:
            errors.append("actions must not be empty.")
        if not decision_rules:
            errors.append("decision_rules must not be empty.")
        if errors:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        # Validate conditions
        condition_ids: List[str] = []
        seen_condition_ids: Set[str] = set()
        for idx, cond in enumerate(conditions):
            prefix = f"conditions[{idx}]"
            if not isinstance(cond, dict):
                errors.append(f"{prefix} must be an object.")
                continue

            cid = cond.get("id")
            if not isinstance(cid, str) or not cid.strip():
                errors.append(f"{prefix}.id is missing or invalid.")
                continue
            cid = cid.strip()
            if cid in seen_condition_ids:
                errors.append(f"Duplicate condition id: {cid}")
            seen_condition_ids.add(cid)
            condition_ids.append(cid)

            self._validate_non_empty_string(cond.get("name"), f"{prefix}.name", errors)
            self._validate_non_empty_string(cond.get("meaning_when_y"), f"{prefix}.meaning_when_y", errors)
            self._validate_non_empty_string(cond.get("meaning_when_n"), f"{prefix}.meaning_when_n", errors)

            values = cond.get("values")
            if not isinstance(values, list) or "Y" not in values or "N" not in values:
                errors.append(f"{prefix}.values must contain Y and N.")

        condition_id_set = set(condition_ids)

        # Validate actions
        action_ids: List[str] = []
        action_expected: Dict[str, str] = {}
        seen_action_ids: Set[str] = set()
        for idx, action in enumerate(actions):
            prefix = f"actions[{idx}]"
            if not isinstance(action, dict):
                errors.append(f"{prefix} must be an object.")
                continue

            aid = action.get("id")
            if not isinstance(aid, str) or not aid.strip():
                errors.append(f"{prefix}.id is missing or invalid.")
                continue
            aid = aid.strip()
            if aid in seen_action_ids:
                errors.append(f"Duplicate action id: {aid}")
            seen_action_ids.add(aid)
            action_ids.append(aid)

            self._validate_non_empty_string(action.get("name"), f"{prefix}.name", errors)
            self._validate_non_empty_string(action.get("expected"), f"{prefix}.expected", errors)
            action_expected[aid] = str(action.get("expected", "")).strip()

        action_id_set = set(action_ids)

        # Validate summary rebuilt by pipeline
        expected_summary = {
            "condition_count": len(condition_ids),
            "action_count": len(action_ids),
            "full_combination_count": 2 ** len(condition_ids) if condition_ids else 0,
            "reduced_rule_count": len(decision_rules),
        }
        for key, expected_value in expected_summary.items():
            actual = decision_summary.get(key)
            if not isinstance(actual, int) or actual != expected_value:
                errors.append(
                    f"decision_summary.{key} must be {expected_value}, got {actual}."
                )

        # Validate rules
        seen_rule_ids: Set[str] = set()
        happy_path_count = 0

        for idx, rule in enumerate(decision_rules):
            prefix = f"decision_rules[{idx}]"
            if not isinstance(rule, dict):
                errors.append(f"{prefix} must be an object.")
                continue

            rule_id = rule.get("id")
            rule_type = rule.get("type")
            condition_states = rule.get("condition_states")
            action_refs = rule.get("action_refs")
            expected = rule.get("expected")
            reduction_note = rule.get("reduction_note")

            if not isinstance(rule_id, str) or not rule_id.strip():
                errors.append(f"{prefix}.id is missing or invalid.")
            elif rule_id in seen_rule_ids:
                errors.append(f"Duplicate decision rule id: {rule_id}")
            else:
                seen_rule_ids.add(rule_id)

            if rule_type not in _ALLOWED_RULE_TYPES:
                errors.append(f"{prefix}.type must be one of {sorted(_ALLOWED_RULE_TYPES)}, got '{rule_type}'.")

            if not isinstance(condition_states, dict) or not condition_states:
                errors.append(f"{prefix}.condition_states must be non-empty object.")
                continue

            state_keys = set(str(k).strip() for k in condition_states.keys())
            missing_conditions = sorted(condition_id_set - state_keys)
            extra_conditions = sorted(state_keys - condition_id_set)
            if missing_conditions:
                errors.append(f"{prefix}.condition_states missing conditions: {missing_conditions}.")
            if extra_conditions:
                errors.append(f"{prefix}.condition_states contains unknown conditions: {extra_conditions}.")

            n_count = 0
            y_count = 0
            for cid in condition_ids:
                state = condition_states.get(cid)
                if state not in {"Y", "N", "-"}:
                    errors.append(f"{prefix}.condition_states[{cid}] must be Y/N/-." )
                if state == "N":
                    n_count += 1
                elif state == "Y":
                    y_count += 1

            if rule_type == "happy_path":
                happy_path_count += 1
                if any(condition_states.get(cid) != "Y" for cid in condition_ids):
                    errors.append(f"{prefix} happy_path must have all condition_states = Y.")

            if rule_type == "single_fault" and n_count != 1:
                errors.append(f"{prefix} single_fault must contain exactly one N.")

            if not isinstance(action_refs, list) or not action_refs:
                errors.append(f"{prefix}.action_refs must be non-empty list.")
            else:
                for ref in action_refs:
                    if not isinstance(ref, str) or not ref.strip():
                        errors.append(f"{prefix}.action_refs contains invalid action id.")
                    elif ref.strip() not in action_id_set:
                        errors.append(f"{prefix}.action_refs references unknown action id '{ref}'.")

            self._validate_non_empty_string(expected, f"{prefix}.expected", errors)
            if isinstance(action_refs, list) and action_refs:
                first_ref = str(action_refs[0]).strip()
                expected_from_action = action_expected.get(first_ref, "")
                if expected_from_action and isinstance(expected, str) and expected.strip() != expected_from_action:
                    warnings.append(
                        f"{prefix}.expected differs from actions[{first_ref}].expected."
                    )

            if not isinstance(reduction_note, str) or not reduction_note.strip():
                errors.append(f"{prefix}.reduction_note must be non-empty string.")

        if happy_path_count != 1:
            errors.append(f"Step2 must contain exactly 1 happy_path rule, got {happy_path_count}.")

        return ValidationResult(ok=not errors, errors=errors, warnings=warnings)

    def validate_or_raise(
        self,
        dt_data: Dict[str, Any],
        step1_data: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        result = self.validate(dt_data, step1_data=step1_data)
        result.raise_if_invalid("Step 2 decision table validation failed")
        return result


class FinalTestcaseValidator(_ValidationCommon):
    """
    Validate Step 3 final testcase JSON.
    """

    def _build_step1_field_validity_index(
        self,
        coverage_map: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, List[str]]]:
        out: Dict[str, Dict[str, List[str]]] = {}
        for cov_id, cov in coverage_map.items():
            field = cov.get("field")
            validity = cov.get("validity")
            if not isinstance(field, str) or not isinstance(validity, str):
                continue
            out.setdefault(field, {"valid": [], "invalid": []})
            if validity in {"valid", "invalid"}:
                out[field][validity].append(cov_id)
        return out

    def _build_dt_rule_index(
        self,
        dt_data: Optional[Dict[str, Any]],
        errors: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        if dt_data is None:
            return {}

        if not isinstance(dt_data, dict):
            errors.append("Step 2 DT data must be a JSON object.")
            return {}

        dt_feature = dt_data.get("feature")
        if dt_feature is not None and (not isinstance(dt_feature, str) or not dt_feature.strip()):
            errors.append("Step 2 DT data has invalid 'feature'.")

        decision_rules = dt_data.get("decision_rules")
        if not isinstance(decision_rules, list):
            errors.append("Step 2 DT data is missing valid 'decision_rules'.")
            return {}

        out: Dict[str, Dict[str, Any]] = {}
        for rule in decision_rules:
            if not isinstance(rule, dict):
                continue
            rule_id = rule.get("id")
            if isinstance(rule_id, str) and rule_id.strip():
                out[rule_id] = rule
        return out

    def validate(
        self,
        final_data: Dict[str, Any],
        step1_data: Optional[Dict[str, Any]] = None,
        dt_data: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        top_level_error = self._validate_top_level_object(final_data)
        if top_level_error is not None:
            return top_level_error

        errors: List[str] = []
        warnings: List[str] = []

        feature, expected_fields = self._load_feature_and_expected_fields(final_data, errors)
        if feature is None:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        self._validate_non_empty_string(final_data.get("description"), "description", errors)

        testcase_summary = self._validate_required_dict(final_data, "testcase_summary", errors)
        testcases = self._validate_required_list(
            final_data,
            "testcases",
            errors,
            message="Missing or invalid 'testcases'. It must be a list.",
        )

        if testcase_summary is None or testcases is None:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        if not testcases:
            errors.append("testcases must not be empty.")
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        step1_feature, coverage_map = self._load_step1_coverage_reference(step1_data, errors)
        if step1_feature is not None and step1_feature != feature:
            errors.append(
                f"Feature mismatch: Step 3 feature='{feature}' but Step 1 feature='{step1_feature}'."
            )

        dt_rule_map = self._build_dt_rule_index(dt_data, errors)
        if isinstance(dt_data, dict):
            dt_feature = dt_data.get("feature")
            if isinstance(dt_feature, str) and dt_feature.strip():
                dt_feature = normalize_feature_name(dt_feature)
                if dt_feature != feature:
                    errors.append(
                        f"Feature mismatch: Step 3 feature='{feature}' but Step 2 feature='{dt_feature}'."
                    )

        declared_total_testcases = testcase_summary.get("total_testcases")
        if not isinstance(declared_total_testcases, int) or declared_total_testcases < 0:
            errors.append("testcase_summary.total_testcases must be a non-negative integer.")
        elif declared_total_testcases != len(testcases):
            errors.append(
                f"testcase_summary.total_testcases={declared_total_testcases} but actual testcases={len(testcases)}."
            )

        seen_testcase_ids: Set[str] = set()
        all_coverage_refs_used: Set[str] = set()
        all_rule_ids_used: Set[str] = set()
        has_happy_path = False
        step1_field_index = self._build_step1_field_validity_index(coverage_map)

        for idx, tc in enumerate(testcases):
            prefix = f"testcases[{idx}]"

            if not isinstance(tc, dict):
                errors.append(f"{prefix} must be an object.")
                continue

            testcase_id = tc.get("id")
            name = tc.get("name")
            description = tc.get("description")
            objective = tc.get("objective")
            coverage_refs = tc.get("coverage_refs")
            decision_basis = tc.get("decision_basis")
            expected = tc.get("expected")
            priority = tc.get("priority")
            inputs = self._validate_inputs_shape(prefix, tc.get("inputs"), expected_fields, errors)

            if not isinstance(testcase_id, str) or not testcase_id.strip():
                errors.append(f"{prefix}.id is missing or invalid.")
            elif testcase_id in seen_testcase_ids:
                errors.append(f"Duplicate testcase id: '{testcase_id}'.")
            else:
                seen_testcase_ids.add(testcase_id)

            self._validate_non_empty_string(name, f"{prefix}.name", errors)
            self._validate_non_empty_string(description, f"{prefix}.description", errors)
            self._validate_non_empty_string(objective, f"{prefix}.objective", errors)
            self._validate_non_empty_string(expected, f"{prefix}.expected", errors)

            if self._is_placeholder(expected):
                errors.append(f"{prefix}.expected must not be placeholder '{expected}'.")

            if priority not in _ALLOWED_PRIORITIES:
                errors.append(
                    f"{prefix}.priority must be one of {sorted(_ALLOWED_PRIORITIES)}, got '{priority}'."
                )

            invalid_refs = 0
            valid_refs = 0
            invalid_fields: Set[str] = set()

            if not isinstance(coverage_refs, list) or not coverage_refs:
                errors.append(f"{prefix}.coverage_refs must be a non-empty list.")
            else:
                seen_local_refs: Set[str] = set()
                for ref in coverage_refs:
                    if not isinstance(ref, str) or not ref.strip():
                        errors.append(f"{prefix}.coverage_refs contains invalid coverage id.")
                        continue

                    if ref in seen_local_refs:
                        errors.append(f"{prefix}.coverage_refs contains duplicate id '{ref}'.")
                        continue
                    seen_local_refs.add(ref)

                    all_coverage_refs_used.add(ref)

                    if coverage_map and ref not in coverage_map:
                        errors.append(f"{prefix}.coverage_refs contains unknown Step 1 coverage id '{ref}'.")
                        continue

                    cov = coverage_map.get(ref)
                    if not cov:
                        continue

                    cov_validity = cov.get("validity")
                    cov_field = cov.get("field")
                    cov_rep = cov.get("representative_value")

                    if cov_validity == "invalid":
                        invalid_refs += 1
                        if isinstance(cov_field, str):
                            invalid_fields.add(cov_field)
                    elif cov_validity == "valid":
                        valid_refs += 1

                    if isinstance(cov_field, str) and cov_field in inputs and inputs[cov_field] != cov_rep:
                        errors.append(
                            f"{prefix}.inputs['{cov_field}'] must match Step 1 representative_value of coverage '{ref}'."
                        )

                if invalid_refs == 0 and valid_refs > 0:
                    has_happy_path = True

                if invalid_refs > 1:
                    errors.append(f"{prefix} has more than one invalid coverage reference, violating single-fault.")

                if len(invalid_fields) > 1:
                    errors.append(f"{prefix} contains invalid coverage on multiple fields, violating single-fault.")

            if not isinstance(decision_basis, dict):
                errors.append(f"{prefix}.decision_basis must be an object.")
            else:
                rule_id = decision_basis.get("rule_id")
                conditions = decision_basis.get("conditions")
                optimization_note = decision_basis.get("optimization_note")

                if not isinstance(rule_id, str) or not rule_id.strip():
                    errors.append(f"{prefix}.decision_basis.rule_id must be non-empty.")
                else:
                    all_rule_ids_used.add(rule_id)
                    if dt_rule_map and rule_id not in dt_rule_map:
                        errors.append(f"{prefix}.decision_basis.rule_id '{rule_id}' not found in Step 2 DT.")
                    elif dt_rule_map:
                        dt_rule = dt_rule_map[rule_id]

                        dt_refs = dt_rule.get("coverage_refs")
                        if isinstance(dt_refs, list) and dt_refs:
                            tc_refs = [r for r in coverage_refs if isinstance(r, str)] if isinstance(coverage_refs, list) else []
                            rule_refs = [r for r in dt_refs if isinstance(r, str)]
                            if tc_refs != rule_refs:
                                errors.append(f"{prefix}.coverage_refs must match Step 2 decision rule '{rule_id}'.")

                        dt_expected = dt_rule.get("expected")
                        if isinstance(dt_expected, str) and isinstance(expected, str) and dt_expected.strip() != expected.strip():
                            errors.append(f"{prefix}.expected must match Step 2 decision rule '{rule_id}'.")

                        dt_conditions = self._normalize_condition_list(dt_rule.get("conditions"))
                        tc_conditions = self._normalize_condition_list(conditions)
                        if tc_conditions != dt_conditions:
                            errors.append(
                                f"{prefix}.decision_basis.conditions must match Step 2 decision rule '{rule_id}'."
                            )

                if not isinstance(conditions, list) or not conditions:
                    errors.append(f"{prefix}.decision_basis.conditions must be a non-empty list.")
                else:
                    seen_conditions: Set[Tuple[str, str]] = set()
                    for cond_idx, cond in enumerate(conditions):
                        cond_prefix = f"{prefix}.decision_basis.conditions[{cond_idx}]"
                        if not isinstance(cond, dict):
                            errors.append(f"{cond_prefix} must be an object.")
                            continue

                        cond_field = cond.get("field")
                        cond_state = cond.get("state")

                        if not isinstance(cond_field, str) or cond_field not in expected_fields:
                            errors.append(f"{cond_prefix}.field must be one of {expected_fields}, got '{cond_field}'.")

                        if cond_state not in {"valid", "invalid"}:
                            errors.append(f"{cond_prefix}.state must be 'valid' or 'invalid', got '{cond_state}'.")

                        if (
                            isinstance(cond_field, str)
                            and isinstance(cond_state, str)
                            and cond_field.strip()
                            and cond_state.strip()
                        ):
                            key = (cond_field.strip(), cond_state.strip())
                            if key in seen_conditions:
                                errors.append(f"{cond_prefix} duplicates condition {key}.")
                            seen_conditions.add(key)

                if not isinstance(optimization_note, str):
                    errors.append(f"{prefix}.decision_basis.optimization_note must be a string.")

            if coverage_map and isinstance(inputs, dict) and invalid_refs == 1:
                for field in expected_fields:
                    if field in invalid_fields:
                        continue
                    valid_ids = step1_field_index.get(field, {}).get("valid", [])
                    if not valid_ids:
                        continue
                    valid_values = {
                        coverage_map[v_id].get("representative_value")
                        for v_id in valid_ids
                        if v_id in coverage_map
                    }
                    if field in inputs and inputs[field] not in valid_values:
                        warnings.append(
                            f"{prefix}.inputs['{field}'] does not match any known valid representative_value from Step 1."
                        )

        if coverage_map:
            missing_coverage = sorted(set(coverage_map.keys()) - all_coverage_refs_used)
            if missing_coverage:
                errors.append(
                    f"Some Step 1 coverage items are not referenced by any testcase: {missing_coverage}."
                )

        if dt_rule_map:
            missing_rules = sorted(set(dt_rule_map.keys()) - all_rule_ids_used)
            if missing_rules:
                errors.append(
                    f"Some Step 2 decision rules are not referenced by any testcase: {missing_rules}."
                )

        if coverage_map and not has_happy_path:
            errors.append("Step 3 must contain at least one happy path testcase using only valid coverage.")

        return ValidationResult(ok=not errors, errors=errors, warnings=warnings)

    def validate_or_raise(
        self,
        final_data: Dict[str, Any],
        step1_data: Optional[Dict[str, Any]] = None,
        dt_data: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        result = self.validate(final_data, step1_data=step1_data, dt_data=dt_data)
        result.raise_if_invalid("Step 3 final output validation failed")
        return result


# Backward compatibility
CoverageValidator = ConditionsValidator
FinalDTValidator = FinalTestcaseValidator
FinalItemsValidator = FinalTestcaseValidator
Step2DecisionTableValidator = DecisionTableValidator
Step3FinalValidator = FinalTestcaseValidator