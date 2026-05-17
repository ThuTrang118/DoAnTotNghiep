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
        except Exception as exc:
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

        text = value.strip()

        if not text:
            return False

        # Rule quá dài thường là dấu hiệu AI đang gộp nhiều behavior.
        if len(text) > 200:
            return False

        return True

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
        """
        Không còn luật riêng theo feature/field.

        Danh sách field hợp lệ được lấy động từ FEATURE SPECIFICATION qua
        get_feature_item_fields(feature). Validator chỉ kiểm tra theo luật chung
        của EP/BVA và schema output.
        """
        return

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

        decision_summary = dt_data.get("decision_summary")
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

            if reduction_note is not None and (
                not isinstance(reduction_note, str)
                or not reduction_note.strip()
            ):
                errors.append(f"{prefix}.reduction_note must be non-empty string if provided.")

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
    Validate Step 3 final testcase JSON theo schema framework-ready mới.

    Schema đúng:
    {
      "feature": "...",
      "description": "...",
      "items": [
        {
          "Testcase": "...",
          "<InputField>": "...",
          "Expected": "..."
        }
      ]
    }

    Không dùng schema cũ:
    - testcase_summary
    - testcases
    - id/name/objective/coverage_refs/decision_basis/inputs/priority/expected
    """

    FORBIDDEN_ITEM_KEYS = {
        "id",
        "name",
        "description",
        "objective",
        "coverage_refs",
        "decision_basis",
        "inputs",
        "priority",
        "expected",
        "testcase",
    }

    FORBIDDEN_TOP_LEVEL_KEYS = {
        "testcase_summary",
        "testcases",
        "coverage_refs",
        "decision_basis",
    }

    def _load_dt_expected_values(
        self,
        dt_data: Optional[Dict[str, Any]],
    ) -> List[str]:
        if not isinstance(dt_data, dict):
            return []

        rules = dt_data.get("decision_rules")
        if not isinstance(rules, list):
            return []

        expected_values: List[str] = []
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            expected = rule.get("expected")
            if isinstance(expected, str) and expected.strip():
                expected_values.append(expected.strip())

        return expected_values

    def _expected_matches_dt(
        self,
        row: Dict[str, Any],
        expected_value: str,
        dt_expected_values: List[str],
        expected_fields: List[str],
    ) -> bool:
        if not dt_expected_values:
            return True

        if expected_value in dt_expected_values:
            return True

        # Trường hợp Step 2 expected là tên field, ví dụ "Username".
        # Khi đó Expected trong final phải bằng giá trị thật của field đó.
        for dt_expected in dt_expected_values:
            if dt_expected in expected_fields:
                field_value = row.get(dt_expected)
                if str(field_value) == expected_value:
                    return True

        return False

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

        top_keys = set(final_data.keys())
        required_top_keys = {"feature", "description", "items"}

        missing_top = sorted(required_top_keys - top_keys)
        extra_forbidden_top = sorted(top_keys & self.FORBIDDEN_TOP_LEVEL_KEYS)

        if missing_top:
            errors.append(f"Step3 top-level missing required keys: {missing_top}.")
        if extra_forbidden_top:
            errors.append(f"Step3 top-level must not contain old/intermediate keys: {extra_forbidden_top}.")

        feature, expected_fields = self._load_feature_and_expected_fields(final_data, errors)
        if feature is None:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        self._validate_non_empty_string(final_data.get("description"), "description", errors)

        items = self._validate_required_list(
            final_data,
            "items",
            errors,
            message="Missing or invalid 'items'. It must be a non-empty list.",
        )

        if items is None:
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        if not items:
            errors.append("items must not be empty.")
            return ValidationResult(ok=False, errors=errors, warnings=warnings)

        step1_feature, _coverage_map = self._load_step1_coverage_reference(step1_data, errors)
        if step1_feature is not None and step1_feature != feature:
            errors.append(
                f"Feature mismatch: Step 3 feature='{feature}' but Step 1 feature='{step1_feature}'."
            )

        if isinstance(dt_data, dict):
            dt_feature = dt_data.get("feature")
            if isinstance(dt_feature, str) and dt_feature.strip():
                dt_feature = normalize_feature_name(dt_feature)
                if dt_feature != feature:
                    errors.append(
                        f"Feature mismatch: Step 3 feature='{feature}' but Step 2 feature='{dt_feature}'."
                    )

        dt_expected_values = self._load_dt_expected_values(dt_data)

        required_item_keys = ["Testcase", *expected_fields, "Expected"]
        allowed_item_keys = set(required_item_keys)

        seen_testcase_ids: Set[str] = set()
        seen_signatures: Set[Tuple[Tuple[str, str], ...]] = set()

        for idx, item in enumerate(items):
            prefix = f"items[{idx}]"

            if not isinstance(item, dict):
                errors.append(f"{prefix} must be an object.")
                continue

            forbidden = sorted(set(item.keys()) & self.FORBIDDEN_ITEM_KEYS)
            if forbidden:
                errors.append(f"{prefix} must not contain intermediate/old keys: {forbidden}.")

            missing = [key for key in required_item_keys if key not in item]
            extra = sorted(set(item.keys()) - allowed_item_keys)

            if missing:
                errors.append(f"{prefix} missing required keys: {missing}.")
            if extra:
                errors.append(f"{prefix} contains unexpected keys: {extra}.")

            testcase_id = item.get("Testcase")
            if not isinstance(testcase_id, str) or not testcase_id.strip():
                errors.append(f"{prefix}.Testcase is missing or empty.")
            else:
                testcase_id = testcase_id.strip()
                if testcase_id in seen_testcase_ids:
                    errors.append(f"Duplicate Testcase: '{testcase_id}'.")
                seen_testcase_ids.add(testcase_id)

            expected_value = item.get("Expected")
            if not isinstance(expected_value, str) or not expected_value.strip():
                errors.append(f"{prefix}.Expected is missing or empty.")
            else:
                expected_value = expected_value.strip()
                if self._is_placeholder(expected_value):
                    errors.append(f"{prefix}.Expected must not be placeholder '{expected_value}'.")

                if not self._expected_matches_dt(
                    row=item,
                    expected_value=expected_value,
                    dt_expected_values=dt_expected_values,
                    expected_fields=expected_fields,
                ):
                    warnings.append(
                        f"{prefix}.Expected='{expected_value}' does not directly match Step 2 expected values."
                    )

            for field in expected_fields:
                value = item.get(field)
                if value is None:
                    errors.append(f"{prefix}.{field} must not be null.")
                elif self._is_placeholder(value):
                    errors.append(f"{prefix}.{field} must not be placeholder '{value}'.")

            signature = tuple(
                (key, "" if item.get(key) is None else str(item.get(key)))
                for key in [*expected_fields, "Expected"]
            )
            if signature in seen_signatures:
                errors.append(f"{prefix} duplicates another item with the same input values and Expected.")
            seen_signatures.add(signature)

        if isinstance(dt_data, dict):
            decision_rules = dt_data.get("decision_rules")
            if isinstance(decision_rules, list) and len(items) < len(decision_rules):
                warnings.append(
                    f"Step3 has fewer items ({len(items)}) than Step2 decision_rules ({len(decision_rules)})."
                )

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