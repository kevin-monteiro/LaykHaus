"""
Data Masking Engine for LaykHaus.

Provides dynamic data masking capabilities for sensitive information.
"""

import hashlib
import random
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern

from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


class MaskingType(Enum):
    """Types of data masking techniques."""
    FULL = "full"  # Replace entire value
    PARTIAL = "partial"  # Mask part of value
    HASH = "hash"  # One-way hash
    TOKENIZE = "tokenize"  # Replace with token
    ENCRYPT = "encrypt"  # Reversible encryption
    SHUFFLE = "shuffle"  # Shuffle characters
    NUMERIC_RANGE = "numeric_range"  # Replace with value in range
    EMAIL = "email"  # Mask email addresses
    PHONE = "phone"  # Mask phone numbers
    SSN = "ssn"  # Mask social security numbers
    CREDIT_CARD = "credit_card"  # Mask credit card numbers


@dataclass
class MaskingPolicy:
    """
    Defines a masking policy for sensitive data.
    
    Attributes:
        name: Policy name
        description: Policy description
        table_pattern: Pattern matching table names
        column_pattern: Pattern matching column names
        masking_type: Type of masking to apply
        masking_params: Parameters for masking function
        conditions: Conditions when to apply masking
    """
    name: str
    description: str
    table_pattern: str
    column_pattern: str
    masking_type: MaskingType
    masking_params: Dict = None
    conditions: Dict = None
    
    def matches(self, table_name: str, column_name: str) -> bool:
        """Check if policy matches a table/column."""
        import fnmatch
        return (fnmatch.fnmatch(table_name, self.table_pattern) and
                fnmatch.fnmatch(column_name, self.column_pattern))
    
    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "description": self.description,
            "table_pattern": self.table_pattern,
            "column_pattern": self.column_pattern,
            "masking_type": self.masking_type.value,
            "masking_params": self.masking_params,
            "conditions": self.conditions
        }


class DataMaskingEngine:
    """
    Manages data masking for sensitive information.
    
    Applies masking policies to query results based on security context.
    """
    
    def __init__(self):
        """Initialize data masking engine."""
        self.logger = get_logger(__name__)
        self.policies: List[MaskingPolicy] = []
        self.masking_functions: Dict[MaskingType, Callable] = {}
        self._initialize_masking_functions()
        self._initialize_default_policies()
    
    def _initialize_masking_functions(self):
        """Initialize built-in masking functions."""
        self.masking_functions = {
            MaskingType.FULL: self._mask_full,
            MaskingType.PARTIAL: self._mask_partial,
            MaskingType.HASH: self._mask_hash,
            MaskingType.TOKENIZE: self._mask_tokenize,
            MaskingType.SHUFFLE: self._mask_shuffle,
            MaskingType.NUMERIC_RANGE: self._mask_numeric_range,
            MaskingType.EMAIL: self._mask_email,
            MaskingType.PHONE: self._mask_phone,
            MaskingType.SSN: self._mask_ssn,
            MaskingType.CREDIT_CARD: self._mask_credit_card,
        }
    
    def _initialize_default_policies(self):
        """Initialize default masking policies."""
        # PII policies
        self.policies.extend([
            MaskingPolicy(
                name="mask_ssn",
                description="Mask social security numbers",
                table_pattern="*",
                column_pattern="*ssn*",
                masking_type=MaskingType.SSN
            ),
            MaskingPolicy(
                name="mask_email",
                description="Mask email addresses",
                table_pattern="*",
                column_pattern="*email*",
                masking_type=MaskingType.EMAIL
            ),
            MaskingPolicy(
                name="mask_phone",
                description="Mask phone numbers",
                table_pattern="*",
                column_pattern="*phone*",
                masking_type=MaskingType.PHONE
            ),
            MaskingPolicy(
                name="mask_credit_card",
                description="Mask credit card numbers",
                table_pattern="*",
                column_pattern="*card*",
                masking_type=MaskingType.CREDIT_CARD
            ),
            MaskingPolicy(
                name="mask_password",
                description="Fully mask passwords",
                table_pattern="*",
                column_pattern="*password*",
                masking_type=MaskingType.FULL,
                masking_params={"mask_char": "*"}
            ),
            MaskingPolicy(
                name="mask_salary",
                description="Replace salary with range",
                table_pattern="*employees*",
                column_pattern="salary",
                masking_type=MaskingType.NUMERIC_RANGE,
                masking_params={"ranges": [
                    (0, 50000, "< $50K"),
                    (50000, 100000, "$50K-$100K"),
                    (100000, 200000, "$100K-$200K"),
                    (200000, float('inf'), "> $200K")
                ]}
            ),
        ])
        
        self.logger.info(f"Initialized {len(self.policies)} default masking policies")
    
    def add_policy(self, policy: MaskingPolicy):
        """Add a masking policy."""
        self.policies.append(policy)
        self.logger.info(f"Added masking policy: {policy.name}")
    
    def remove_policy(self, policy_name: str) -> bool:
        """Remove a masking policy by name."""
        for i, policy in enumerate(self.policies):
            if policy.name == policy_name:
                del self.policies[i]
                self.logger.info(f"Removed masking policy: {policy_name}")
                return True
        return False
    
    def mask_value(
        self,
        value: Any,
        table_name: str,
        column_name: str,
        user_roles: List[str] = None
    ) -> Any:
        """
        Mask a single value based on applicable policies.
        
        Args:
            value: Value to mask
            table_name: Table containing the value
            column_name: Column containing the value
            user_roles: User roles for conditional masking
            
        Returns:
            Masked value or original if no policy applies
        """
        if value is None:
            return None
        
        # Find applicable policies
        for policy in self.policies:
            if not policy.matches(table_name, column_name):
                continue
            
            # Check conditions
            if policy.conditions:
                if "exclude_roles" in policy.conditions:
                    exclude_roles = policy.conditions["exclude_roles"]
                    if user_roles and any(role in exclude_roles for role in user_roles):
                        continue
                
                if "include_roles" in policy.conditions:
                    include_roles = policy.conditions["include_roles"]
                    if not user_roles or not any(role in include_roles for role in user_roles):
                        continue
            
            # Apply masking
            masking_func = self.masking_functions.get(policy.masking_type)
            if masking_func:
                return masking_func(value, policy.masking_params or {})
        
        return value
    
    def mask_results(
        self,
        table_name: str,
        columns: List[str],
        rows: List[Dict],
        user_roles: List[str] = None
    ) -> List[Dict]:
        """
        Mask query results based on policies.
        
        Args:
            table_name: Table name
            columns: Column names
            rows: Query result rows
            user_roles: User roles for conditional masking
            
        Returns:
            Masked rows
        """
        masked_rows = []
        
        for row in rows:
            masked_row = {}
            for col in columns:
                value = row.get(col)
                masked_value = self.mask_value(value, table_name, col, user_roles)
                masked_row[col] = masked_value
            masked_rows.append(masked_row)
        
        return masked_rows
    
    # Masking functions
    
    def _mask_full(self, value: Any, params: Dict) -> str:
        """Fully mask a value."""
        if value is None:
            return None
        mask_char = params.get("mask_char", "*")
        return mask_char * len(str(value))
    
    def _mask_partial(self, value: Any, params: Dict) -> str:
        """Partially mask a value."""
        if value is None:
            return None
        
        value_str = str(value)
        show_first = params.get("show_first", 0)
        show_last = params.get("show_last", 0)
        mask_char = params.get("mask_char", "*")
        
        if len(value_str) <= show_first + show_last:
            return mask_char * len(value_str)
        
        masked = value_str[:show_first]
        masked += mask_char * (len(value_str) - show_first - show_last)
        masked += value_str[-show_last:] if show_last > 0 else ""
        
        return masked
    
    def _mask_hash(self, value: Any, params: Dict) -> str:
        """Hash a value (one-way)."""
        if value is None:
            return None
        
        algorithm = params.get("algorithm", "sha256")
        salt = params.get("salt", "")
        
        value_str = str(value) + salt
        
        if algorithm == "sha256":
            return hashlib.sha256(value_str.encode()).hexdigest()
        elif algorithm == "md5":
            return hashlib.md5(value_str.encode()).hexdigest()
        else:
            return hashlib.sha256(value_str.encode()).hexdigest()
    
    def _mask_tokenize(self, value: Any, params: Dict) -> str:
        """Replace value with a token."""
        if value is None:
            return None
        
        prefix = params.get("prefix", "TOKEN")
        # In production, would use a token vault
        token = hashlib.md5(str(value).encode()).hexdigest()[:8].upper()
        return f"{prefix}_{token}"
    
    def _mask_shuffle(self, value: Any, params: Dict) -> str:
        """Shuffle characters in value."""
        if value is None:
            return None
        
        value_str = str(value)
        chars = list(value_str)
        random.shuffle(chars)
        return ''.join(chars)
    
    def _mask_numeric_range(self, value: Any, params: Dict) -> str:
        """Replace numeric value with range."""
        if value is None:
            return None
        
        try:
            num_value = float(value)
            ranges = params.get("ranges", [])
            
            for min_val, max_val, label in ranges:
                if min_val <= num_value < max_val:
                    return label
            
            return "Unknown"
        except (ValueError, TypeError):
            return str(value)
    
    def _mask_email(self, value: Any, params: Dict) -> str:
        """Mask email address."""
        if value is None:
            return None
        
        value_str = str(value)
        if "@" not in value_str:
            return self._mask_partial(value_str, {"show_first": 2, "show_last": 2})
        
        local, domain = value_str.split("@", 1)
        
        if len(local) <= 2:
            masked_local = "*" * len(local)
        else:
            masked_local = local[0] + "*" * (len(local) - 2) + local[-1]
        
        if "." in domain:
            domain_parts = domain.split(".")
            domain_parts[0] = "*" * len(domain_parts[0])
            masked_domain = ".".join(domain_parts)
        else:
            masked_domain = "*" * len(domain)
        
        return f"{masked_local}@{masked_domain}"
    
    def _mask_phone(self, value: Any, params: Dict) -> str:
        """Mask phone number."""
        if value is None:
            return None
        
        value_str = re.sub(r'\D', '', str(value))  # Remove non-digits
        
        if len(value_str) == 10:  # US phone
            return f"(***) ***-{value_str[-4:]}"
        elif len(value_str) == 11 and value_str[0] == "1":  # US with country code
            return f"+1 (***) ***-{value_str[-4:]}"
        else:
            # Generic masking
            if len(value_str) <= 4:
                return "*" * len(value_str)
            return "*" * (len(value_str) - 4) + value_str[-4:]
    
    def _mask_ssn(self, value: Any, params: Dict) -> str:
        """Mask social security number."""
        if value is None:
            return None
        
        value_str = re.sub(r'\D', '', str(value))  # Remove non-digits
        
        if len(value_str) == 9:
            return f"***-**-{value_str[-4:]}"
        else:
            return self._mask_partial(value_str, {"show_last": 4})
    
    def _mask_credit_card(self, value: Any, params: Dict) -> str:
        """Mask credit card number."""
        if value is None:
            return None
        
        value_str = re.sub(r'\D', '', str(value))  # Remove non-digits
        
        if len(value_str) >= 13 and len(value_str) <= 19:
            # Show first 4 and last 4
            if len(value_str) <= 8:
                return "*" * len(value_str)
            return value_str[:4] + " **** **** " + value_str[-4:]
        else:
            return self._mask_partial(value_str, {"show_first": 4, "show_last": 4})
    
    def export_config(self) -> Dict:
        """Export masking configuration."""
        return {
            "policies": [p.to_dict() for p in self.policies]
        }
    
    def import_config(self, config: Dict):
        """Import masking configuration."""
        self.policies = []
        
        for policy_data in config.get("policies", []):
            policy = MaskingPolicy(
                name=policy_data["name"],
                description=policy_data["description"],
                table_pattern=policy_data["table_pattern"],
                column_pattern=policy_data["column_pattern"],
                masking_type=MaskingType(policy_data["masking_type"]),
                masking_params=policy_data.get("masking_params"),
                conditions=policy_data.get("conditions")
            )
            self.policies.append(policy)
        
        self.logger.info(f"Imported {len(self.policies)} masking policies")