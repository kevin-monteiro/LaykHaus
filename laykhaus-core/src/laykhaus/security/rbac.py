"""
Role-Based Access Control (RBAC) implementation for LaykHaus.

Provides fine-grained access control for data sources, tables, and columns.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
import fnmatch
import json

from laykhaus.core.logging import get_logger

logger = get_logger(__name__)


class PermissionType(Enum):
    """Types of permissions in the system."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    EXECUTE = "execute"


class ResourceType(Enum):
    """Types of resources that can be protected."""
    CONNECTOR = "connector"
    DATABASE = "database"
    SCHEMA = "schema"
    TABLE = "table"
    COLUMN = "column"
    QUERY = "query"


@dataclass
class Permission:
    """
    Represents a permission on a resource.
    
    Attributes:
        resource_type: Type of resource
        resource_pattern: Pattern matching resource names (supports wildcards)
        permission_type: Type of permission granted
        conditions: Optional conditions for permission (e.g., row-level filters)
    """
    resource_type: ResourceType
    resource_pattern: str
    permission_type: PermissionType
    conditions: Optional[Dict] = None
    
    def matches(self, resource_type: ResourceType, resource_name: str) -> bool:
        """Check if this permission matches a resource."""
        if self.resource_type != resource_type:
            return False
        return fnmatch.fnmatch(resource_name, self.resource_pattern)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return {
            "resource_type": self.resource_type.value,
            "resource_pattern": self.resource_pattern,
            "permission_type": self.permission_type.value,
            "conditions": self.conditions
        }


@dataclass
class Role:
    """
    Represents a security role with a set of permissions.
    
    Attributes:
        name: Role name
        description: Role description
        permissions: Set of permissions granted to this role
        parent_roles: Parent roles to inherit permissions from
    """
    name: str
    description: str
    permissions: List[Permission] = field(default_factory=list)
    parent_roles: List[str] = field(default_factory=list)
    
    def has_permission(
        self,
        resource_type: ResourceType,
        resource_name: str,
        permission_type: PermissionType
    ) -> bool:
        """Check if role has a specific permission."""
        for perm in self.permissions:
            if (perm.matches(resource_type, resource_name) and
                perm.permission_type == permission_type):
                return True
        return False
    
    def add_permission(self, permission: Permission):
        """Add a permission to the role."""
        self.permissions.append(permission)
    
    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return {
            "name": self.name,
            "description": self.description,
            "permissions": [p.to_dict() for p in self.permissions],
            "parent_roles": self.parent_roles
        }


@dataclass
class SecurityContext:
    """
    Security context for a user session.
    
    Attributes:
        user_id: User identifier
        roles: List of roles assigned to the user
        attributes: User attributes for attribute-based access control
        session_id: Session identifier
    """
    user_id: str
    roles: List[str]
    attributes: Dict[str, any] = field(default_factory=dict)
    session_id: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return {
            "user_id": self.user_id,
            "roles": self.roles,
            "attributes": self.attributes,
            "session_id": self.session_id
        }


class RBACManager:
    """
    Manages Role-Based Access Control for LaykHaus.
    
    Handles role definitions, permission checking, and access control enforcement.
    """
    
    def __init__(self):
        """Initialize RBAC manager."""
        self.logger = get_logger(__name__)
        self.roles: Dict[str, Role] = {}
        self.user_roles: Dict[str, List[str]] = {}
        self._initialize_default_roles()
    
    def _initialize_default_roles(self):
        """Initialize default system roles."""
        # Admin role - full access
        admin_role = Role(
            name="admin",
            description="System administrator with full access"
        )
        admin_role.add_permission(Permission(
            resource_type=ResourceType.CONNECTOR,
            resource_pattern="*",
            permission_type=PermissionType.ADMIN
        ))
        admin_role.add_permission(Permission(
            resource_type=ResourceType.TABLE,
            resource_pattern="*",
            permission_type=PermissionType.ADMIN
        ))
        self.roles["admin"] = admin_role
        
        # Data Analyst role - read access to most data
        analyst_role = Role(
            name="data_analyst",
            description="Data analyst with read access to data sources"
        )
        analyst_role.add_permission(Permission(
            resource_type=ResourceType.CONNECTOR,
            resource_pattern="*",
            permission_type=PermissionType.READ
        ))
        analyst_role.add_permission(Permission(
            resource_type=ResourceType.TABLE,
            resource_pattern="*",
            permission_type=PermissionType.READ
        ))
        analyst_role.add_permission(Permission(
            resource_type=ResourceType.QUERY,
            resource_pattern="SELECT*",
            permission_type=PermissionType.EXECUTE
        ))
        self.roles["data_analyst"] = analyst_role
        
        # Data Engineer role - read/write access
        engineer_role = Role(
            name="data_engineer",
            description="Data engineer with read/write access",
            parent_roles=["data_analyst"]  # Inherits analyst permissions
        )
        engineer_role.add_permission(Permission(
            resource_type=ResourceType.TABLE,
            resource_pattern="*",
            permission_type=PermissionType.WRITE
        ))
        engineer_role.add_permission(Permission(
            resource_type=ResourceType.QUERY,
            resource_pattern="*",
            permission_type=PermissionType.EXECUTE
        ))
        self.roles["data_engineer"] = engineer_role
        
        # Guest role - limited read access
        guest_role = Role(
            name="guest",
            description="Guest with limited read access to public data"
        )
        guest_role.add_permission(Permission(
            resource_type=ResourceType.TABLE,
            resource_pattern="public.*",
            permission_type=PermissionType.READ
        ))
        guest_role.add_permission(Permission(
            resource_type=ResourceType.QUERY,
            resource_pattern="SELECT*",
            permission_type=PermissionType.EXECUTE,
            conditions={"limit": 100}  # Limit result size
        ))
        self.roles["guest"] = guest_role
        
        self.logger.info("Initialized default RBAC roles")
    
    def create_role(self, role: Role) -> bool:
        """
        Create a new role.
        
        Args:
            role: Role to create
            
        Returns:
            True if created, False if role already exists
        """
        if role.name in self.roles:
            self.logger.warning(f"Role {role.name} already exists")
            return False
        
        self.roles[role.name] = role
        self.logger.info(f"Created role: {role.name}")
        return True
    
    def delete_role(self, role_name: str) -> bool:
        """
        Delete a role.
        
        Args:
            role_name: Name of role to delete
            
        Returns:
            True if deleted, False if not found
        """
        if role_name not in self.roles:
            return False
        
        # Remove from all users
        for user_id in list(self.user_roles.keys()):
            if role_name in self.user_roles[user_id]:
                self.user_roles[user_id].remove(role_name)
        
        del self.roles[role_name]
        self.logger.info(f"Deleted role: {role_name}")
        return True
    
    def assign_role(self, user_id: str, role_name: str) -> bool:
        """
        Assign a role to a user.
        
        Args:
            user_id: User identifier
            role_name: Role to assign
            
        Returns:
            True if assigned, False if role doesn't exist
        """
        if role_name not in self.roles:
            self.logger.warning(f"Role {role_name} does not exist")
            return False
        
        if user_id not in self.user_roles:
            self.user_roles[user_id] = []
        
        if role_name not in self.user_roles[user_id]:
            self.user_roles[user_id].append(role_name)
            self.logger.info(f"Assigned role {role_name} to user {user_id}")
        
        return True
    
    def revoke_role(self, user_id: str, role_name: str) -> bool:
        """
        Revoke a role from a user.
        
        Args:
            user_id: User identifier
            role_name: Role to revoke
            
        Returns:
            True if revoked, False if not found
        """
        if user_id not in self.user_roles:
            return False
        
        if role_name in self.user_roles[user_id]:
            self.user_roles[user_id].remove(role_name)
            self.logger.info(f"Revoked role {role_name} from user {user_id}")
            return True
        
        return False
    
    def check_permission(
        self,
        context: SecurityContext,
        resource_type: ResourceType,
        resource_name: str,
        permission_type: PermissionType
    ) -> bool:
        """
        Check if a user has permission to access a resource.
        
        Args:
            context: Security context
            resource_type: Type of resource
            resource_name: Name of resource
            permission_type: Type of permission required
            
        Returns:
            True if permission granted, False otherwise
        """
        # Get all roles for user (including inherited)
        all_roles = self._get_all_roles(context.roles)
        
        # Check each role's permissions
        for role_name in all_roles:
            if role_name not in self.roles:
                continue
                
            role = self.roles[role_name]
            if role.has_permission(resource_type, resource_name, permission_type):
                self.logger.debug(
                    f"Permission granted to {context.user_id} for "
                    f"{permission_type.value} on {resource_type.value}:{resource_name} "
                    f"via role {role_name}"
                )
                return True
        
        self.logger.warning(
            f"Permission denied to {context.user_id} for "
            f"{permission_type.value} on {resource_type.value}:{resource_name}"
        )
        return False
    
    def _get_all_roles(self, role_names: List[str]) -> Set[str]:
        """Get all roles including inherited ones."""
        all_roles = set(role_names)
        to_process = list(role_names)
        
        while to_process:
            role_name = to_process.pop()
            if role_name in self.roles:
                role = self.roles[role_name]
                for parent in role.parent_roles:
                    if parent not in all_roles:
                        all_roles.add(parent)
                        to_process.append(parent)
        
        return all_roles
    
    def filter_query_results(
        self,
        context: SecurityContext,
        table_name: str,
        columns: List[str],
        rows: List[Dict]
    ) -> Tuple[List[str], List[Dict]]:
        """
        Filter query results based on user permissions.
        
        Args:
            context: Security context
            table_name: Table being queried
            columns: Column names
            rows: Query result rows
            
        Returns:
            Filtered columns and rows
        """
        # Check column-level permissions
        allowed_columns = []
        for col in columns:
            resource_name = f"{table_name}.{col}"
            if self.check_permission(
                context,
                ResourceType.COLUMN,
                resource_name,
                PermissionType.READ
            ):
                allowed_columns.append(col)
        
        # If no column-specific permissions, check table-level
        if not allowed_columns:
            if self.check_permission(
                context,
                ResourceType.TABLE,
                table_name,
                PermissionType.READ
            ):
                allowed_columns = columns
        
        # Filter rows to only include allowed columns
        if allowed_columns != columns:
            filtered_rows = []
            for row in rows:
                filtered_row = {col: row.get(col) for col in allowed_columns}
                filtered_rows.append(filtered_row)
            return allowed_columns, filtered_rows
        
        return columns, rows
    
    def get_resource_filter(
        self,
        context: SecurityContext,
        resource_type: ResourceType
    ) -> Optional[str]:
        """
        Get resource filter SQL clause for a user.
        
        Args:
            context: Security context
            resource_type: Type of resource
            
        Returns:
            SQL WHERE clause or None
        """
        filters = []
        all_roles = self._get_all_roles(context.roles)
        
        for role_name in all_roles:
            if role_name not in self.roles:
                continue
            
            role = self.roles[role_name]
            for perm in role.permissions:
                if perm.resource_type == resource_type and perm.conditions:
                    if "filter" in perm.conditions:
                        filters.append(perm.conditions["filter"])
        
        if filters:
            return " OR ".join(f"({f})" for f in filters)
        
        return None
    
    def export_config(self) -> Dict:
        """Export RBAC configuration."""
        return {
            "roles": {name: role.to_dict() for name, role in self.roles.items()},
            "user_roles": self.user_roles
        }
    
    def import_config(self, config: Dict):
        """Import RBAC configuration."""
        # Import roles
        for role_data in config.get("roles", {}).values():
            role = Role(
                name=role_data["name"],
                description=role_data["description"],
                parent_roles=role_data.get("parent_roles", [])
            )
            
            for perm_data in role_data.get("permissions", []):
                permission = Permission(
                    resource_type=ResourceType(perm_data["resource_type"]),
                    resource_pattern=perm_data["resource_pattern"],
                    permission_type=PermissionType(perm_data["permission_type"]),
                    conditions=perm_data.get("conditions")
                )
                role.add_permission(permission)
            
            self.roles[role.name] = role
        
        # Import user role assignments
        self.user_roles = config.get("user_roles", {})
        
        self.logger.info(f"Imported RBAC config with {len(self.roles)} roles")