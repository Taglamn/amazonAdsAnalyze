from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator

from .models import RoleName, UserStatus


class UserRegisterRequest(BaseModel):
    """Payload for creating a new user account."""

    email: EmailStr
    username: str | None = Field(default=None, min_length=3, max_length=64)
    password: str = Field(min_length=8, max_length=128)
    tenant_id: int
    role: RoleName = RoleName.VIEWER


class AdminCreateUserRequest(BaseModel):
    """Admin-only payload for creating users."""

    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=8, max_length=128)
    role: RoleName = RoleName.VIEWER
    store_ids: list[str] = Field(default_factory=list)


class UserLoginRequest(BaseModel):
    """Payload for login and JWT issuance."""

    account: str | None = Field(default=None, min_length=1, max_length=255)
    email: EmailStr | None = None
    password: str = Field(min_length=8, max_length=128)

    @model_validator(mode="after")
    def _validate_account_or_email(self):
        if (self.account or "").strip() or self.email:
            return self
        raise ValueError("account or email is required")


class TokenResponse(BaseModel):
    """JWT token response payload."""

    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_expires_in: int


class RefreshTokenRequest(BaseModel):
    """Payload for access token refresh."""

    refresh_token: str = Field(min_length=16, max_length=4096)


class UserOut(BaseModel):
    """Public user profile payload."""

    user_id: int
    username: str
    email: str
    tenant_id: int
    role_id: int
    role: str
    status: str
    created_at: datetime
    last_login: datetime | None

    model_config = ConfigDict(from_attributes=True)


class StoreOut(BaseModel):
    """Public store payload."""

    store_id: int
    tenant_id: int
    external_store_id: str
    store_name: str
    status: str

    model_config = ConfigDict(from_attributes=True)


class UserListResponse(BaseModel):
    """Response wrapper for user listing."""

    items: list[UserOut]


class AssignRoleRequest(BaseModel):
    """Payload for changing user's role."""

    role: RoleName


class SetStoreAccessRequest(BaseModel):
    """Payload for granting store authorization."""

    external_store_id: str = Field(min_length=1, max_length=128)
    store_name: str = Field(default="", max_length=255)


class BulkSetStoreAccessRequest(BaseModel):
    """Payload for bulk setting store authorization."""

    store_ids: list[str] = Field(default_factory=list)
    replace_existing: bool = True


class RemoveStoreAccessRequest(BaseModel):
    """Payload for revoking store authorization."""

    external_store_id: str = Field(min_length=1, max_length=128)


class PasswordResetRequest(BaseModel):
    """Payload for password reset."""

    new_password: str = Field(min_length=8, max_length=128)


class UserStatusRequest(BaseModel):
    """Payload to activate/deactivate account."""

    status: UserStatus


class PermissionCheckResponse(BaseModel):
    """Response for store authorization checks."""

    user_id: int
    external_store_id: str
    has_access: bool


class AccessibleStoresResponse(BaseModel):
    """Response for current user's visible stores."""

    stores: list[StoreOut]


class UserStoreAccessListResponse(BaseModel):
    """Response for target user's authorized stores."""

    user_id: int
    stores: list[StoreOut]


class TenantStoresResponse(BaseModel):
    """Response for listing all tenant stores for permission management."""

    stores: list[StoreOut]


class MeResponse(BaseModel):
    """Current user profile with role metadata."""

    user_id: int
    username: str
    email: str
    tenant_id: int
    role: str
    status: str
    created_at: datetime
    last_login: datetime | None
