#include "acl.h"

#include <string.h>

static const redis_acl_user_t fallback_users[] = {
    {.username = ACL_DEFAULT_USERNAME, .password = "", .roles = ACL_ROLE_ALL},
};

static unsigned int acl_effective_roles(const redis_acl_user_t *user) {
    if (!user) {
        return ACL_ROLE_ALL;
    }
    return (user->roles == 0) ? ACL_ROLE_ALL : user->roles;
}

static const redis_acl_user_t *acl_lookup_user(const acl_state_t *acl, const char *username) {
    if (!acl || !acl->users || acl->user_count == 0) {
        return NULL;
    }
    if (!username || username[0] == '\0') {
        return acl->default_user;
    }
    for (size_t i = 0; i < acl->user_count; ++i) {
        if (strcmp(acl->users[i].username, username) == 0) {
            return &acl->users[i];
        }
    }
    return NULL;
}

static void acl_apply_default_user(acl_state_t *acl) {
    if (!acl) {
        return;
    }
    if (!acl->users || acl->user_count == 0) {
        acl->users = fallback_users;
        acl->user_count = 1;
        acl->default_user_index = 0;
    }
    if (acl->default_user_index >= acl->user_count) {
        acl->default_user_index = 0;
    }
    acl->default_user = &acl->users[acl->default_user_index];
    if (!acl->default_user) {
        acl->username[0] = '\0';
        acl->require_auth = (acl->user_count > 0);
        acl->authenticated = !acl->require_auth;
        acl->roles = acl->authenticated ? ACL_ROLE_ALL : 0;
        return;
    }
    strncpy(acl->username, acl->default_user->username, sizeof(acl->username) - 1);
    acl->username[sizeof(acl->username) - 1] = '\0';
    if (acl->default_user->password[0] == '\0') {
        acl->require_auth = false;
        acl->authenticated = true;
        acl->roles = acl_effective_roles(acl->default_user);
    } else {
        acl->require_auth = true;
        acl->authenticated = false;
        acl->roles = 0;
    }
}

void acl_init(acl_state_t *acl,
              const redis_acl_user_t *users,
              size_t user_count,
              size_t default_user_index) {
    if (!acl) {
        return;
    }
    memset(acl, 0, sizeof(*acl));
    if (!users || user_count == 0) {
        acl->users = fallback_users;
        acl->user_count = 1;
        acl->default_user_index = 0;
    } else {
        acl->users = users;
        acl->user_count = user_count;
        acl->default_user_index = (default_user_index < user_count) ? default_user_index : 0;
    }
    acl_apply_default_user(acl);
}

void acl_reset_session(acl_state_t *acl) {
    if (!acl) {
        return;
    }
    acl_apply_default_user(acl);
}

bool acl_is_authenticated(const acl_state_t *acl) {
    return acl ? acl->authenticated : true;
}

bool acl_requires_auth(const acl_state_t *acl) {
    return acl ? acl->require_auth : false;
}

int acl_authenticate(acl_state_t *acl, const char *username, const char *password) {
    if (!acl) {
        return -1;
    }
    const redis_acl_user_t *user = acl_lookup_user(acl, username);
    if (!user) {
        acl->authenticated = false;
        acl->roles = 0;
        return -1;
    }
    const char *expected_password = user->password;
    size_t expected_len = strlen(expected_password);
    size_t provided_len = (password && password[0] != '\0') ? strlen(password) : 0;
    if (expected_len > 0) {
        if (!password || provided_len != expected_len ||
            strncmp(password, expected_password, expected_len) != 0) {
            acl->authenticated = false;
            acl->roles = 0;
            return -1;
        }
    }

    acl->authenticated = true;
    acl->roles = acl_effective_roles(user);
    strncpy(acl->username, user->username, sizeof(acl->username) - 1);
    acl->username[sizeof(acl->username) - 1] = '\0';
    return 0;
}

bool acl_has_role(const acl_state_t *acl, unsigned int roles) {
    if (!acl) {
        return false;
    }
    if (roles == 0) {
        return true;
    }
    if (!acl->authenticated && acl->require_auth) {
        return false;
    }
    return (acl->roles & roles) == roles;
}
