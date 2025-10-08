#ifndef ACL_H
#define ACL_H

#include <stdbool.h>

#include "config.h"

#define ACL_DEFAULT_USERNAME "default"

#define ACL_ROLE_READ  (1u << 0)
#define ACL_ROLE_WRITE (1u << 1)
#define ACL_ROLE_ADMIN (1u << 2)
#define ACL_ROLE_ALL   (ACL_ROLE_READ | ACL_ROLE_WRITE | ACL_ROLE_ADMIN)

typedef struct {
    bool require_auth;
    bool authenticated;
    unsigned int roles;
    const redis_acl_user_t *users;
    size_t user_count;
    size_t default_user_index;
    const redis_acl_user_t *default_user;
    char username[REDIS_USERNAME_MAX];
} acl_state_t;

void acl_init(acl_state_t *acl,
              const redis_acl_user_t *users,
              size_t user_count,
              size_t default_user_index);
void acl_reset_session(acl_state_t *acl);
bool acl_is_authenticated(const acl_state_t *acl);
bool acl_requires_auth(const acl_state_t *acl);
int acl_authenticate(acl_state_t *acl, const char *username, const char *password);
bool acl_has_role(const acl_state_t *acl, unsigned int roles);

#endif /* ACL_H */
