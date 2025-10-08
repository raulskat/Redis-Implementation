#ifndef COMMAND_HANDLERS_H
#define COMMAND_HANDLERS_H

#include "command.h"

int handle_ping_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_echo_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_auth_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_set_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_get_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_hello_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_keys_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_config_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_info_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_expire_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_pexpire_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_ttl_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_pttl_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_persist_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_flush_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_save_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_bgsave_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_replconf_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);
int handle_psync_command(int fd, const resp_command_t *cmd, command_context_t *ctx, command_session_t *session);

#endif // COMMAND_HANDLERS_H
